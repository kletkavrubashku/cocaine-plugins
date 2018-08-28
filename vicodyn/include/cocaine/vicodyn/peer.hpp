#pragma once

#include "cocaine/idl/node.hpp"

#include <cocaine/executor/asio.hpp>
#include <cocaine/forwards.hpp>
#include <cocaine/locked_ptr.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/rpc/session.hpp>
#include <cocaine/rpc/upstream.hpp>
#include <metrics/usts/ewma.hpp>

#include <asio/ip/tcp.hpp>

#include <unordered_map>

namespace cocaine {
namespace vicodyn {
namespace defaults {

    constexpr double system_weight = 1;

} // namespace defaults

class peer_t : public std::enable_shared_from_this<peer_t> {
public:
    using app_streaming_tag = io::stream_of<std::string>::tag;

    using endpoints_t = std::vector<asio::ip::tcp::endpoint>;

    ~peer_t();

    peer_t(context_t& context, asio::io_service& loop, endpoints_t endpoints, std::string uuid, dynamic_t::object_t extra);

    template<class Event, class ...Args>
    auto open_stream(std::shared_ptr<io::basic_dispatch_t> dispatch, Args&& ...args) -> io::upstream_ptr_t {
        auto locked = session_.synchronize();
        auto session = *locked;
        if(!session) {
            schedule_reconnect(session);
            throw error_t(error::not_connected, "session is not connected");
        }
        d_.last_active = std::chrono::system_clock::now();
        auto stream = session->fork(std::move(dispatch));
        stream->send<Event>(std::forward<Args>(args)...);
        return stream;
    }

    auto connect() -> void;

    auto schedule_reconnect() -> void;

    auto uuid() const -> const std::string&;

    auto hostname() const -> const std::string&;

    auto endpoints() const -> const std::vector<asio::ip::tcp::endpoint>&;

    auto connected() const -> bool;

    auto last_active() const -> std::chrono::system_clock::time_point;

    auto extra() const -> const dynamic_t::object_t&;

    auto x_cocaine_cluster() const -> const std::string&;

private:
    auto schedule_reconnect(std::shared_ptr<cocaine::session_t>& session) -> void;

    context_t& context_;
    asio::io_service& loop_;
    asio::deadline_timer timer_;
    std::unique_ptr<logging::logger_t> logger_;
    synchronized<std::shared_ptr<cocaine::session_t>> session_;
    bool connecting_{false};

    struct {
        std::string uuid;
        std::vector<asio::ip::tcp::endpoint> endpoints;
        std::chrono::system_clock::time_point last_active;
        dynamic_t::object_t extra;
        std::string x_cocaine_cluster;
        std::string hostname;
    } d_;

};

// thread safe wrapper on map of peers indexed by uuid
class peers_t {
public:
    using clock_t = std::chrono::steady_clock;

    class app_service_t {
        clock_t::time_point ban_until_;
        std::unique_ptr<metrics::usts::ewma<clock_t>> timings_ewma_;

    public:
        app_service_t(clock_t::duration timings_window);

        auto ban(std::chrono::milliseconds timeout) -> void;
        auto banned() const -> bool;
        auto banned_for() const -> clock_t::duration;

        /// Adds the request processing time to consider the average value.
        ///
        /// \param elapsed Observed value.
        auto add_request_duration(clock_t::duration elapsed) -> void;

        /// Returns the average processing time of request in nanoseconds.
        auto avg_request_duration() const -> clock_t::duration;
    };

    struct peer_data_t {
        std::shared_ptr<peer_t> peer;
        std::atomic<double> system_weight{defaults::system_weight};
    };

    using app_predicate_t = std::function<bool(const app_service_t& app_service)>;
    using peer_predicate_t = std::function<bool(const peer_t& peer_service)>;
    using endpoints_t = std::vector<asio::ip::tcp::endpoint>;
    // peer_uuid -> peer_data
    using peers_map_t = std::unordered_map<std::string, peer_data_t>;
    // peer_uuid -> app_service
    using app_services_t = std::unordered_map<std::string, app_service_t>;
    // app_name -> app_services
    using apps_map_t = std::unordered_map<std::string, app_services_t>;

    struct data_t {
        peers_map_t peers;
        apps_map_t apps;
    };


private:
    using app_handler_t = std::function<void(const std::string& uuid, const app_service_t& app_service)>;
    using app_enumerator_t = std::function<void(const app_services_t& apps, const app_handler_t& handler)>;

    struct timings_t {
        const bool enabled;
        const clock_t::duration window;

        timings_t(const dynamic_t& args);
    };

    context_t& context;
    std::unique_ptr<logging::logger_t> logger;
    executor::owning_asio_t executor;
    data_t data;
    const timings_t timings;
    mutable boost::shared_mutex mutex;


public:
    template<class F>
    auto apply_shared(F&& f) const -> decltype(f(std::declval<const data_t&>())) {
        boost::shared_lock<boost::shared_mutex> lock(mutex);
        return f(data);
    }

    /// Use this method at your own risk. The method takes a shared lock and allows to change data.
    /// You can use this method if you go to make changes atomically.
    template<class F>
    auto apply_shared_unsafe(F&& f) -> decltype(f(std::declval<data_t&>())) {
        boost::shared_lock<boost::shared_mutex> lock(mutex);
        return f(data);
    }

    template<class F>
    auto apply(F&& f) -> decltype(f(std::declval<data_t&>())) {
        boost::unique_lock<boost::shared_mutex> lock(mutex);
        return f(data);
    }


    peers_t(context_t& context, const dynamic_t& args);

    auto register_peer(const std::string& uuid, const endpoints_t& endpoints, dynamic_t::object_t extra)
                    -> std::shared_ptr<peer_t>;

    auto register_peer(const std::string& uuid, std::shared_ptr<peer_t> peer) -> void;

    auto erase_peer(const std::string& uuid) -> void;

    auto register_app(const std::string& uuid, const std::string& name) -> void;

    auto erase_app(const std::string& uuid, const std::string& name) -> void;

    auto ban_app(const std::string& uuid, const std::string& name, const std::chrono::milliseconds& timeout) -> void;

    auto add_app_request_duration(const std::string& uuid, const std::string& name, clock_t::duration elapsed) -> void;

    auto erase(const std::string& uuid) -> void;

    auto peer(const std::string& uuid) -> std::shared_ptr<peer_t>;

    auto choose_random(const std::string& app_name, const peer_predicate_t& peer_predicate,
                    const app_predicate_t& app_service_predicate) const -> std::shared_ptr<peer_t>;
    auto choose_random(const std::vector<std::string>& uuids, const std::string& app_name,
                    const peer_predicate_t& peer_predicate, const app_predicate_t& app_service_predicate) const
                    -> std::shared_ptr<peer_t>;


private:
    auto choose_random(const app_enumerator_t& enumerator, const std::string& app_name,
                    const peer_predicate_t& peer_predicate, const app_predicate_t& app_service_predicate) const
                    -> std::shared_ptr<peer_t>;
};

} // namespace vicodyn
} // namespace cocaine
