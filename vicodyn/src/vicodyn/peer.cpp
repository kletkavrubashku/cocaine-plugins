#include "cocaine/vicodyn/peer.hpp"

#include "cocaine/format/endpoint.hpp"
#include "cocaine/format/peer.hpp"

#include <cocaine/context.hpp>
#include <cocaine/engine.hpp>
#include <cocaine/errors.hpp>
#include <cocaine/logging.hpp>
#include <cocaine/rpc/asio/decoder.hpp>
#include <cocaine/rpc/graph.hpp>
#include <cocaine/rpc/session.hpp>
#include <cocaine/memory.hpp>

#include <asio/ip/tcp.hpp>
#include <asio/connect.hpp>

#include <blackhole/logger.hpp>

#include <metrics/registry.hpp>
#include <cocaine/rpc/upstream.hpp>

#include <random>

namespace cocaine {
namespace vicodyn {
namespace {

template<class T>
class weighted_distribution {
    std::vector<T> elements_;
    std::vector<double> weights_;

public:
    weighted_distribution(std::size_t reserve) {
        elements_.reserve(reserve);
        weights_.reserve(reserve);
    }

    auto add(T elem, double weight) -> void {
        weights_.push_back( weight);
        elements_.push_back(std::move(elem));
    }

    auto random() -> boost::optional<T> {
        if (weights_.empty()) {
            return {};
        }
        // We use keyword "static", because creation of entity below is very expensive
        static std::mt19937 generator{std::random_device{}()};
        // Attention: unnecessary copying. TODO: Fix when it will be possible
        std::discrete_distribution<std::size_t> distribution(weights_.begin(), weights_.end());
        return elements_[distribution(generator)];
    }
};

} // anonymous namespace

peer_t::~peer_t(){
    session_.apply([&](std::shared_ptr<session_t>& session) {
        if (session) {
            session->detach(std::error_code());
        }
    });
}

peer_t::peer_t(context_t& context, asio::io_service& loop, endpoints_t endpoints, std::string uuid, dynamic_t::object_t extra) :
    context_(context),
    loop_(loop),
    timer_(loop),
    logger_(context.log(format("vicodyn_peer/{}", uuid))),
    d_{
        std::move(uuid),
        std::move(endpoints),
        std::chrono::system_clock::now(),
        std::move(extra),
        /*x_cocaine_cluster*/ {},
        /*hostname*/ {},
    }
{
    d_.x_cocaine_cluster = d_.extra.at("x-cocaine-cluster", "").as_string();
    d_.hostname = [&]() -> std::string {
        asio::io_service asio;
        asio::ip::tcp::resolver resolver(asio);
        const asio::ip::tcp::resolver::iterator end;

        for (const auto& endpoint: d_.endpoints) {
            auto it = resolver.resolve(endpoint);
            if (it != end) {
                return it->host_name();
            }
        }
        return {};
    }();
}

auto peer_t::schedule_reconnect() -> void {
    COCAINE_LOG_INFO(logger_, "scheduling reconnection of peer {} to {}", uuid(), endpoints());
    session_.apply([&](std::shared_ptr<session_t>& session) {
        schedule_reconnect(session);
    });
}
auto peer_t::schedule_reconnect(std::shared_ptr<cocaine::session_t>& session) -> void {
    if (connecting_) {
        COCAINE_LOG_INFO(logger_, "reconnection is already in progress for {}", uuid());
        return;
    }
    if (session) {
        // In fact it should be detached already
        session->detach(std::error_code());
        session = nullptr;
    }
    timer_.expires_from_now(boost::posix_time::seconds(1));
    timer_.async_wait([&](std::error_code ec) {
        if (!ec) {
            connect();
        }
    });
    connecting_ = true;
    COCAINE_LOG_INFO(logger_, "scheduled reconnection of peer {} to {}", uuid(), endpoints());
}

auto peer_t::connect() -> void {
    connecting_ = true;
    COCAINE_LOG_INFO(logger_, "connecting peer {} to {}", uuid(), endpoints());

    auto socket = std::make_shared<asio::ip::tcp::socket>(loop_);
    auto connect_timer = std::make_shared<asio::deadline_timer>(loop_);

    std::weak_ptr<peer_t> weak_self(shared_from_this());

    auto begin = d_.endpoints.begin();
    auto end = d_.endpoints.end();

    connect_timer->expires_from_now(boost::posix_time::seconds(60));
    connect_timer->async_wait([=](std::error_code ec) {
        auto self = weak_self.lock();
        if (!self){
            return;
        }
        if (!ec) {
            COCAINE_LOG_INFO(logger_, "connection timer expired, canceling socket, going to schedule reconnect");
            socket->cancel();
            self->connecting_ = false;
            self->schedule_reconnect();
        } else {
            COCAINE_LOG_DEBUG(logger_, "connection timer was cancelled");
        }
    });

    asio::async_connect(*socket, begin, end, [=](const std::error_code& ec, endpoints_t::const_iterator endpoint_it) {
        auto self = weak_self.lock();
        if (!self){
            return;
        }
        COCAINE_LOG_DEBUG(logger_, "cancelling timer");
        if (!connect_timer->cancel()) {
            COCAINE_LOG_ERROR(logger_, "could not connect to {} - timed out (timer could not be cancelled)", *endpoint_it);
            return;
        }
        if (ec) {
            COCAINE_LOG_ERROR(logger_, "could not connect to {} - {}({})", *endpoint_it, ec.message(), ec.value());
            if (endpoint_it == end) {
                connecting_ = false;
                schedule_reconnect();
            }
            return;
        }
        try {
            COCAINE_LOG_INFO(logger_, "successfully connected peer {} to {}", uuid(), endpoints());
            auto ptr = std::make_unique<asio::ip::tcp::socket>(std::move(*socket));
            auto new_session = context_.engine().attach(std::move(ptr), nullptr);
            session_.apply([&](std::shared_ptr<session_t>& session) {
                connecting_ = false;
                session = std::move(new_session);
                d_.last_active = std::chrono::system_clock::now();
            });
        } catch(const std::exception& e) {
            COCAINE_LOG_WARNING(logger_, "failed to attach session to queue: {}", e.what());
            schedule_reconnect();
        }
    });
}

auto peer_t::uuid() const -> const std::string& {
    return d_.uuid;
}

auto peer_t::hostname() const -> const std::string& {
    return d_.hostname;
}

auto peer_t::endpoints() const -> const std::vector<asio::ip::tcp::endpoint>& {
    return d_.endpoints;
}

auto peer_t::connected() const -> bool {
    return static_cast<bool>(session_.unsafe());
}

auto peer_t::last_active() const -> std::chrono::system_clock::time_point {
    return d_.last_active;
}

auto peer_t::extra() const -> const dynamic_t::object_t& {
    return d_.extra;
}

auto peer_t::x_cocaine_cluster() const -> const std::string& {
    return d_.x_cocaine_cluster;
}

peers_t::peers_t(context_t& context, const dynamic_t& args)
    : context(context)
    , logger(context.log("vicodyn/peers_t")) {
    auto timings_ms = args.as_object().at("timings-window-ms", 30000U).as_uint();
    timings_window = std::chrono::duration_cast<clock_t::duration>(std::chrono::milliseconds(timings_ms));
}

auto peers_t::register_peer(const std::string& uuid, const endpoints_t& endpoints, dynamic_t::object_t extra)
    -> std::shared_ptr<peer_t>
{
    return apply([&](data_t& data){
        auto& peer = data.peers[uuid];
        if (!peer) {
            peer = std::make_shared<peer_t>(context, executor.asio(), endpoints, uuid, std::move(extra));
            peer->connect();
        } else if (endpoints != peer->endpoints()) {
            COCAINE_LOG_ERROR(logger, "changed endpoints detected for uuid {}, previous {}, new {}", uuid,
                              peer->endpoints(), endpoints);
            peer = std::make_shared<peer_t>(context, executor.asio(), endpoints, uuid, extra);
            peer->connect();
        }
        return peer;
    });
}

auto peers_t::register_peer(const std::string& uuid, std::shared_ptr<peer_t> peer) -> void {
    apply([&](data_t& data) {
        data.peers[uuid] = std::move(peer);
    });
}

auto peers_t::erase_peer(const std::string& uuid) -> void {
    apply([&](data_t& data){
        data.peers.erase(uuid);
    });
}

auto peers_t::register_app(const std::string& uuid, const std::string& name) -> void {
    apply([&](data_t& data) {
        data.apps[name].emplace(uuid, app_service_t(timings_window));
    });
}

auto peers_t::erase_app(const std::string& uuid, const std::string& name) -> void {
    apply([&](data_t& data) {
        data.apps[name].erase(uuid);
    });
}

auto peers_t::ban_app(const std::string& uuid, const std::string& name, const std::chrono::milliseconds& timeout)
                -> void {
    apply([&](data_t& data) {
        auto apps_it = data.apps.find(name);
        if(apps_it ==  std::end(data.apps)) {
            return;
        }
        auto app_service_it = apps_it->second.find(uuid);
        if (app_service_it == std::end(apps_it->second)) {
            return;
        }
        app_service_it->second.ban(timeout);
    });
}

auto peers_t::add_app_request_timing(const std::string& uuid, const std::string& name, clock_t::time_point start,
                clock_t::duration elapsed) -> void {
    apply([&](data_t& data) {
        auto apps_it = data.apps.find(name);
        if(apps_it ==  std::end(data.apps)) {
            return;
        }
        auto app_service_it = apps_it->second.find(uuid);
        if (app_service_it == std::end(apps_it->second)) {
            return;
        }
        app_service_it->second.add_request_timing(start, elapsed);
    });
}

auto peers_t::erase(const std::string& uuid) -> void {
    erase_peer(uuid);
    apply([&](data_t& data) {
        data.peers.erase(uuid);
        for (auto& pair : data.apps) {
            pair.second.erase(uuid);
        }
    });
}

auto peers_t::peer(const std::string& uuid) -> std::shared_ptr<peer_t> {
    return apply_shared([&](const data_t& data) -> std::shared_ptr<peer_t>{
        auto it = data.peers.find(uuid);
        if (it != data.peers.end()) {
            return it->second;
        }
        return nullptr;
    });
}

peers_t::app_service_t::app_service_t(clock_t::duration timings_window)
    : timings_ewma_(new metrics::usts::ewma<clock_t >(timings_window)) {}

auto peers_t::app_service_t::ban(std::chrono::milliseconds timeout) -> void {
    if (timeout.count() > 0) {
        ban_until_ = clock_t::now() + timeout;
    }
}

auto peers_t::app_service_t::banned() const -> bool {
    return ban_until_ > clock_t::now();
}

auto peers_t::app_service_t::add_request_timing(clock_t::time_point start, clock_t::duration elapsed) -> void {
    timings_ewma_->add(start, elapsed.count());
}

auto peers_t::app_service_t::average_elapsed_ns() const -> double {
    return timings_ewma_->get();
}

auto peers_t::choose_random(const std::string& app_name, const peer_predicate_t& peer_predicate,
                const app_predicate_t& app_predicate) const -> std::shared_ptr<peer_t> {
    auto enumerator = [&](const app_services_t& apps, const app_handler_t& handler) {
        for (const auto& app_service : apps) {
            handler(app_service.first, app_service.second);
        }
    };
    return choose_random(enumerator, app_name, peer_predicate, app_predicate);
}

auto peers_t::choose_random(const std::vector<std::string>& uuids, const std::string& app_name,
                const peer_predicate_t& peer_predicate, const app_predicate_t& app_predicate) const
                -> std::shared_ptr<peer_t> {
    auto enumerator = [&](const app_services_t& apps, const app_handler_t& handler) {
        for (const std::string& uuid : uuids) {
            auto app_service_it = apps.find(uuid);
            if (app_service_it == apps.end()) {
                continue;
            }
            handler(uuid, app_service_it->second);
        }
    };
    return choose_random(enumerator, app_name, peer_predicate, app_predicate);
}

auto peers_t::choose_random(const app_enumerator_t& app_enumerator, const std::string& app_name,
                const peer_predicate_t& peer_predicate, const app_predicate_t& app_predicate) const
                -> std::shared_ptr<peer_t> {
    return apply_shared([&](const data_t& data) -> std::shared_ptr<peer_t> {
        auto apps_it = data.apps.find(app_name);
        if (apps_it == data.apps.end() || apps_it->second.empty()) {
            COCAINE_LOG_WARNING(logger, "peer list for app \"{}\" is empty", app_name);
            return {};
        }
        const auto& app_services = apps_it->second;

        // Choose peer according average elapsed time per request
        weighted_distribution<peers_data_t::const_iterator> distribution(app_services.size());
        app_enumerator(app_services, [&](const std::string& uuid, const app_service_t& app_service) {
            if (!app_predicate(app_service)) {
                return;
            }
            auto peer_it = data.peers.find(uuid);
            if (peer_it == data.peers.end()) {
                return;
            }
            if (!peer_predicate(*peer_it->second)) {
                return;
            }
            auto positive_duration_ns = std::max(app_service.average_elapsed_ns(), 1.);
            distribution.add(peer_it, 1. / positive_duration_ns);
        });
        auto chosen = distribution.random();
        if (!chosen) {
            return {};
        }
        return chosen.get()->second;
    });
}

} // namespace vicodyn
} // namespace cocaine
