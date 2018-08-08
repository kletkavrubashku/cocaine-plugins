#pragma once

#include "cocaine/vicodyn/forwards.hpp"
#include "cocaine/vicodyn/proxy/buffer.hpp"
#include "cocaine/vicodyn/proxy/discardable.hpp"
#include "cocaine/vicodyn/proxy/middlewares.hpp"
#include "cocaine/vicodyn/proxy/safe_stream.hpp"
#include "cocaine/vicodyn/request_context.hpp"

namespace cocaine {
namespace vicodyn {

class endpoint_t : public std::enable_shared_from_this<endpoint_t>  {
    using clock_t = peers_t::clock_t;

    struct data_t {
        std::shared_ptr<peer_t> peer;
        safe_stream_t forward_stream;
        std::unique_ptr<logging::logger_t> logger;
        clock_t::time_point start_time;
        bool error_sent = false;
    };

    data_t current_;
    buffer_t buffer_;
    boost::shared_mutex mutex_;

    discardable_dispatch_t backward_dispatch_;
    std::atomic_bool backward_started_{false};

    std::string app_name_;
    const api::vicodyn::balancer_ptr balancer_;
    const std::shared_ptr<request_context_t> request_context_;
    logging::logger_t& base_logger_;

public:
    endpoint_t(const std::string& dispatch_name, api::vicodyn::balancer_ptr balancer, std::size_t buffer_capacity,
                    std::shared_ptr<request_context_t> request_context, logging::logger_t& logger);

    auto enqueue(const std::string& app_name, const hpack::headers_t& headers, std::string&& event) -> void;
    auto finalize() -> void;

    /// Upstream
    auto chunk(const hpack::headers_t& headers, std::string&& data) -> bool;
    auto choke(const hpack::headers_t& headers) -> bool;
    auto error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> bool;

    /// Dispatch
    template<class Event>
    auto on() -> slot_builder<Event, std::tuple<on_success_t, on_error_t>> {
        namespace ph = std::placeholders;
        on_success_t set_started_on_success(std::bind(&endpoint_t::set_started, this));
        on_error_t check_condition_on_error(std::bind(&endpoint_t::send_error_backward, this, ph::_1, ph::_2));
        return backward_dispatch_.on<Event>()
                        .with_middleware(check_condition_on_error).with_middleware(set_started_on_success);
    }

    template<class F>
    auto on_discard(F discarder) -> void {
        backward_dispatch_.on_discard(discarder);
    }

private:
    auto set_started() -> bool;
    auto send_error_backward(const std::error_code& ec, const std::string& msg) -> bool;

    auto choose_endpoint(const hpack::headers_t& headers, std::string&& event) -> bool;
    auto retry() -> void;
};

} // namespace vicodyn
} // namespace cocaine
