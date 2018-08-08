#pragma once

#include "cocaine/vicodyn/forwards.hpp"
#include "cocaine/vicodyn/proxy/backward.hpp"
#include "cocaine/vicodyn/proxy/buffer.hpp"
#include "cocaine/vicodyn/proxy/discardable.hpp"
#include "cocaine/vicodyn/proxy/safe_stream.hpp"
#include "cocaine/vicodyn/request_context.hpp"

namespace cocaine {
namespace vicodyn {

class forward_t : public std::enable_shared_from_this<forward_t> {
    using clock_t = peers_t::clock_t;

    std::string app_name_;
    std::shared_ptr<peer_t> peer_;
    safe_stream_t stream_;
    buffer_t buffer_;
    discardable_dispatch_t dispatch_;
    clock_t::time_point start_time_;
    bool error_sent_ = false;
    boost::shared_mutex mutex_;

    std::weak_ptr<backward_t> backward_stream_;

    std::unique_ptr<logging::logger_t> logger_;
    const std::shared_ptr<request_context_t> request_context_;

    logging::logger_t& base_logger_;
    const api::vicodyn::balancer_ptr balancer_;

public:
    forward_t(const std::string& name, std::size_t buffer_capacity, logging::logger_t& logger,
                    std::shared_ptr<request_context_t> request_context, api::vicodyn::balancer_ptr balancer);

    auto enqueue(const std::string& app_name, const hpack::headers_t& headers, std::string&& event,
                    const std::shared_ptr<backward_t>& backward) -> std::shared_ptr<dispatch<app_tag_t>>;

    auto error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void;
    auto retry_or_fail(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void;
    auto finish() -> void;

private:
    auto on_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void;
    auto on_choke(const hpack::headers_t& headers) -> void;
    auto on_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void;
    auto on_connection_error(const std::system_error& e) -> void;
    auto on_discard(const std::error_code& ec) -> void;

    auto choose_endpoint(const hpack::headers_t& headers, std::string&& event,
                    const std::shared_ptr<backward_t>& backward) -> bool;
};

} // namespace vicodyn
} // namespace cocaine