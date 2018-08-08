#include "cocaine/vicodyn/proxy/endpoint.hpp"

#include "cocaine/repository/vicodyn/balancer.hpp"

#include <blackhole/wrapper.hpp>

namespace cocaine {
namespace vicodyn {

endpoint_t::endpoint_t(const std::string& dispatch_name,api::vicodyn::balancer_ptr balancer,std::size_t buffer_capacity,
                std::shared_ptr<request_context_t> request_context, logging::logger_t& logger)
    : buffer_(buffer_capacity)
    , backward_dispatch_(dispatch_name)
    , balancer_(std::move(balancer))
    , request_context_(std::move(request_context))
    , base_logger_(logger) {
}

auto endpoint_t::enqueue(const std::string& app_name, const hpack::headers_t& headers, std::string&& event) -> void {
    app_name_ = app_name;
    buffer_.store_event(headers, event);
    if (!choose_endpoint(headers, std::move(event))) {
        retry();
    }
}

auto endpoint_t::finalize() -> void {

}

auto endpoint_t::chunk(const hpack::headers_t&, std::string&&) -> bool {
    return false;
}

auto endpoint_t::choke(const hpack::headers_t&) -> bool {
    return false;
}

auto endpoint_t::error(const hpack::headers_t&, std::error_code&&, std::string&&) -> bool {
    return false;
}

auto endpoint_t::set_started() -> bool {
    backward_started_ = true;
    return true;
}

auto endpoint_t::send_error_backward(const std::error_code& ec, const std::string& msg) -> bool {
    balancer_->on_error(*current_.peer, ec, msg);
    if (!balancer_->is_recoverable(ec)) {
        return true;
    }
    try {
        retry();
        return false;
    } catch (const std::system_error&) {
    }
    return true;
}

auto endpoint_t::choose_endpoint(const hpack::headers_t& headers, std::string&& event) -> bool {
    current_.peer = balancer_->choose_peer(request_context_, headers, event);
    current_.logger = std::make_unique<blackhole::wrapper_t>(base_logger_,
                    blackhole::attributes_t{{"peer", current_.peer->uuid()}});
    current_.start_time = clock_t::now();
    request_context_->mark_used_peer(current_.peer);
    try {
        auto shared_dispatch = std::shared_ptr<dispatch<app_tag_t>>(shared_from_this(), &backward_dispatch_);
        auto upstream = current_.peer->open_stream<io::node::enqueue>(std::move(shared_dispatch), headers, app_name_,
                        std::move(event));
        current_.forward_stream = safe_stream_t(std::move(upstream));
    } catch (const std::system_error&) {
        return false;
    }
    return true;
}

auto endpoint_t::retry() -> void {
    if (backward_started_) {
        throw error_t("retry is forbidden - response chunk was sent");
    }
    if (current_.error_sent) {
        throw error_t("retry is forbidden - client sent error");
    }
    if (buffer_.overfull()) {
        throw error_t("retry is forbidden - buffer overflow");
    }
    do {
        if (request_context_->retry_count() >= balancer_->retry_count()) {
            throw error_t("retry is forbidden - maximum number of tries reached");
        }
        request_context_->add_checkpoint("retry");
        request_context_->register_retry();
    } while (!choose_endpoint(buffer_.headers(), std::string(buffer_.event())));
    buffer_.flush(current_.forward_stream);
    request_context_->add_checkpoint("after_retry");
}

} // namespace vicodyn
} // namespace cocaine
