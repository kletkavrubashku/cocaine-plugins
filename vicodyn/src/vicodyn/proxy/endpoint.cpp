#include "cocaine/vicodyn/proxy/endpoint.hpp"

#include "cocaine/repository/vicodyn/balancer.hpp"

#include <blackhole/wrapper.hpp>
#include <cocaine/format.hpp>
#include <cocaine/format/exception.hpp>

namespace cocaine {
namespace vicodyn {

endpoint_t::endpoint_t(const std::string& dispatch_name, const std::string& app_name, std::size_t buffer_capacity,
        api::vicodyn::balancer_ptr balancer, peers_t& peers, std::shared_ptr<request_context_t> request_context,
        logging::logger_t& logger)
    : buffer_(buffer_capacity)
    , backward_dispatch_(dispatch_name)
    , app_name_(app_name)
    , peers_(peers)
    , balancer_(std::move(balancer))
    , request_context_(std::move(request_context))
    , base_logger_(logger) {
}

auto endpoint_t::enqueue(const hpack::headers_t& headers, std::string&& event,
        std::weak_ptr<vicodyn_dispatch_t> life_cycle) -> void {
    life_cycle_ = std::move(life_cycle);
    buffer_.store_event(headers, event);
    if (!choose_endpoint(headers, std::move(event))) {
        peer_->schedule_reconnect();
        retry(true);
    }
}

auto endpoint_t::chunk(const hpack::headers_t& headers, std::string&& data) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!backward_started_ && !error_sent_) {
        buffer_.chunk(headers, data);
    }
    return catch_connection_error([&]() {
        return forward_stream_.chunk(headers, std::move(data));
    });
}

auto endpoint_t::choke(const hpack::headers_t& headers) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    if (!backward_started_ && !error_sent_) {
        buffer_.choke(headers);
    }
    return catch_connection_error([&]() {
        return forward_stream_.choke(headers);
    });
}

auto endpoint_t::error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    error_sent_ = true;
    return forward_stream_.error(headers, std::move(ec), std::move(msg));
}

auto endpoint_t::on_discard(discardable_dispatch_t::discarder_t discarder) -> void {
    backward_dispatch_.on_discard(std::move(discarder));
}

auto endpoint_t::set_started() -> void {
    backward_started_ = true;
}

auto endpoint_t::send_error_backward(const std::error_code& ec, const std::string& msg) -> bool {
    std::lock_guard<std::mutex> lock(mutex_);
    balancer_->on_error(peer_, ec, msg);
    if (!balancer_->is_recoverable(peer_, ec)) {
        return true;
    }
    try {
        retry(false);
        return false;
    } catch (const std::system_error& e) {
        COCAINE_LOG_WARNING(logger_, "retry failed - {}", e);
    }
    return true;
}

auto endpoint_t::store_elapsed_time() -> void {
    std::lock_guard<std::mutex> lock(mutex_);
    peers_.add_app_request_duration(peer_->uuid(), app_name_, clock_t::now() - start_time_);
}

auto endpoint_t::choose_endpoint(const hpack::headers_t& headers, std::string&& event) -> bool {
    static const std::string total_counter_name = "total_attempts";

    COCAINE_LOG_DEBUG(base_logger_, "choose endpoint");
    peer_ = balancer_->choose_peer(request_context_, headers, event);
    request_context_->mark_used_peer(peer_);
    ++request_context_->counter(total_counter_name);
    logger_ = std::make_unique<blackhole::wrapper_t>(base_logger_, blackhole::attributes_t{{"peer", peer_->uuid()}});
    start_time_ = clock_t::now();
    try {
        COCAINE_LOG_DEBUG(logger_, "open stream");
        auto shared_dispatch = std::shared_ptr<dispatch<app_tag_t>>(life_cycle_.lock(), &backward_dispatch_);
        auto upstream = peer_->open_stream<io::node::enqueue>(std::move(shared_dispatch), headers, app_name_,
                std::move(event));
        forward_stream_ = safe_stream_t(std::move(upstream));
    } catch (const std::system_error& e) {
        COCAINE_LOG_WARNING(logger_, "failed to open stream to peer - {}", e);
        return false;
    }
    return true;
}

auto endpoint_t::retry(bool reconnect) -> void {
    COCAINE_LOG_DEBUG(logger_, "retry");
    if (backward_started_) {
        throw error_t("retry is forbidden - response chunk was sent");
    }
    if (error_sent_) {
        throw error_t("retry is forbidden - client sent error");
    }
    if (buffer_.overfull()) {
        throw error_t("retry is forbidden - buffer overflow");
    }
    while (true) {
        request_context_->add_checkpoint("retry");
        if (choose_endpoint(buffer_.headers(), std::string(buffer_.event()))) {
            break;
        } else if (reconnect) {
            peer_->schedule_reconnect();
        }
    }
    buffer_.flush(forward_stream_);
    request_context_->add_checkpoint("after_retry");
}

template<typename F>
auto endpoint_t::catch_connection_error(F f) -> bool {
    try {
        return f();
    } catch (const std::system_error& e) {
        COCAINE_LOG_WARNING(logger_, "catched connection error to peer - {}", e);
        peer_->schedule_reconnect();
        // TODO: Retry
    }
    return false;
}

} // namespace vicodyn
} // namespace cocaine
