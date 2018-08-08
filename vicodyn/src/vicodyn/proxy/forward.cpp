#include "cocaine/vicodyn/proxy/forward.hpp"

#include "cocaine/vicodyn/proxy/middlewares.hpp"
#include "cocaine/repository/vicodyn/balancer.hpp"

#include <blackhole/wrapper.hpp>

namespace cocaine {
namespace vicodyn {

forward_t::forward_t(const std::string& name, std::size_t buffer_capacity, logging::logger_t& logger,
                std::shared_ptr<request_context_t> request_context, api::vicodyn::balancer_ptr balancer)
        : buffer_(buffer_capacity)
        , dispatch_(name + "/forward")
        , request_context_(std::move(request_context))
        , base_logger_(logger)
        , balancer_(std::move(balancer)) {
    namespace ph = std::placeholders;

    locker_t locker(mutex_);
    catcher_t catcher(std::bind(&forward_t::on_connection_error, this, ph::_1));

    dispatch_.on<protocol_t::chunk>()
                    .with_middleware(catcher)
                    .with_middleware(locker)
                    .execute(std::bind(&forward_t::on_chunk, this, ph::_1, ph::_2));

    dispatch_.on<protocol_t::choke>()
                    .with_middleware(catcher)
                    .with_middleware(locker)
                    .execute(std::bind(&forward_t::on_choke, this, ph::_1));

    dispatch_.on<protocol_t::error>()
                    .with_middleware(catcher)
                    .with_middleware(locker)
                    .execute(std::bind(&forward_t::on_error, this, ph::_1, ph::_2, ph::_3));

    dispatch_.on_discard(std::bind(&forward_t::on_discard, this, ph::_1));
}

auto forward_t::enqueue(const std::string& app_name, const hpack::headers_t& headers, std::string&& event,
                const std::shared_ptr<backward_t>& backward) -> std::shared_ptr<dispatch<app_tag_t>> {
    app_name_ = app_name;
    buffer_.store_event(headers, event);
    backward_stream_ = backward;
    if (!choose_endpoint(headers, event)) {
        retry();
    }
    return std::shared_ptr<dispatch<app_tag_t>>(shared_from_this(), &dispatch_);
}

auto forward_t::error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {

}

auto forward_t::retry_or_fail(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
    apply_shared([&](const data_t& safe) {
        proxy.balancer->on_error(safe.endpoint->peer(), ec, msg);
    });
    if (proxy.balancer->is_recoverable(ec)) {
        retry_or_fail();
    } else {
        if (!client_stream.error(headers, ec, msg)) {
            apply([&](data_t& safe) {
                //safe.endpoint->error(headers, ec, msg);
            });
        }
        request_context->add_checkpoint("after_berror");
        request_context->fail(ec, msg);
    }

    //////////

    COCAINE_LOG_INFO(logger_, "retrying");
    auto backward = backward_stream_.lock();
    if (backward && backward->started()) {
        throw error_t("retry is forbidden - response chunk was sent");
    }
    if (error_sent_) {
        throw error_t("retry is forbidden - client sent error");
    }
    if (buffer_.overfull()) {
        throw error_t("retry is forbidden - buffer overflow");
    }
    do {
        request_context_->register_retry();
        if (request_context_->retry_count() >= balancer_->retry_count()) {
            throw error_t("retry is forbidden - maximum number of tries reached");
        }
        request_context_->add_checkpoint("retry");
    } while (!choose_endpoint(buffer_.headers(), buffer_.event()));
    buffer_.flush(stream_);
    request_context_->add_checkpoint("after_retry");
}

auto forward_t::finish() -> void {
    peers.add_app_request_duration(peer_->uuid(), app_name_, clock_t::now() - start_time_);
}

auto forward_t::on_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void {
    auto backward = backward_stream_.lock();
    if (!backward || !backward->started()) {
        buffer_.chunk(headers, chunk);
    }
    stream_.chunk(headers, std::move(chunk));
    request_context_->add_checkpoint("after_fchunk");
}

auto forward_t::on_choke(const hpack::headers_t& headers) -> void {
    auto backward = backward_stream_.lock();
    if (!backward || !backward->started()) {
        buffer_.choke(headers);
    }
    stream_.choke(headers);
    request_context_->add_checkpoint("after_fchoke");
}

auto forward_t::on_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
    error_sent_ = true;
    stream_.error(headers, std::move(ec), std::move(msg));
    request_context_->add_checkpoint("after_ferror");
}

auto forward_t::on_connection_error(const std::system_error& e) -> void {
    // This will lead to backward_t::on_discard
    peer_->schedule_reconnect();
}

auto forward_t::on_discard(const std::error_code& ec) -> void {
    try {
        if (auto backward = backward_stream_.lock()) {
            //backward->error(headers, std::move(ec), std::move(msg));
        }
    } catch (const std::system_error& error) {

    }
}

auto forward_t::choose_endpoint(const hpack::headers_t& headers, std::string&& event,
                const std::shared_ptr<backward_t>& backward) -> bool {
    peer_ = balancer_->choose_peer(request_context_, headers, event);
    logger_ = std::make_unique<blackhole::wrapper_t>(base_logger_, blackhole::attributes_t{{"peer", peer_->uuid()}})
    start_time_ = clock_t::now();
    request_context_->mark_used_peer(peer_);
    try {
        auto upstream = peer_->open_stream<io::node::enqueue>(backward->shared_dispatch(), std::move(headers),
                        app_name_, std::move(event));
        stream_ = safe_stream_t(std::move(upstream));
    } catch (const std::system_error& e) {
        return false;
    }
    return true;
}

} // namespace vicodyn
} // namespace cocaine