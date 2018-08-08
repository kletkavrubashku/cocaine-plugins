#include "cocaine/vicodyn/proxy/backward.hpp"

#include "cocaine/vicodyn/proxy/middlewares.hpp"

namespace cocaine {
namespace vicodyn {

backward_t::backward_t(const std::string& name, upstream<app_tag_t>&& stream, logging::logger_t& logger,
                std::shared_ptr<request_context_t> request_context)
        : dispatch_(name + "/backward")
        , stream_(std::move(stream))
        , logger_(logger)
        , request_context_(std::move(request_context)) {
    namespace ph = std::placeholders;

    flag_t flag(started_);
    catcher_t catcher(std::bind(&backward_t::on_connection_error, this, ph::_1));

    dispatch_.on<protocol_t::chunk>()
                    .with_middleware(flag)
                    .with_middleware(catcher)
                    .execute(std::bind(&backward_t::on_chunk, this, ph::_1, ph::_2));
    dispatch_.on<protocol_t::choke>()
                    .with_middleware(flag)
                    .with_middleware(catcher)
                    .execute(std::bind(&backward_t::on_choke, this, ph::_1));
    dispatch_.on<protocol_t::error>()
                    .with_middleware(flag)
                    .with_middleware(catcher)
                    .execute(std::bind(&backward_t::on_error, this, ph::_1, ph::_2, ph::_3));
    dispatch_.on_discard(std::bind(&backward_t::on_discard, this, ph::_1));
}

auto backward_t::started() const -> bool {
    return started_;
}

auto backward_t::error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
    stream_.error(headers, std::move(ec), std::move(msg));
}

auto backward_t::shared_dispatch() const -> std::shared_ptr<dispatch<app_tag_t>> {
    return std::shared_ptr<dispatch<app_tag_t>>(shared_from_this(), &dispatch_);
}

auto backward_t::on_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void {
    stream_.chunk(headers, std::move(chunk));
    request_context_->add_checkpoint("after_bchunk");
}

auto backward_t::on_choke(const hpack::headers_t& headers) -> void {
    if (!stream_.choke(headers)) {
        return;
    }
    if (auto forward = forward_stream_.lock()) {
        forward->finish();
    }
    request_context_->add_checkpoint("after_bchoke");
    request_context_->finish();
}

auto backward_t::on_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void {
    if (auto forward = forward_stream_.lock()) {
        forward->retry_or_fail(headers, std::move(ec), std::move(msg));
    }
}

auto backward_t::on_connection_error(const std::system_error& e) -> void {
    COCAINE_LOG_WARNING(logger_, "connection error with client - exit: {}", error::to_string(e));
    if (auto forward = forward_stream_.lock()) {
        forward->error({}, std::error_code(e.code()), e.what());
    }
}

auto backward_t::on_discard(const std::error_code& ec) -> void {
    if (auto forward = forward_stream_.lock()) {
        forward->retry_or_fail({}, std::error_code(ec), "peer is discarded");
    }
}

} // namespace vicodyn
} // namespace cocaine