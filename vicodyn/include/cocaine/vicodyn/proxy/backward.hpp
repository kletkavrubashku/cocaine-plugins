#pragma once

#include "cocaine/vicodyn/proxy/discardable.hpp"
#include "cocaine/vicodyn/proxy/forward.hpp"
#include "cocaine/vicodyn/proxy/safe_stream.hpp"
#include "cocaine/vicodyn/request_context.hpp"

namespace cocaine {
namespace vicodyn {

class backward_t : public std::enable_shared_from_this<backward_t>  {
    discardable_dispatch_t dispatch_;
    safe_stream_t stream_;
    std::atomic_bool started_;

    std::weak_ptr<forward_t> forward_stream_;

    logging::logger_t& logger_;
    const std::shared_ptr<request_context_t> request_context_;

public:
    backward_t(const std::string& name, upstream<app_tag_t>&& stream, logging::logger_t& logger,
                    std::shared_ptr<request_context_t> request_context);

    auto started() const -> bool;
    auto error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void;

    auto shared_dispatch() const -> std::shared_ptr<dispatch<app_tag_t>>;

private:
    auto on_chunk(const hpack::headers_t& headers, std::string&& chunk) -> void;
    auto on_choke(const hpack::headers_t& headers) -> void;
    auto on_error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> void;

    auto on_connection_error(const std::system_error& e) -> void;
    auto on_discard(const std::error_code& ec) -> void;
};

} // namespace vicodyn
} // namespace cocaine