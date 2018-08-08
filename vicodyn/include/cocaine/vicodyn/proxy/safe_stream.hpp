#pragma once

#include "cocaine/vicodyn/proxy/protocol.hpp"

namespace cocaine {
namespace vicodyn {

class safe_stream_t {
    boost::optional<upstream<app_tag_t>> stream_;
    std::atomic_bool closed_{false};

public:
    safe_stream_t() = default;
    safe_stream_t(upstream<app_tag_t>&& stream);
    auto operator=(safe_stream_t&& that) -> safe_stream_t&;

    auto chunk(const hpack::headers_t& headers, std::string&& data) -> bool;
    auto choke(const hpack::headers_t& headers) -> bool;
    auto error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> bool;
};

} // namespace vicodyn
} // namespace cocaine
