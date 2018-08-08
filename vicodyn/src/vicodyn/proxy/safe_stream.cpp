#include "cocaine/vicodyn/proxy/safe_stream.hpp"

namespace cocaine {
namespace vicodyn {

safe_stream_t::safe_stream_t(upstream<app_tag_t>&& stream)
    : stream_(std::move(stream)) {
}

auto safe_stream_t::operator=(safe_stream_t&& that) -> safe_stream_t& {
    stream_ = std::move(that.stream_);
    closed_ = that.closed_.load();
    return *this;
}

auto safe_stream_t::chunk(const hpack::headers_t& headers, std::string&& data) -> bool {
    if(!closed_ && stream_) {
        stream_ = stream_->send<protocol_t::chunk>(headers, std::move(data));
        return true;
    }
    return false;
}

auto safe_stream_t::choke(const hpack::headers_t& headers) -> bool {
    if(!closed_.exchange(true) && stream_) {
        stream_->send<protocol_t::choke>(headers);
        return true;
    }
    return false;
}

auto safe_stream_t::error(const hpack::headers_t& headers, std::error_code&& ec, std::string&& msg) -> bool {
    if(!closed_.exchange(true) && stream_) {
        stream_->send<protocol_t::error>(headers, ec, std::move(msg));
        return true;
    }
    return false;
}

} // namespace vicodyn
} // namespace cocaine
