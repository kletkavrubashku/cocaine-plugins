#include "cocaine/vicodyn/proxy/buffer.hpp"

namespace cocaine {
namespace vicodyn {

buffer_t::buffer_t(std::size_t capacity)
    : capacity_(capacity) {
}

auto buffer_t::overfull() const -> bool {
    return overfull_;
}

auto buffer_t::store_event(const hpack::headers_t &headers, const std::string &event) -> void {
    if (try_account(headers, event.size())) {
        headers_ = headers;
        event_ = event;
    }
}

auto buffer_t::event() const -> const std::string & {
    return event_;
}

auto buffer_t::headers() const -> const hpack::headers_t & {
    return headers_;
}

auto buffer_t::chunk(const hpack::headers_t &headers, const std::string &data) -> void {
    if (try_account(headers, data.size())) {
        chunks_.emplace_back(headers, data);
    }
}

auto buffer_t::choke(const hpack::headers_t &headers) -> void {
    if (try_account(headers)) {
        choke_ = headers;
    }
}

auto buffer_t::flush(safe_stream_t &stream) const -> void {
    for (const auto &chunk : chunks_) {
        stream.chunk(chunk.first, std::string(chunk.second));
    }
    if (choke_) {
        stream.choke(choke_.get());
    }
}

auto buffer_t::clear() -> void {
    event_.clear();
    event_.shrink_to_fit();
    headers_.clear();
    headers_.shrink_to_fit();
    chunks_.clear();
    chunks_.shrink_to_fit();
    choke_.reset();
}

auto buffer_t::try_account(const hpack::headers_t &headers, std::size_t data_size) -> bool {
    if (overfull()) {
        return false;
    }
    for (const auto &header : headers) {
        data_size += header.http2_size();
    }
    if (data_size > capacity_) {
        // If we exceed the buffer once, we'll disable it for the request forever, because it has became invalid.
        overfull_ = true;
        clear();
        return false;
    }
    capacity_ -= data_size;
    return true;
}

} // namespace vicodyn
} // namespace cocaine