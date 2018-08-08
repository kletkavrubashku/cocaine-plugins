#pragma once

#include <cocaine/hpack/header.hpp>
#include "cocaine/vicodyn/proxy/safe_stream.hpp"

namespace cocaine {
namespace vicodyn {

class buffer_t {
    std::string event_;
    hpack::headers_t headers_;
    std::vector<std::pair<hpack::headers_t, std::string>> chunks_;
    boost::optional<hpack::headers_t> choke_;
    bool overfull_ = false;
    std::size_t capacity_;

public:
    buffer_t(std::size_t capacity);

    auto overfull() const -> bool;

    auto store_event(const hpack::headers_t &headers, const std::string &event) -> void;
    auto headers() const -> const hpack::headers_t&;
    auto event() const -> const std::string&;

    auto chunk(const hpack::headers_t &headers, const std::string &data) -> void;
    auto choke(const hpack::headers_t &headers) -> void;

    auto flush(safe_stream_t &stream) const -> void;
    auto clear() -> void;

private:
    auto try_account(const hpack::headers_t &headers, std::size_t data_size = 0) -> bool;
};

} // namespace vicodyn
} // namespace cocaine