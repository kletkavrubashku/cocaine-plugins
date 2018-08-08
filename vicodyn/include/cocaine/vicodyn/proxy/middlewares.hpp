#pragma once

#include "cocaine/vicodyn/proxy/protocol.hpp"

#include <cocaine/hpack/header.hpp>

namespace cocaine {
namespace vicodyn {

class catcher_t {
    using on_catch_t = std::function<void(const std::system_error& e)>;
    on_catch_t on_catch_;

public:
    catcher_t(on_catch_t on_catch);

    template<typename F, typename Event, typename... Args>
    auto operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) -> void {
        try {
            fn(headers, std::forward<Args>(args)...);
        }
        catch(const std::system_error& e) {
            on_catch_(e);
        }
    }
};

class on_error_t {
    using condition_t = std::function<bool(const std::error_code& ec, const std::string& msg)>;
    condition_t condition_;

public:
    on_error_t(condition_t condition_);

    template<typename F, typename Event, typename... Args>
    inline typename std::enable_if<std::is_same<Event, protocol_t::error>::value, void>::type
    operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) {
        auto tie = std::tie(args...);
        const auto& ec = std::get<0>(tie);
        const auto& msg = std::get<1>(tie);
        if (condition_(ec, msg)) {
            fn(headers, std::forward<Args>(args)...);
        }
    }

    template<typename F, typename Event, typename... Args>
    inline typename std::enable_if<!std::is_same<Event, protocol_t::error>::value, void>::type
    operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) {
        fn(headers, std::forward<Args>(args)...);
    }
};

class on_success_t {
    using condition_t = std::function<bool()>;
    condition_t condition_;

public:
    on_success_t(condition_t condition);

    template<typename F, typename Event, typename... Args>
    inline typename std::enable_if<std::is_same<Event, protocol_t::error>::value, void>::type
    operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) {
        fn(headers, std::forward<Args>(args)...);
    }

    template<typename F, typename Event, typename... Args>
    inline typename std::enable_if<!std::is_same<Event, protocol_t::error>::value, void>::type
    operator()(F fn, Event, const hpack::headers_t& headers, Args&&... args) {
        if (condition_) {
            fn(headers, std::forward<Args>(args)...);
        }
    }
};

} // namespace vicodyn
} // namespace cocaine