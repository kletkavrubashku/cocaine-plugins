#pragma once

#include "cocaine/vicodyn/proxy/protocol.hpp"

namespace cocaine {
namespace vicodyn {

class discardable_dispatch_t : public dispatch<app_tag_t> {
public:
    using discarder_t = std::function<void(const std::error_code&)>;

    discardable_dispatch_t(const std::string& name);

    auto discard(const std::error_code& ec) -> void override;
    auto on_discard(discarder_t d) -> void;

private:
    discarder_t discarder;
};

} // namespace vicodyn
} // namespace cocaine
