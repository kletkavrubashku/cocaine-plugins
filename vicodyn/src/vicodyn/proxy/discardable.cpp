#include "cocaine/vicodyn/proxy/discardable.hpp"

namespace cocaine {
namespace vicodyn {

discardable_dispatch_t::discardable_dispatch_t(const std::string& name)
    : dispatch<app_tag_t>(name) {
}

auto discardable_dispatch_t::discard(const std::error_code& ec) -> void {
    if(discarder != nullptr) {
        discarder(ec);
    }
}

auto discardable_dispatch_t::on_discard(discarder_t d) -> void {
    discarder = std::move(d);
}

} // namespace vicodyn
} // namespace cocaine