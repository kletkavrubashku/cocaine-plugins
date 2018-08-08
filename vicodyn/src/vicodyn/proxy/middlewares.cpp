#include "cocaine/vicodyn/proxy/middlewares.hpp"

namespace cocaine {
namespace vicodyn {

catcher_t::catcher_t(on_catch_t on_catch)
    : on_catch_(std::move(on_catch)) {
}

on_error_t::on_error_t(condition_t condition_)
    : condition_(std::move(condition_)) {
}

on_success_t::on_success_t(condition_t condition)
    : condition_(std::move(condition)) {
}

} // namespace vicodyn
} // namespace cocaine