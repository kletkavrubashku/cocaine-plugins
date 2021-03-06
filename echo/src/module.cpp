#include "cocaine/service/echo.hpp"

#include <cocaine/repository.hpp>
#include <cocaine/repository/service.hpp>

extern "C" {
    auto validation() -> cocaine::api::preconditions_t {
        return cocaine::api::preconditions_t{COCAINE_MAKE_VERSION(0, 12, 0)};
    }

    auto initialize(cocaine::api::repository_t& repository) -> void {
        repository.insert<cocaine::service::echo_t>("echo");
    }
}
