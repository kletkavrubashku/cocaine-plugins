INCLUDE_DIRECTORIES(
        ${PROJECT_SOURCE_DIR}/vicodyn/include
        ${PROJECT_SOURCE_DIR}/node/include)

ADD_LIBRARY(vicodyn SHARED
        src/api/vicodyn/balancer.cpp
        src/gateway/vicodyn.cpp
        src/module.cpp
        src/vicodyn/balancer/simple.cpp
        src/vicodyn/error.cpp
        src/vicodyn/peer.cpp
        src/vicodyn/proxy/buffer.cpp
        src/vicodyn/proxy/discardable.cpp
        src/vicodyn/proxy/endpoint.cpp
        src/vicodyn/proxy/middlewares.cpp
        src/vicodyn/proxy/proxy.cpp
        src/vicodyn/proxy/safe_stream.cpp
        src/vicodyn/request_context.cpp
        )

ADD_DEFINITIONS("-DVICODYN_USE_DEBUG")
TARGET_LINK_LIBRARIES(vicodyn
        node
        msgpack
        blackhole
        cocaine-core
        cocaine-io-util
        metrics
        ${Boost_LIBRARIES})

SET_TARGET_PROPERTIES(vicodyn PROPERTIES
        PREFIX ""
        SUFFIX "${COCAINE_PLUGIN_SUFFIX}"
        COMPILE_FLAGS "-std=c++0x -Wall -Wextra -pedantic -Winit-self -Wold-style-cast -Woverloaded-virtual -Wctor-dtor-privacy -Wnon-virtual-dtor")

INSTALL(TARGETS vicodyn
        LIBRARY DESTINATION lib/cocaine
        COMPONENT runtime)

INSTALL(DIRECTORY
        include/cocaine
        DESTINATION include
        COMPONENT development)
