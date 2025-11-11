cmake_minimum_required(VERSION 3.10)
project(kvs_service C)

set(CMAKE_C_STANDARD 99)

# === Paths ===
set(KVS_SDK_ROOT ${CMAKE_CURRENT_SOURCE_DIR})
set(KVS_CERT_PATH ${CMAKE_CURRENT_SOURCE_DIR}/certs/cert.pem)
set(KVS_LIB_PATH /root/copilot/lib/kvs)       # Final install location for .so files

# === Sources ===
set(KVS_SAMPLE_SRC
    ${KVS_SDK_ROOT}/samples/kvsWebrtcClientMasterGstSample.c
    ${KVS_SDK_ROOT}/samples/Common.c
)

# === Include Directories ===
include_directories(
    ${KVS_SDK_ROOT}/include
    ${KVS_SDK_ROOT}/src/include
    ${KVS_SDK_ROOT}/src
    ${KVS_SDK_ROOT}/src/lib
    ${KVS_SDK_ROOT}/open-source/include
    ${KVS_SDK_ROOT}/src/include/com/amazonaws/kinesis/video
    ${KVS_SDK_ROOT}/include/com/amazonaws/kinesis/video/webrtcclient
    ${KVS_SDK_ROOT}/src/include/com/amazonaws/kinesis/video/utils
    ${KVS_SDK_ROOT}/samples
)

# === Library search paths ===
link_directories(
    ${KVS_SDK_ROOT}/lib
    ${KVS_LIB_PATH}
    )

# === GStreamer ===
find_package(PkgConfig REQUIRED)
pkg_check_modules(GST REQUIRED gstreamer-1.0 gstreamer-app-1.0)
include_directories(${GST_INCLUDE_DIRS})
link_directories(${GST_LIBRARY_DIRS})
add_definitions(${GST_CFLAGS_OTHER})

# === Build executable ===
add_executable(kvs_service ${KVS_SAMPLE_SRC})

# === Link libraries ===
target_link_libraries(kvs_service
    PRIVATE
    kvsWebrtcClient
    kvsWebrtcSignalingClient
    kvsCommonLws
    kvspicUtils
    kvspicClient
    kvspicState
    kvspic
    websockets
    ssl
    crypto
    pthread
    m
    ami
    ${GST_LIBRARIES}
)

target_include_directories(kvs_service
    PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/../../amu/inc
)

# === RPATH setting ===
# Ensures the binary looks for .so files inside /root/copilot/lib/kvs at runtime
set_target_properties(kvs_service PROPERTIES
    BUILD_RPATH "${KVS_LIB_PATH}"
    INSTALL_RPATH "${KVS_LIB_PATH}"
    INSTALL_RPATH_USE_LINK_PATH TRUE
)

# === Install binary ===
install(TARGETS ${PROJECT_NAME}
        DESTINATION ${CMAKE_EXE_PATH}
)

# === Install libraries ===
file(GLOB KVS_SO_FILES
    "${KVS_SDK_ROOT}/lib/*.so*"
    # "${KVS_SDK_ROOT}/open-source/lib/libkvs*.so*"
)

file (GLOB KVS_WEBSOCKET
    "${KVS_SDK_ROOT}/lib/libwebsockets.so*"
)

install(FILES ${KVS_SO_FILES}
        DESTINATION ${CMAKE_LIB_PATH}/kvs)

install(FILES ${KVS_WEBSOCKET}
        DESTINATION ${CMAKE_LIB_PATH})

install(FILES  ${KVS_CERT_PATH}
        DESTINATION ${CMAKE_CFG_PATH})