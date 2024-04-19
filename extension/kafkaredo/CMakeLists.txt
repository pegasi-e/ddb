cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME kafkaredo)
project(${TARGET_NAME})

set(OS_NAME "unknown")
set(OS_ARCH "amd64")

include(CMakePrintHelpers)
cmake_print_variables(IS_ARM)
cmake_print_variables(CMAKE_SYSTEM_PROCESSOR)

string(REGEX MATCH "(arm64|aarch64)" IS_ARM "$ENV{CMAKE_SYSTEM_ARCH}")
string(REGEX MATCH "(Linux|linux)" IS_Linux "$ENV{CMAKE_SYSTEM_NAME}")

if (IS_ARM)
    message(STATUS "setting...")
    cmake_print_variables(IS_Linux)
    set(CMAKE_SYSTEM_PROCESSOR "arm64")
    set(VCPKG_TARGET_ARCHITECTURE arm64)
    set(VCPKG_CRT_LINKAGE static)
    set(VCPKG_LIBRARY_LINKAGE static)

    if (IS_Linux)
        set(VCPKG_CMAKE_SYSTEM_NAME Linux)
        set(VCPKG_DEFAULT_TRIPLET "arm64-linux")
        cmake_print_variables(VCPKG_DEFAULT_TRIPLET)
    endif ()
    set(CMAKE_CROSSCOMPILING 1)
endif ()

cmake_print_variables(CMAKE_SYSTEM_NAME)
cmake_print_variables(CMAKE_SYSTEM_PROCESSOR)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
#find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)



find_package(RdKafka CONFIG REQUIRED)
include_directories(include)

set(EXTENSION_SOURCES kafkaredo_extension.cpp kafkafs.cpp)

build_static_extension(${TARGET_NAME} ${EXTENSION_SOURCES})
build_loadable_extension(${TARGET_NAME} " " ${EXTENSION_SOURCES})

# Link OpenSSL in both the static library as the loadable extension
#target_link_libraries(${EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto RdKafka::rdkafka RdKafka::rdkafka++)
#target_link_libraries(${LOADABLE_EXTENSION_NAME} OpenSSL::SSL OpenSSL::Crypto RdKafka::rdkafka RdKafka::rdkafka++)
target_link_libraries(${EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)
target_link_libraries(${LOADABLE_EXTENSION_NAME} RdKafka::rdkafka RdKafka::rdkafka++)

install(
  TARGETS ${EXTENSION_NAME}
  EXPORT "${DUCKDB_EXPORT_SET}"
  LIBRARY DESTINATION "${INSTALL_LIB_DIR}"
  ARCHIVE DESTINATION "${INSTALL_LIB_DIR}")
