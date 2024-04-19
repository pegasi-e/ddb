cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME kafkaredo)

cmake_print_variables(CMAKE_SYSTEM_NAME)
cmake_print_variables(CMAKE_SYSTEM_PROCESSOR)

option(OSX_BUILD_UNIVERSAL "Build both architectures on OSX and create a single binary containing both." FALSE)

include(CMakePrintHelpers)

cmake_print_variables(APPLE)
cmake_print_variables(OSX_BUILD_UNIVERSAL)

if (OSX_BUILD_UNIVERSAL)
    if (NOT APPLE)
        error("This only makes sense on OSX")
    endif()
    SET(CMAKE_OSX_ARCHITECTURES "x86_64;arm64" CACHE STRING "Build architectures for Mac OS X" FORCE)
    set(CMAKE_OSX_DEPLOYMENT_TARGET 11.0 CACHE STRING "Minimum OS X deployment version" FORCE)
endif()

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
