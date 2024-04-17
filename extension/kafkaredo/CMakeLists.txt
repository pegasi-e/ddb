cmake_minimum_required(VERSION 2.8.12)

# Set extension name here
set(TARGET_NAME kafkaredo)

# DuckDB's extension distribution supports vcpkg. As such, dependencies can be added in ./vcpkg.json and then
# used in cmake with find_package. Feel free to remove or replace with other dependencies.
# Note that it should also be removed from vcpkg.json to prevent needlessly installing it..
#find_package(OpenSSL REQUIRED)

set(EXTENSION_NAME ${TARGET_NAME}_extension)
set(LOADABLE_EXTENSION_NAME ${TARGET_NAME}_loadable_extension)

project(${TARGET_NAME})

find_package(rdkafka CONFIG REQUIRED)
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
