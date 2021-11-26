cmake_minimum_required(VERSION 3.10)
message(STATUS "Building using CMake version: ${CMAKE_VERSION}")

project(pg2arrow VERSION 0.1)

# -DOPENSSL_ROOT_DIR=/usr/local/opt/openssl@3
find_package(Arrow REQUIRED)
message(STATUS "Building using Arrow version: ${ARROW_VERSION}")

find_package(Parquet REQUIRED
    PATHS ${Arrow_DIR}
    NO_DEFAULT_PATH
)
message(STATUS "Building using Parquet version: ${PARQUET_VERSION}")

find_package(PostgreSQL REQUIRED)
message(STATUS "Building using PostgreSQL version: ${PostgreSQL_VERSION_STRING}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED True)

add_library(pg2arrow SHARED src/builder.cc src/schema.cc src/sql_copy.cc)
target_link_libraries(pg2arrow PRIVATE arrow_shared PostgreSQL::PostgreSQL)
set_target_properties(pg2arrow PROPERTIES VERSION ${PROJECT_VERSION})
set_target_properties(pg2arrow PROPERTIES SOVERSION 1)
set_target_properties(pg2arrow PROPERTIES PUBLIC_HEADER pg2arrow.h)

add_executable(pg2parquet src/main.cc)
target_link_libraries(pg2parquet PRIVATE pg2arrow arrow_shared parquet_shared PostgreSQL::PostgreSQL)
