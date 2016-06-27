cmake_minimum_required (VERSION 2.8)

project (hls_proxy)

find_package(Boost REQUIRED COMPONENTS system filesystem log log_setup thread chrono program_options)
include_directories(Boost_INCLUDE_DIR)

include_directories(urdl/include)
add_definitions(-DURDL_HEADER_ONLY=1)

add_executable (hls_proxy hls_proxy.cpp)
add_definitions(-std=c++11)
target_link_libraries(hls_proxy ${Boost_LIBRARIES} -lcrypto -lssl -lpthread)
