cmake_minimum_required(VERSION 3.16)
project(tbb_bench CXX)
set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -O3 -march=native")
find_package(TBB REQUIRED)

add_executable(all_to_all src/all_to_all.cpp)
target_link_libraries(all_to_all TBB::tbb pthread)

add_executable(server_client src/server_client.cpp)
target_link_libraries(server_client TBB::tbb pthread)
