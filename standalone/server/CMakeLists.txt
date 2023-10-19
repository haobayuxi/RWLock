# Author: Ming Zhang
# Copyright (c) 2022

set(SERVER_SOURCE server.cc)
add_executable(rw_server ${SERVER_SOURCE})
target_link_libraries(rw_server pthread rwbench)
