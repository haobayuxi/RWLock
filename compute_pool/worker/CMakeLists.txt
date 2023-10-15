# Author: Ming Zhang
# Copyright (c) 2022

set(WORKER_SRC
        handler.cc
        worker.cc)

add_library(worker STATIC
        ${WORKER_SRC}
        )

set_target_properties(worker PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(worker rwlock tatp_db tatp_txn micro_db micro_txn)
