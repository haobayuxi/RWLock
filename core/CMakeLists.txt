# Author: Ming Zhang
# Copyright (c) 2022

set(CONNECTION_SRC
        connection/meta_manager.cc
        connection/qp_manager.cc
        )

set(DTX_SRC
        dtx/doorbell.cc
        dtx/read_only.cc
        dtx/read_write.cc
        dtx/dtx.cc
        dtx/dtx_check_ro.cc
        dtx/dtx_check_rw.cc
        # dtx/drtm.cc
        # dtx/dlmr.cc
        )

set(SCHEDULER_SRC
        scheduler/corotine_scheduler.cc
        )

set(SMART_SRC
        smart/initiator.cpp 
        smart/resource_manager.cpp 
        smart/thread.cpp 
        )

add_library(rwlock STATIC
        ${CONNECTION_SRC}
        ${SMART_SRC}
        ${SCHEDULER_SRC}
        ${DTX_SRC}
        )



set_target_properties(rwlock PROPERTIES LINKER_LANGUAGE CXX)

target_link_libraries(rwlock rlib pthread boost_coroutine boost_context boost_system numa pthread ibverbs mlx5)