// // Author: Ming Zhang
// // Copyright (c) 2022

// #pragma once

#include <atomic>
#include <cstdio>
#include <fstream>
#include <functional>
#include <memory>

#include "allocator/buffer_allocator.h"
#include "allocator/log_allocator.h"
#include "connection/qp_manager.h"
#include "scheduler/corotine_scheduler.h"
#include "util/debug.h"
#include "util/json_config.h"
// #include "dtx/dtx.h"
// #include "micro/micro_txn.h"
// #include "tatp/tatp_txn.h"
// #include "tpcc/tpcc_txn.h"
// #include "util/latency.h"
// #include "util/zipf.h"
#include "base/common.h"
// #include "cache/lock_status.h"
// #include "cache/version_status.h"
#include "connection/qp_manager.h"
struct thread_params {
  t_id_t thread_local_id;
  t_id_t thread_global_id;
  t_id_t thread_num_per_machine;
  t_id_t total_thread_num;
  MetaManager* global_meta_man;
  // VersionCache* global_status;
  // LockCache* global_lcache;
  RDMARegionAllocator* global_rdma_region;
  int coro_num;
  std::string bench_name;
};

// void run_thread(thread_params* params, TATP* tatp_client, TPCC* tpcc_client);

void run_thread(thread_params* params);
