// Author: Ming Zhang
// Copyright (c) 2022

#include "handler.h"

std::atomic<uint64_t> tx_id_generator;
std::atomic<uint64_t> connected_t_num;

std::vector<t_id_t> tid_vec;
std::vector<double> attemp_tp_vec;
std::vector<double> tp_vec;
std::vector<double> medianlat_vec;
std::vector<double> taillat_vec;
std::vector<double> lock_durations;
std::vector<uint64_t> total_try_times;
std::vector<uint64_t> total_commit_times;
uint64_t micro_commit[100];
bool running;

void Handler::test() {
  char* local_mem = (char*)malloc(8192);
  // connect rdma
  auto* global_meta_man = new MetaManager();
  RDMA_ASSERT(global_meta_man->global_rdma_ctrl->register_memory(
      CLIENT_MR_ID, local_mem, 8192, global_meta_man->opened_rnic));
  QPManager* qp_man = new QPManager(0);
  qp_man->BuildQPConnection(global_meta_man);
  RDMA_LOG(INFO) << "rdma connected";
  auto coro_id = 0;
  auto remote_offset = 0;
  auto qp = qp_man->data_qps[0];
  for (int i = 0; i < 10; i++) {
    memcpy(local_mem, (char*)&i, sizeof(int));
    // write
    auto rc = qp->post_send(IBV_WR_RDMA_WRITE, local_mem, sizeof(int),
                            remote_offset, IBV_SEND_SIGNALED, coro_id);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post write fail. rc=" << rc;
      return;
    }
    ibv_wc wc{};
    rc = qp->poll_till_completion(wc, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll write fail. rc=" << rc;
      return;
    }
    RDMA_LOG(INFO) << "rdma write success";
    memset(local_mem, 0, sizeof(int));
    // read
    rc = qp->post_send(IBV_WR_RDMA_READ, local_mem, sizeof(int), remote_offset,
                       IBV_SEND_SIGNALED, coro_id);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: post read fail. rc=" << rc;
      return;
    }
    ibv_wc wc1{};
    rc = qp->poll_till_completion(wc1, no_timeout);
    if (rc != SUCC) {
      RDMA_LOG(ERROR) << "client: poll read fail. rc=" << rc;
      return;
    }
    int result = 0;
    memcpy(&result, local_mem, sizeof(int));
    // print
    RDMA_LOG(INFO) << "read" << result;
  }
}

// void Handler::ConfigureComputeNode(int argc, char* argv[]) {
//   std::string config_file = "compute_node_config.json";
//   std::string system_name = std::string(argv[2]);
//   if (argc == 5) {
//     std::string s1 =
//         "sed -i '5c \"thread_num_per_machine\": " + std::string(argv[3]) +
//         ",' " + config_file;
//     std::string s2 = "sed -i '6c \"coroutine_num\": " + std::string(argv[4])
//     +
//                      ",' " + config_file;
//     system(s1.c_str());
//     system(s2.c_str());
//   }
//   // Customized test without modifying configs
//   int txn_system_value = 0;
//   if (system_name.find("farm") != std::string::npos) {
//     txn_system_value = 0;
//   } else if (system_name.find("drtm") != std::string::npos) {
//     txn_system_value = 1;
//   } else if (system_name.find("ford") != std::string::npos) {
//     txn_system_value = 2;
//   } else if (system_name.find("local") != std::string::npos) {
//     txn_system_value = 3;
//   }
//   std::string s =
//       "sed -i '8c \"txn_system\": " + std::to_string(txn_system_value) + ",'
//       " + config_file;
//   system(s.c_str());
//   return;
// }

//
//

void Handler::GenThreads(std::string bench_name) {
  std::string config_filepath = "compute_node_config.json";
  auto json_config = JsonConfig::load_file(config_filepath);
  auto client_conf = json_config.get("local_compute_node");
  node_id_t machine_num = (node_id_t)client_conf.get("machine_num").get_int64();
  node_id_t machine_id = (node_id_t)client_conf.get("machine_id").get_int64();
  uint64_t lease = (uint64_t)client_conf.get("lease").get_uint64();
  t_id_t thread_num_per_machine =
      (t_id_t)client_conf.get("thread_num_per_machine").get_int64();
  const int coro_num = (int)client_conf.get("coroutine_num").get_int64();
  assert(machine_id >= 0 && machine_id < machine_num);
  RDMA_LOG(INFO) << "thread num=" << thread_num_per_machine
                 << "coro_num num = " << coro_num;
  /* Start working */
  tx_id_generator = 0;  // Initial transaction id == 0
  connected_t_num = 0;  // Sync all threads' RDMA QP connections
  auto thread_arr = new std::thread[thread_num_per_machine];
  running = true;

  auto* global_meta_man = new MetaManager();
  RDMA_LOG(INFO) << "Alloc local memory: "
                 << (size_t)(thread_num_per_machine * PER_THREAD_ALLOC_SIZE) /
                        (1024 * 1024)
                 << " MB. Waiting...";
  auto* global_rdma_region =
      new RDMARegionAllocator(global_meta_man, thread_num_per_machine);

  auto* param_arr = new struct thread_params[thread_num_per_machine];
  memset(micro_commit, 0, 100 * sizeof(uint64_t));
  // TATP* tatp_client = nullptr;
  // TPCC* tpcc_client = nullptr;

  // if (bench_name == "tatp") {
  //   tatp_client = new TATP();
  //   total_try_times.resize(TATP_TX_TYPES, 0);
  //   total_commit_times.resize(TATP_TX_TYPES, 0);
  // } else if (bench_name == "tpcc") {
  //   tpcc_client = new TPCC();
  //   total_try_times.resize(TPCC_TX_TYPES, 0);
  //   total_commit_times.resize(TPCC_TX_TYPES, 0);
  // }

  RDMA_LOG(INFO) << "Spawn threads to execute...";
  RDMA_LOG(INFO) << "lease = " << lease;
  struct timespec msr_start, msr_end;
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    param_arr[i].thread_local_id = i;
    param_arr[i].lease = lease;
    param_arr[i].thread_global_id = (machine_id * thread_num_per_machine) + i;
    param_arr[i].coro_num = coro_num;
    param_arr[i].bench_name = bench_name;
    param_arr[i].global_meta_man = global_meta_man;
    // param_arr[i].global_status = global_vcache;
    // param_arr[i].global_lcache = global_lcache;
    param_arr[i].global_rdma_region = global_rdma_region;
    param_arr[i].thread_num_per_machine = thread_num_per_machine;
    param_arr[i].total_thread_num = thread_num_per_machine * machine_num;
    // thread_arr[i] =
    //     std::thread(run_thread, &param_arr[i], tatp_client, tpcc_client);
    thread_arr[i] = std::thread(run_thread, &param_arr[i]);
    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      RDMA_LOG(WARNING) << "Error calling pthread_setaffinity_np: " << rc;
    }
  }
  sleep(10);
  // memset(micro_commit, 0, 100 * sizeof(uint64_t));

  clock_gettime(CLOCK_REALTIME, &msr_start);
  sleep(10);
  running = false;
  for (t_id_t i = 0; i < thread_num_per_machine; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }

  clock_gettime(CLOCK_REALTIME, &msr_end);
  // double msr_usec = (msr_end.tv_sec - msr_start.tv_sec) * 1000000 +
  // (double) (msr_end.tv_nsec - msr_start.tv_nsec) / 1000;
  double msr_sec = (msr_end.tv_sec - msr_start.tv_sec) +
                   (double)(msr_end.tv_nsec - msr_start.tv_nsec) / 1000000000;

  // auto total_msr_us = msr_sec * 1000000;

  RDMA_LOG(INFO) << "DONE";
  auto total = 0;
  for (int i = 0; i < thread_num_per_machine; i++) {
    total += micro_commit[i];
  }

  RDMA_LOG(INFO) << "committed " << total << "time " << msr_sec
                 << " persecond=" << total / msr_sec;

  delete[] param_arr;
  delete global_rdma_region;
  delete global_meta_man;
  // if (tatp_client) delete tatp_client;
  // if (tpcc_client) delete tpcc_client;
}

//
//
//

// void Handler::OutputResult(std::string bench_name, std::string system_name) {
//   std::string results_cmd = "mkdir -p bench_results/" + bench_name;
//   system(results_cmd.c_str());
//   std::ofstream of, of_detail, of_abort_rate;
//   std::string res_file = "bench_results/" + bench_name + "/result.txt";
//   std::string detail_res_file =
//       "bench_results/" + bench_name + "/detail_result.txt";
//   std::string abort_rate_file =
//       "bench_results/" + bench_name + "/abort_rate.txt";

//   of.open(res_file.c_str(), std::ios::app);
//   of_detail.open(detail_res_file.c_str(), std::ios::app);
//   of_abort_rate.open(abort_rate_file.c_str(), std::ios::app);

//   of_detail << system_name << std::endl;
//   of_detail << "tid attemp_tp tp 50lat 99lat" << std::endl;

//   of_abort_rate << system_name << " tx_type try_num commit_num abort_rate"
//                 << std::endl;

//   double total_attemp_tp = 0;
//   double total_tp = 0;
//   double total_median = 0;
//   double total_tail = 0;

//   for (int i = 0; i < tid_vec.size(); i++) {
//     of_detail << tid_vec[i] << " " << attemp_tp_vec[i] << " " << tp_vec[i]
//               << " " << medianlat_vec[i] << " " << taillat_vec[i] <<
//               std::endl;
//     total_attemp_tp += attemp_tp_vec[i];
//     total_tp += tp_vec[i];
//     total_median += medianlat_vec[i];
//     total_tail += taillat_vec[i];
//   }

//   size_t thread_num = tid_vec.size();

//   double avg_median = total_median / thread_num;
//   double avg_tail = total_tail / thread_num;

//   std::sort(medianlat_vec.begin(), medianlat_vec.end());
//   std::sort(taillat_vec.begin(), taillat_vec.end());

//   of_detail << total_attemp_tp << " " << total_tp << " " << medianlat_vec[0]
//             << " " << medianlat_vec[thread_num - 1] << " " << avg_median << "
//             "
//             << taillat_vec[0] << " " << taillat_vec[thread_num - 1] << " "
//             << avg_tail << std::endl;

//   of << system_name << " " << total_attemp_tp / 1000 << " " << total_tp /
//   1000
//      << " " << avg_median << " " << avg_tail << std::endl;

//   if (bench_name == "tatp") {
//     for (int i = 0; i < TATP_TX_TYPES; i++) {
//       of_abort_rate << TATP_TX_NAME[i] << " " << total_try_times[i] << " "
//                     << total_commit_times[i] << " "
//                     << (double)(total_try_times[i] - total_commit_times[i]) /
//                            (double)total_try_times[i]
//                     << std::endl;
//     }
//   } else if (bench_name == "smallbank") {
//     for (int i = 0; i < SmallBank_TX_TYPES; i++) {
//       of_abort_rate << SmallBank_TX_NAME[i] << " " << total_try_times[i] << "
//       "
//                     << total_commit_times[i] << " "
//                     << (double)(total_try_times[i] - total_commit_times[i]) /
//                            (double)total_try_times[i]
//                     << std::endl;
//     }
//   } else if (bench_name == "tpcc") {
//     for (int i = 0; i < TPCC_TX_TYPES; i++) {
//       of_abort_rate << TPCC_TX_NAME[i] << " " << total_try_times[i] << " "
//                     << total_commit_times[i] << " "
//                     << (double)(total_try_times[i] - total_commit_times[i]) /
//                            (double)total_try_times[i]
//                     << std::endl;
//     }
//   }

//   of_detail << std::endl;
//   of_abort_rate << std::endl;

//   of.close();
//   of_detail.close();
//   of_abort_rate.close();

//   std::cerr << system_name << " " << total_attemp_tp / 1000 << " "
//             << total_tp / 1000 << " " << avg_median << " " << avg_tail
//             << std::endl;

//   // Open it when testing the duration
// #if LOCK_WAIT
//   if (bench_name == "MICRO") {
//     // print avg lock duration
//     std::string file = "bench_results/" + bench_name +
//     "/avg_lock_duration.txt"; of.open(file.c_str(), std::ios::app);

//     double total_lock_dur = 0;
//     for (int i = 0; i < lock_durations.size(); i++) {
//       total_lock_dur += lock_durations[i];
//     }

//     of << system_name << " " << total_lock_dur / lock_durations.size()
//        << std::endl;
//     std::cerr << system_name
//               << " avg_lock_dur: " << total_lock_dur / lock_durations.size()
//               << std::endl;
//   }
// #endif
// }
