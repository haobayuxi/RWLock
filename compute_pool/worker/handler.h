// Author: Ming Zhang
// Copyright (c) 2022

#pragma once

#include <unistd.h>

#include <algorithm>
#include <atomic>
#include <fstream>
#include <iostream>
#include <mutex>
#include <string>
#include <vector>

#include "connection/meta_manager.h"
#include "rlib/rdma_ctrl.hpp"
#include "util/json_config.h"
#include "worker/handler.h"
#include "worker/worker.h"

class Handler {
 public:
  Handler() {}

  // void ConfigureComputeNode(int argc, char* argv[]);
  void GenThreads(std::string bench_name);
  void test();
  // void OutputResult(std::string bench_name, std::string system_name);
};