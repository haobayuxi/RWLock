
#include <pthread.h>
#include <sched.h>
#include <stdlib.h>
#include <sys/types.h>
#include <unistd.h>

#include <cstdio>
#include <cstring>
#include <iostream>
#include <string>
#include <thread>

#include "rwbench/rwbench.h"

class Server {
 public:
  void gen_threads(int thread_num);

 private:
  int thread_num;
};
