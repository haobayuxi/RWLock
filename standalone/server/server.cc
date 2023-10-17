
#include "server.h"

// using namespace std;

const uint64_t mem_size = 1024 * 1024 * 1024;
char* test_memory;

// test for random cas and read in memory
void run_test() {
  // init memory
  test_memory = (char*)malloc(mem_size);
  memset(test_memory, 0, mem_size);

  //   random cas
}

void Server::gen_threads(int thread_num) {
  auto thread_arr = new std::thread[thread_num];
  for (int i = 0; i < thread_num; i++) {
    // thread_arr[i] =
    //     std::thread(run_thread, &param_arr[i], tatp_client, tpcc_client);
    thread_arr[i] = std::thread(run_test);
    /* Pin thread i to hardware thread i */
    cpu_set_t cpuset;
    CPU_ZERO(&cpuset);
    CPU_SET(i, &cpuset);
    int rc = pthread_setaffinity_np(thread_arr[i].native_handle(),
                                    sizeof(cpu_set_t), &cpuset);
    if (rc != 0) {
      std::cout << "Error calling pthread_setaffinity_np: " << rc;
    }
  }

  for (int i = 0; i < thread_num; i++) {
    if (thread_arr[i].joinable()) {
      thread_arr[i].join();
    }
  }
}

int main(int argc, char* argv[]) { return 0; }