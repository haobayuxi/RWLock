
#include "server.h"

#include <atomic>

// using namespace std;

const uint64_t mem_size = 1024 * 1024 * 1024;
char* test_memory;
uint64_t commits[100];
int times = 1000000;

// test for random cas and read in memory
void run_test(int thread_id, int thread_num) {
  //   random cas
  auto offset = sizeof(uint64_t) * thread_id;
  uint64_t exp = 0;
  for (int i = 0; i < times; i++) {
    auto ptr = (std::atomic<uint64_t>*)(test_memory + offset);
    ptr->compare_exchange_strong(exp, 1);
    // auto x = *(test_memory + offset);
    offset = (offset + thread_num * sizeof(uint64_t)) % mem_size;
  }
  //   std::cout << " thread " << thread_id << std::endl;
  //   sleep(1);
}

void Server::gen_threads(int thread_num) {
  // init memory
  test_memory = (char*)malloc(mem_size * sizeof(uint64_t));
  memset(test_memory, 0, mem_size);
  memset(commits, 0, sizeof(uint64_t) * 100);
  uint64_t start_time = get_clock_sys_time_us();
  // gen threads
  auto thread_arr = new std::thread[thread_num];
  for (int i = 0; i < thread_num; i++) {
    thread_arr[i] = std::thread(run_test, i, thread_num);
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

  uint64_t end_time = get_clock_sys_time_us();
  double second = (end_time - start_time) / 1000000.0;
  std::cout << (thread_num * times) / second << std::endl;
}

int main(int argc, char* argv[]) {
  Server* s = new Server();
  int thread_num = atoi(argv[1]);
  std::cout << "thread_num" << thread_num << std::endl;
  s->gen_threads(thread_num);
  return 0;
}