
#include "server.h"

#include <atomic>

// using namespace std;

const uint64_t mem_size = 1024 * 1024 * 1024;
char* test_memory;
uint64_t commits[100];

// test for random cas and read in memory
void run_test(int thread_id, int thread_num) {
  //   random cas
  auto offset = sizeof(uint64_t) * thread_id;
  for (int i = 0; i < 1000; i++) {
    auto ptr = static_cast<std::atomic<uint64_t>*>(test_memory + offset);
    ptr->compare_exchange_strong(0, 1);
    offset = (offset + thread_num) % mem_size;
  }
  std::cout << " thread " << thread_id << std::endl;
  sleep(1);
}

void Server::gen_threads(int thread_num) {
  // init memory
  test_memory = (char*)malloc(mem_size * sizeof(uint64_t));
  memset(test_memory, 0, mem_size);
  memset(commits, 0, sizeof(uint64_t) * 100);

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
}

int main(int argc, char* argv[]) {
  Server* s = new Server();
  s->gen_threads(5);
  return 0;
}