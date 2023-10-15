

#include "dtx.h"

bool DTX::RWLock(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    // auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    // if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
    //   return false;
    // }
  }
  for (auto& item : read_write_set) {
    // cas lock
  }

  return true;
}

bool DTX::Drtm(coro_yield_t& yield) {
  for (auto& item : read_only_set) {
    if (item.is_fetched) continue;
    auto it = item.item_ptr;
    // node_id_t remote_node_id =
    // global_meta_man->GetPrimaryNodeID(it->table_id);
    node_id_t remote_node_id = 0;
    RCQP* qp = thread_qp_man->GetRemoteDataQPWithNodeID(remote_node_id);
    // auto offset = addr_cache->Search(remote_node_id, it->table_id, it->key);
    char* data_buf = thread_rdma_buffer_alloc->Alloc(DataItemSize);
    // if (!coro_sched->RDMARead(coro_id, qp, data_buf, offset, DataItemSize)) {
    //   return false;
    // }
  }
  for (auto& item : read_write_set) {
    // cas lock
  }
  return true;
}
