// Author: Ming Zhang
// Copyright (c) 2022

#include "micro/micro_txn.h"

#include <set>

/******************** The business logic (Transaction) start
 * ********************/

bool TxTestCachedAddr(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield,
                      tx_id_t tx_id, DTX* dtx, bool is_skewed,
                      uint64_t data_set_size, uint64_t num_keys_global,
                      uint64_t write_ratio) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemDuplicate micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id =
        688;  // First read is non-cacheable, set ATTEMPED_NUM to 5

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i].data_item_ptr = std::make_shared<DataItem>(
        (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    micro_objs[i].is_dup = false;

    dtx->AddToReadWriteSet(micro_objs[i].data_item_ptr);
    is_write[i] = true;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i].data_item_ptr->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id
                      << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxLockContention(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield,
                      tx_id_t tx_id, DTX* dtx, bool is_skewed,
                      uint64_t data_set_size, uint64_t num_keys_global,
                      uint64_t write_ratio) {
  dtx->TxBegin(tx_id);
  bool is_write[data_set_size];
  DataItemPtr micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(seed) & (num_keys_global - 1);
    }

    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    micro_objs[i] = std::make_shared<DataItem>(
        (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);

    if (FastRand(seed) % 100 < write_ratio) {
      dtx->AddToReadWriteSet(micro_objs[i]);
      is_write[i] = true;
    } else {
      dtx->AddToReadOnlySet(micro_objs[i]);
      is_write[i] = false;
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id
                      << "-" << dtx->coro_id << "-" << tx_id;
    }
    if (is_write[i]) {
      micro_val->magic[1] = micro_magic * 2;
    }
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxReadOnly(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield,
                tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size,
                uint64_t num_keys_global, uint64_t rw_ratio) {
  dtx->TxBegin(tx_id);
  bool read_only = true;
  dtx->rw_ratio += 1;
  if (dtx->rw_ratio == rw_ratio) {
    read_only = false;
    rw_ratio = 0;
  }
  for (int i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    if (is_skewed) {
      // Skewed distribution
      micro_key.micro_id = (itemkey_t)(zipf_gen->next());
      // micro_key.micro_id = 100;
    } else {
      // Uniformed distribution
      micro_key.micro_id = (itemkey_t)FastRand(seed) & (num_keys_global - 1);
      // micro_key.micro_id = dtx->t_id * 100 + dtx->coro_id;
    }
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);

    DataItemPtr micro_obj = std::make_shared<DataItem>(
        (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    if (read_only) {
      dtx->AddToReadOnlySet(micro_obj);
    } else {
      dtx->AddToReadWriteSet(micro_obj);
    }
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush1(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield,
               tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size,
               uint64_t num_keys_global, uint64_t write_ratio) {
  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_key_t micro_key;
    micro_key.micro_id = i;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>(
        (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id
                      << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

bool TxRFlush2(ZipfGen* zipf_gen, uint64_t* seed, coro_yield_t& yield,
               tx_id_t tx_id, DTX* dtx, bool is_skewed, uint64_t data_set_size,
               uint64_t num_keys_global, uint64_t write_ratio) {
  // Test remote flush steps:
  // 1. Turn off undo log to accurately test the perf. difference
  // 2. In Coalescent Commit. Use RDMABatchSync and RDMAReadSync for both
  // full/batch flush
  // 3. Turn off Yield in Coalescent Commit because RDMABatchSync and
  // RDMAReadSync already poll acks
  std::set<itemkey_t> key_set;

  dtx->TxBegin(tx_id);
  DataItemPtr micro_objs[data_set_size];

  // gen keys
  itemkey_t key = 0;
  while (key_set.size() != data_set_size) {
    if (is_skewed) {
      key = (itemkey_t)(zipf_gen->next());
    } else {
      key = (itemkey_t)FastRand(seed) & (num_keys_global - 1);
    }
    key_set.insert(key);
  }

  int i = 0;
  for (auto it = key_set.begin(); it != key_set.end(); ++it) {
    micro_key_t micro_key;
    micro_key.micro_id = *it;
    assert(micro_key.item_key >= 0 && micro_key.item_key < num_keys_global);
    micro_objs[i] = std::make_shared<DataItem>(
        (table_id_t)MicroTableType::kMicroTable, micro_key.item_key);
    dtx->AddToReadWriteSet(micro_objs[i]);
    i++;
  }

  if (!dtx->TxExe(yield)) {
    // TLOG(DBG, thread_gid) << "tx " << tx_id << " aborts after exe";
    return false;
  }

  for (uint64_t i = 0; i < data_set_size; i++) {
    micro_val_t* micro_val = (micro_val_t*)micro_objs[i]->value;
    if (micro_val->magic[0] != micro_magic) {
      RDMA_LOG(FATAL) << "[FATAL] Read unmatch, tid-cid-txid: " << dtx->t_id
                      << "-" << dtx->coro_id << "-" << tx_id;
    }
    micro_val->magic[1] = micro_magic * 2;
  }

  bool commit_status = dtx->TxCommit(yield);
  // TLOG(DBG, thread_gid) << "tx " << tx_id << " commit? " << commit_status;
  return commit_status;
}

/******************** The business logic (Transaction) end ********************/