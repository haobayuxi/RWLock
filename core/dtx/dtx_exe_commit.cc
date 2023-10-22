// // Author: Ming Zhang
// // Copyright (c) 2022

#include "dtx/dtx.h"

bool DTX::TxExe(coro_yield_t& yield, bool fail_abort) {
  // Start executing transaction
  tx_status = TXStatus::TX_EXE;
  if (read_write_set.empty() && read_only_set.empty()) {
    RDMA_LOG(INFO) << "wrong";
    return true;
  }

  if (global_meta_man->txn_system == DTX_SYS::RWLock ||
      global_meta_man->txn_system == DTX_SYS::OCC) {
    // Run our system
    if (RWLock(yield)) {
      return true;
    } else {
      goto ABORT;
    }
  } else if (global_meta_man->txn_system == DTX_SYS::DrTMH) {
    if (Drtm(yield)) {
      return true;
    } else {
      goto ABORT;
    }
  } else if (global_meta_man->txn_system == DTX_SYS::DLMR) {
    // get read lock

    // get write lock
  } else {
    RDMA_LOG(FATAL) << "NOT SUPPORT SYSTEM ID: " << global_meta_man->txn_system;
  }

  return true;

ABORT:
  if (fail_abort) Abort();
  return false;
}

bool DTX::TxCommit(coro_yield_t& yield) {
  bool commit_stat;

  /*!
    RWLock's commit protocol
    */

  if (global_meta_man->txn_system == DTX_SYS::RWLock) {
    // check lease
    auto end_time = get_clock_sys_time_us();
    if ((end_time - start_time) > lease) {
      if (!Validate(yield)) {
        goto ABORT;
      }
    }

    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }
  } else if (global_meta_man->txn_system == DTX_SYS::OCC) {
    /*
      OCC commit protocol
    */
    if (!Validate(yield)) {
      goto ABORT;
    }
    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }

  } else if (global_meta_man->txn_system == DTX_SYS::DrTMH) {
    // check lease

    if (!Validate(yield)) {
      goto ABORT;
    }

    // Next step. If read-write txns, we need to commit the updates to remote
    // replicas
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }
  } else if (global_meta_man->txn_system == DTX_SYS::DLMR) {
    if (!read_write_set.empty()) {
      // Write log

      // write data and unlock
    }
    // unlock read lock and write lock
  }

  return true;
ABORT:
  Abort();
  return false;
}
