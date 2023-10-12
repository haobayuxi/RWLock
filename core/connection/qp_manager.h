#pragma once

#include "meta_manager.h"

class QPManager {
 public:
  QPManager(t_id_t golbal_tid) : global_tid(global_tid) {}
  void BuildQPConnection(MetaManager* meta_man);

 private:
  RCQP* data_qps[MAX_REMOTE_NODE_NUM]{nullptr};
  t_id_t global_tid;
};