/*
 * Copyright (c) 2019-2022 ExpoLab, UC Davis
 *
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use,
 * copy, modify, merge, publish, distribute, sublicense, and/or
 * sell copies of the Software, and to permit persons to whom the
 * Software is furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES
 * OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
 * HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY,
 * WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER
 * DEALINGS IN THE SOFTWARE.
 *
 */

#include "platform/consensus/ordering/pbft/message_manager.h"

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {

// Constructor for MessageManager
MessageManager::MessageManager(
    const ResDBConfig& config,
    std::unique_ptr<TransactionManager> transaction_manager,
    CheckPointManager* checkpoint_manager, SystemInfo* system_info)
    : config_(config),
      queue_("executed"),
      txn_db_(checkpoint_manager->GetTxnDB()),
      system_info_(system_info),
      checkpoint_manager_(checkpoint_manager),
      transaction_executor_(std::make_unique<TransactionExecutor>(
          config,
          [&](std::unique_ptr<Request> request,
              std::unique_ptr<BatchUserResponse> resp_msg) {
            if (request->is_recovery()) {
              return;
            }
            resp_msg->set_proxy_id(request->proxy_id());
            resp_msg->set_seq(request->seq());
            resp_msg->set_current_view(request->current_view());
            resp_msg->set_primary_id(GetCurrentPrimary());
            if (transaction_executor_->NeedResponse() &&
                resp_msg->proxy_id() != 0) {
              queue_.Push(std::move(resp_msg));
            }
            if (checkpoint_manager_) {
              checkpoint_manager_->AddCommitData(std::move(request));
            }
          },
          system_info_, std::move(transaction_manager))),
      collector_pool_(std::make_unique<LockFreeCollectorPool>(
          "txn", config_.GetMaxProcessTxn(), transaction_executor_.get(),
          config_.GetConfigData().enable_viewchange())) {
  global_stats_ = Stats::GetGlobalStats();
  transaction_executor_->SetSeqUpdateNotifyFunc(
      [&](uint64_t seq) { collector_pool_->Update(seq - 1); });
  checkpoint_manager_->SetExecutor(transaction_executor_.get());
}

// Destructor for MessageManager
MessageManager::~MessageManager() {
  if (transaction_executor_) {
    transaction_executor_->Stop();
  }
}

// Get the next response message from the queue
std::unique_ptr<BatchUserResponse> MessageManager::GetResponseMsg() {
  return queue_.Pop();
}

// Get the current primary replica
int64_t MessageManager::GetCurrentPrimary() const {
  return system_info_->GetPrimaryId();
}

// Get the current view
uint64_t MessageManager::GetCurrentView() const {
  return system_info_->GetCurrentView();
}

// Set the next sequence number
void MessageManager::SetNextSeq(uint64_t seq) {
  next_seq_ = seq;
  LOG(ERROR) << "set next seq:" << next_seq_;
}

// Get the next sequence number
int64_t MessageManager::GetNextSeq() {
  return next_seq_;
}

// Assign the next available sequence number
absl::StatusOr<uint64_t> MessageManager::AssignNextSeq() {
  std::unique_lock<std::mutex> lk(seq_mutex_);
  uint32_t max_executed_seq = transaction_executor_->GetMaxPendingExecutedSeq();
  global_stats_->SeqGap(next_seq_ - max_executed_seq);
  if (next_seq_ - max_executed_seq >
      static_cast<uint64_t>(config_.GetMaxProcessTxn())) {
    return absl::InvalidArgumentError("Seq has been used up.");
  }
  return next_seq_++;
}

// Get a list of replicas
std::vector<ReplicaInfo> MessageManager::GetReplicas() {
  return system_info_->GetReplicas();
}

// yathin017 - MessageManager::AddReplica and MessageManager::RemoveReplica
void MessageManager::AddReplica(const ReplicaInfo& replica) {
  return system_info_->AddReplica(replica);
};
void MessageManager::RemoveReplica(int64_t replica_id) {
  return system_info_->RemoveReplica(replica_id);
};

// yathin017 - MessageManager::GetReplicaCount and MessageManager::GetMinDataReceiveNum
int MessageManager::GetReplicaCount() {
  return  system_info_->GetReplicaCount();
};
int MessageManager::GetMinDataReceiveNum() {
  return system_info_->GetMinDataReceiveNum();
};

// Check if the request is valid
bool MessageManager::IsValidMsg(const Request& request) {
  if (request.type() == Request::TYPE_RESPONSE) {
    return true;
  }

  // The view should be the same as the current one.
  if (static_cast<uint64_t>(request.current_view()) != GetCurrentView()) {
    LOG(ERROR) << "message view :[" << request.current_view()
               << "] is older than the cur view :[" << GetCurrentView() << "]";
    return false;
  }

  if (static_cast<uint64_t>(request.seq()) <
      transaction_executor_->GetMaxPendingExecutedSeq()) {
    return false;
  }

  return true;
}

// Check if consensus status should change
bool MessageManager::MayConsensusChangeStatus(
    int type, int received_count, std::atomic<TransactionStatue>* status,
    bool ret) {
  switch (type) {
    case Request::TYPE_PRE_PREPARE:
      if (*status == TransactionStatue::None) {
        TransactionStatue old_status = TransactionStatue::None;
        return status->compare_exchange_strong(
            old_status, TransactionStatue::READY_PREPARE,
            std::memory_order_acq_rel, std::memory_order_acq_rel);
      }
      break;
    case Request::TYPE_PREPARE:
      if (*status == TransactionStatue::READY_PREPARE &&
          config_.GetMinDataReceiveNum() <= received_count) {
        TransactionStatue old_status = TransactionStatue::READY_PREPARE;
        return status->compare_exchange_strong(
            old_status, TransactionStatue::READY_COMMIT,
            std::memory_order_acq_rel, std::memory_order_acq_rel);
      }
      break;
    case Request::TYPE_COMMIT:
      if (*status == TransactionStatue::READY_COMMIT &&
          config_.GetMinDataReceiveNum() <= received_count) {
        TransactionStatue old_status = TransactionStatue::READY_COMMIT;
        return status->compare_exchange_strong(
            old_status, TransactionStatue::READY_EXECUTE,
            std::memory_order_acq_rel, std::memory_order_acq_rel);
        return true;
      }
      break;
  }
  return ret;
}

// Add consensus messages and return the number of messages received.
CollectorResultCode MessageManager::AddConsensusMsg(
    const SignatureInfo& signature, std::unique_ptr<Request> request) {
  if (request == nullptr || !IsValidMsg(*request)) {
    return CollectorResultCode::INVALID;
  }
  int type = request->type();
  uint64_t seq = request->seq();
  int resp_received_count = 0;
  int proxy_id = request->proxy_id();

  int ret = collector_pool_->GetCollector(seq)->AddRequest(
      std::move(request), signature, type == Request::TYPE_PRE_PREPARE,
      [&](const Request& request, int received_count,
          TransactionCollector::CollectorDataType* data,
          std::atomic<TransactionStatue>* status, bool force) {
        if (MayConsensusChangeStatus(type, received_count, status, force)) {
          resp_received_count = 1;
        }
      });
  if (ret == 1) {
    SetLastCommittedTime(proxy_id);
  } else if (ret != 0) {
    return CollectorResultCode::INVALID;
  }
  if (resp_received_count > 0) {
    return CollectorResultCode::STATE_CHANGED;
  }
  return CollectorResultCode::OK;
}

// Get a set of requests in a specified range
RequestSet MessageManager::GetRequestSet(uint64_t min_seq, uint64_t max_seq) {
  RequestSet ret;
  std::unique_lock<std::mutex> lk(data_mutex_);
  for (uint64_t i = min_seq; i <= max_seq; ++i) {
    if (committed_data_.find(i) == committed_data_.end()) {
      LOG(ERROR) << "seq :" << i << " doesn't exist";
      continue;
    }
    RequestWithProof* request = ret.add_requests();
    *request->mutable_request() = committed_data_[i];
    request->set_seq(i);
    for (const auto& request_info : committed_proof_[i]) {
      RequestWithProof::RequestData* data = request->add_proofs();
      *data->mutable_request() = *request_info->request;
      *data->mutable_signature() = request_info->signature;
    }
  }
  return ret;
}

// Get a request by sequence number
Request* MessageManager::GetRequest(uint64_t seq) {
  return txn_db_->Get(seq);
}

// Get prepared proofs for a specific sequence
std::vector<RequestInfo> MessageManager::GetPreparedProof(uint64_t seq) {
  return collector_pool_->GetCollector(seq)->GetPreparedProof();
}

// Get the state of a transaction by sequence number
TransactionStatue MessageManager::GetTransactionState(uint64_t seq) {
  return collector_pool_->GetCollector(seq)->GetStatus();
}

// Get replica state information
int MessageManager::GetReplicaState(ReplicaState* state) {
  state->set_view(GetCurrentView());
  *state->mutable_replica_info() = config_.GetSelfInfo();
  *state->mutable_replica_config() = config_.GetConfigData();
  return 0;
}

// Get the storage object
Storage* MessageManager::GetStorage() {
  return transaction_executor_->GetStorage();
}

// Set the last committed time for a proxy
void MessageManager::SetLastCommittedTime(uint64_t proxy_id) {
  lct_lock_.lock();
  last_committed_time_[proxy_id] = GetCurrentTime();
  lct_lock_.unlock();
}

// Get the last committed time for a proxy
uint64_t MessageManager::GetLastCommittedTime(uint64_t proxy_id) {
  lct_lock_.lock();
  auto value = last_committed_time_[proxy_id];
  lct_lock_.unlock();
  return value;
}

// Check if a sequence number is prepared
// yathin017 IsPreapared -> IsPrepared
bool MessageManager::IsPrepared(uint64_t seq) {
  return collector_pool_->GetCollector(seq)->IsPrepared();
}

// Get the highest prepared sequence number
uint64_t MessageManager::GetHighestPreparedSeq() {
  return checkpoint_manager_->GetHighestPreparedSeq();
}

// Set the highest prepared sequence number
void MessageManager::SetHighestPreparedSeq(uint64_t seq) {
  return checkpoint_manager_->SetHighestPreparedSeq(seq);
}

// Set the duplicate manager for transaction execution
void MessageManager::SetDuplicateManager(DuplicateManager* manager) {
  transaction_executor_->SetDuplicateManager(manager);
}

// Send a response message
void MessageManager::SendResponse(std::unique_ptr<Request> request) {
  std::unique_ptr<BatchUserResponse> response =
      std::make_unique<BatchUserResponse>();
  response->set_createtime(GetCurrentTime());
  response->set_hash(request->hash());
  response->set_proxy_id(request->proxy_id());
  response->set_seq(request->seq());
  response->set_current_view(GetCurrentView());
  response->set_primary_id(GetCurrentPrimary());
  if (transaction_executor_->NeedResponse() && response->proxy_id() != 0) {
    queue_.Push(std::move(response));
  }
}

// Get the collector pool
LockFreeCollectorPool* MessageManager::GetCollectorPool() {
  return collector_pool_.get();
}

}  // namespace resdb

