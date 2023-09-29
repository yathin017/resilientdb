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

#include "platform/consensus/ordering/pbft/performance_manager.h"

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {

// Constructor for PerformanceClientTimeout
PerformanceClientTimeout::PerformanceClientTimeout(std::string hash_, uint64_t time_) {
  this->hash = hash_;
  this->timeout_time = time_;
}

// Copy constructor for PerformanceClientTimeout
PerformanceClientTimeout::PerformanceClientTimeout(const PerformanceClientTimeout& other) {
  this->hash = other.hash;
  this->timeout_time = other.timeout_time;
}

// Comparison operator for PerformanceClientTimeout
bool PerformanceClientTimeout::operator<(const PerformanceClientTimeout& other) const {
  return timeout_time > other.timeout_time;
}

// Constructor for PerformanceManager
PerformanceManager::PerformanceManager(
    const ResDBConfig& config, ReplicaCommunicator* replica_communicator,
    SystemInfo* system_info, SignatureVerifier* verifier)
    : config_(config),
      replica_communicator_(replica_communicator),
      collector_pool_(std::make_unique<LockFreeCollectorPool>("response", config_.GetMaxProcessTxn(), nullptr)),
      context_pool_(std::make_unique<LockFreeCollectorPool>("context", config_.GetMaxProcessTxn(), nullptr)),
      batch_queue_("user request"),
      system_info_(system_info),
      verifier_(verifier) {
  stop_ = false;
  eval_started_ = false;
  eval_ready_future_ = eval_ready_promise_.get_future();
  
  // Start user request threads for client mode
  if (config_.GetPublicKeyCertificateInfo().public_key().public_key_info().type() == CertificateKeyInfo::CLIENT) {
    for (int i = 0; i < 2; ++i) {
      user_req_thread_[i] = std::thread(&PerformanceManager::BatchProposeMsg, this);
    }
  }

  // Start the thread for monitoring client timeouts
  checking_timeout_thread_ = std::thread(&PerformanceManager::MonitoringClientTimeOut, this);
  
  global_stats_ = Stats::GetGlobalStats();
  
  // Initialize send_num_ vector
  for (size_t i = 0; i <= config_.GetReplicaNum(); i++) {
    send_num_.push_back(0);
  }
  total_num_ = 0;
  timeout_length_ = 10000000;  // 10s
}

// Destructor for PerformanceManager
PerformanceManager::~PerformanceManager() {
  stop_ = true;
  
  // Join user request threads
  for (int i = 0; i < 16; ++i) {
    if (user_req_thread_[i].joinable()) {
      user_req_thread_[i].join();
    }
  }
  
  // Join the monitoring client timeout thread
  if (checking_timeout_thread_.joinable()) {
    checking_timeout_thread_.join();
  }
}

// Get the primary replica
int PerformanceManager::GetPrimary() {
  return system_info_->GetPrimaryId();
}

// Generate a user request
std::unique_ptr<Request> PerformanceManager::GenerateUserRequest() {
  std::unique_ptr<Request> request = std::make_unique<Request>();
  request->set_data(data_func_());
  return request;
}

// Set the data generation function
void PerformanceManager::SetDataFunc(std::function<std::string()> func) {
  data_func_ = std::move(func);
}

// Start evaluation
int PerformanceManager::StartEval() {
  if (eval_started_) {
    return 0;
  }
  eval_started_ = true;
  
  // Generate and push user requests to the batch queue
  for (int i = 0; i < 60000000000; ++i) {
    std::unique_ptr<QueueItem> queue_item = std::make_unique<QueueItem>();
    queue_item->context = nullptr;
    queue_item->user_request = GenerateUserRequest();
    batch_queue_.Push(std::move(queue_item));
    if (i == 20000000) {
      eval_ready_promise_.set_value(true);
    }
  }
  LOG(WARNING) << "start eval done";
  return 0;
}

// Process a response message
int PerformanceManager::ProcessResponseMsg(std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  std::unique_ptr<Request> response;
  std::string hash = request->hash();
  int32_t primary_id = request->primary_id();
  uint64_t seq = request->seq();
  
  // Add the response message and use a callback to collect received messages
  CollectorResultCode ret = AddResponseMsg(context->signature, std::move(request), [&](const Request& request, const TransactionCollector::CollectorDataType*) {
    response = std::make_unique<Request>(request);
    return;
  });
  
  if (ret == CollectorResultCode::STATE_CHANGED) {
    BatchUserResponse batch_response;
    if (batch_response.ParseFromString(response->data())) {
      if (seq > highest_seq_) {
        highest_seq_ = seq;
        if (highest_seq_primary_id_ != primary_id) {
          system_info_->SetPrimary(primary_id);
          highest_seq_primary_id_ = primary_id;
        }
      }
      SendResponseToClient(batch_response);
      RemoveWaitingResponseRequest(hash);
    } else {
      LOG(ERROR) << "parse response fail:";
    }
  }
  return ret == CollectorResultCode::INVALID ? -2 : 0;
}

// Check if consensus status should change
bool PerformanceManager::MayConsensusChangeStatus(int type, int received_count, std::atomic<TransactionStatue>* status) {
  switch (type) {
    case Request::TYPE_RESPONSE:
      // If received f+1 response results, ack to the caller
      if (*status == TransactionStatue::None && config_.GetMinClientReceiveNum() <= received_count) {
        TransactionStatue old_status = TransactionStatue::None;
        return status->compare_exchange_strong(old_status, TransactionStatue::EXECUTED, std::memory_order_acq_rel, std::memory_order_acq_rel);
      }
      break;
  }
  return false;
}

// Add a response message to the collector pool
CollectorResultCode PerformanceManager::AddResponseMsg(const SignatureInfo& signature, std::unique_ptr<Request> request, std::function<void(const Request&, const TransactionCollector::CollectorDataType*)> response_call_back) {
  if (request == nullptr) {
    return CollectorResultCode::INVALID;
  }

  int type = request->type();
  uint64_t seq = request->seq();
  int resp_received_count = 0;
  int ret = collector_pool_->GetCollector(seq)->AddRequest(std::move(request), signature, false, [&](const Request& request, int received_count, TransactionCollector::CollectorDataType* data, std::atomic<TransactionStatue>* status, bool force) {
    if (MayConsensusChangeStatus(type, received_count, status)) {
      resp_received_count = 1;
      response_call_back(request, data);
    }
  });
  if (ret != 0) {
    return CollectorResultCode::INVALID;
  }
  if (resp_received_count > 0) {
    collector_pool_->Update(seq);
    return CollectorResultCode::STATE_CHANGED;
  }
  return CollectorResultCode::OK;
}

// Send a response to the client
void PerformanceManager::SendResponseToClient(const BatchUserResponse& batch_response) {
  uint64_t create_time = batch_response.createtime();
  uint64_t local_id = batch_response.local_id();
  if (create_time > 0) {
    uint64_t run_time = GetSysClock() - create_time;
    global_stats_->AddLatency(run_time);
  } else {
    LOG(ERROR) << "seq:" << local_id << " no resp";
  }
  
  {
    if (send_num_[batch_response.primary_id()] > 0) {
      send_num_[batch_response.primary_id()]--;
    }
  }

  if (config_.IsPerformanceRunning()) {
    return;
  }
}

// Batch process user requests
int PerformanceManager::BatchProposeMsg() {
  LOG(WARNING) << "batch wait time:" << config_.ClientBatchWaitTimeMS()
               << " batch num:" << config_.ClientBatchNum()
               << " max txn:" << config_.GetMaxProcessTxn();
  std::vector<std::unique_ptr<QueueItem>> batch_req;
  eval_ready_future_.get();
  while (!stop_) {
    if (send_num_[GetPrimary()] >= config_.GetMaxProcessTxn()) {
      usleep(100000);
      continue;
    }
    if (batch_req.size() < config_.ClientBatchNum()) {
      std::unique_ptr<QueueItem> item = batch_queue_.Pop(config_.ClientBatchWaitTimeMS());
      if (item == nullptr) {
        continue;
      }
      batch_req.push_back(std::move(item));
      if (batch_req.size() < config_.ClientBatchNum()) {
        continue;
      }
    }
    int ret = DoBatch(batch_req);
    batch_req.clear();
    if (ret != 0) {
      Response response;
      response.set_result(Response::ERROR);
      for (size_t i = 0; i < batch_req.size(); ++i) {
        if (batch_req[i]->context && batch_req[i]->context->client) {
          int ret = batch_req[i]->context->client->SendRawMessage(response);
          if (ret) {
            LOG(ERROR) << "send resp" << response.DebugString()
                       << " fail ret:" << ret;
          }
        }
      }
    }
  }
  return 0;
}

// Process a batch of user requests
int PerformanceManager::DoBatch(const std::vector<std::unique_ptr<QueueItem>>& batch_req) {
  auto new_request = NewRequest(Request::TYPE_NEW_TXNS, Request(), config_.GetSelfInfo().id());
  if (new_request == nullptr) {
    return -2;
  }
  std::vector<std::unique_ptr<Context>> context_list;

  BatchUserRequest batch_request;
  for (size_t i = 0; i < batch_req.size(); ++i) {
    BatchUserRequest::UserRequest* req = batch_request.add_user_requests();
    *req->mutable_request() = *batch_req[i]->user_request.get();
    if (batch_req[i]->context) {
      *req->mutable_signature() = batch_req[i]->context->signature;
    }
    req->set_id(i);
  }

  batch_request.set_createtime(GetSysClock());
  batch_request.SerializeToString(new_request->mutable_data());
  if (verifier_) {
    auto signature_or = verifier_->SignMessage(new_request->data());
    if (!signature_or.ok()) {
      LOG(ERROR) << "Sign message fail";
      return -2;
    }
    *new_request->mutable_data_signature() = *signature_or;
  }

  new_request->set_hash(SignatureVerifier::CalculateHash(new_request->data()));
  new_request->set_proxy_id(config_.GetSelfInfo().id());

  replica_communicator_->SendMessage(*new_request, GetPrimary());
  global_stats_->BroadCastMsg();
  send_num_[GetPrimary()]++;
  if (total_num_++ == 1000000) {
    stop_ = true;
    LOG(WARNING) << "total num is done:" << total_num_;
  }
  if (total_num_ % 10000 == 0) {
    LOG(WARNING) << "total num is :" << total_num_;
  }
  global_stats_->IncClientCall();
  AddWaitingResponseRequest(std::move(new_request));
  return 0;
}

// Add a request to the waiting response batch
void PerformanceManager::AddWaitingResponseRequest(std::unique_ptr<Request> request) {
  if (!config_.GetConfigData().enable_viewchange()) {
    return;
  }
  pm_lock_.lock();
  uint64_t time = GetCurrentTime() + this->timeout_length_;
  client_timeout_min_heap_.push(PerformanceClientTimeout(request->hash(), time));
  waiting_response_batches_.insert(std::make_pair(request->hash(), std::move(request)));
  pm_lock_.unlock();
  sem_post(&request_sent_signal_);
}

// Remove a request from the waiting response batch
void PerformanceManager::RemoveWaitingResponseRequest(std::string hash) {
  if (!config_.GetConfigData().enable_viewchange()) {
    return;
  }
  pm_lock_.lock();
  if (waiting_response_batches_.find(hash) != waiting_response_batches_.end()) {
    waiting_response_batches_.erase(waiting_response_batches_.find(hash));
  }
  pm_lock_.unlock();
}

// Check if a request is in the waiting response batch
bool PerformanceManager::CheckTimeOut(std::string hash) {
  pm_lock_.lock();
  bool value = (waiting_response_batches_.find(hash) != waiting_response_batches_.end());
  pm_lock_.unlock();
  return value;
}

// Get a timed-out request from the waiting response batch
std::unique_ptr<Request> PerformanceManager::GetTimeOutRequest(std::string hash) {
  pm_lock_.lock();
  auto value = std::move(waiting_response_batches_.find(hash)->second);
  pm_lock_.unlock();
  return value;
}

// Monitor client timeouts and resend requests if necessary
void PerformanceManager::MonitoringClientTimeOut() {
  while (!stop_) {
    sem_wait(&request_sent_signal_);
    pm_lock_.lock();
    if (client_timeout_min_heap_.empty()) {
      pm_lock_.unlock();
      continue;
    }
    auto client_timeout = client_timeout_min_heap_.top();
    client_timeout_min_heap_.pop();
    pm_lock_.unlock();

    if (client_timeout.timeout_time > GetCurrentTime()) {
      usleep(client_timeout.timeout_time - GetCurrentTime());
    }

    if (CheckTimeOut(client_timeout.hash)) {
      auto request = GetTimeOutRequest(client_timeout.hash);
      if (request) {
        LOG(ERROR) << "Client Request Timeout " << client_timeout.hash;
        replica_communicator_->BroadCast(*request);
      }
    }
  }
}

}  // namespace resdb
