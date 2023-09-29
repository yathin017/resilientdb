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

#include "platform/consensus/ordering/pbft/checkpoint_manager.h"
#include <glog/logging.h>
#include "platform/consensus/ordering/pbft/transaction_utils.h"
#include "platform/proto/checkpoint_info.pb.h"

namespace resdb {

// Constructor for the CheckPointManager class
CheckPointManager::CheckPointManager(const ResDBConfig& config,
                                     ReplicaCommunicator* replica_communicator,
                                     SignatureVerifier* verifier)
    : config_(config),
      replica_communicator_(replica_communicator),
      txn_db_(std::make_unique<TxnMemoryDB>()),
      verifier_(verifier),
      stop_(false),
      txn_accessor_(config),
      highest_prepared_seq_(0) {
  current_stable_seq_ = 0;

  // Check if checkpoint is enabled in the configuration
  if (config_.GetConfigData().enable_viewchange()) {
    config_.EnableCheckPoint(true);
  }

  // If checkpoint is enabled, start checkpoint threads
  if (config_.IsCheckPointEnabled()) {
    LOG(INFO) << "CheckPointManager: Checkpoint is enabled.";
    stable_checkpoint_thread_ =
        std::thread(&CheckPointManager::UpdateStableCheckPointStatus, this);
    checkpoint_thread_ =
        std::thread(&CheckPointManager::UpdateCheckPointStatus, this);
  }

  // Initialize a semaphore for signaling committable sequences
  sem_init(&committable_seq_signal_, 0, 0);
}

// Destructor for the CheckPointManager class
CheckPointManager::~CheckPointManager() {
  Stop();
}

// Stop the checkpoint manager and join threads
void CheckPointManager::Stop() {
  stop_ = true;
  if (checkpoint_thread_.joinable()) {
    checkpoint_thread_.join();
  }
  if (stable_checkpoint_thread_.joinable()) {
    stable_checkpoint_thread_.join();
  }
  LOG(INFO) << "CheckPointManager: Stopped.";
}

// Function to calculate the hash of two strings
std::string GetHash(const std::string& h1, const std::string& h2) {
  return SignatureVerifier::CalculateHash(h1 + h2);
}

// Get a pointer to the transaction database
TxnMemoryDB* CheckPointManager::GetTxnDB() {
  return txn_db_.get();
}

// Get the maximum transaction sequence number
uint64_t CheckPointManager::GetMaxTxnSeq() {
  return txn_db_->GetMaxSeq();
}

// Get the stable checkpoint sequence
uint64_t CheckPointManager::GetStableCheckpoint() {
  std::lock_guard<std::mutex> lk(mutex_);
  return current_stable_seq_;
}

// Get the stable checkpoint information with votes
StableCheckPoint CheckPointManager::GetStableCheckpointWithVotes() {
  std::lock_guard<std::mutex> lk(mutex_);
  return stable_ckpt_;
}

// Add commit data to the checkpoint manager
void CheckPointManager::AddCommitData(std::unique_ptr<Request> request) {
  if (config_.IsCheckPointEnabled()) {
    data_queue_.Push(std::move(request));
  } else {
    txn_db_->Put(std::move(request));
  }
  LOG(INFO) << "CheckPointManager: Added commit data.";
}

// Check if there are enough valid checkpoint proofs
bool CheckPointManager::IsValidCheckpointProof(
    const StableCheckPoint& stable_ckpt) {
  // Check if the signatures on the stable checkpoint are valid
  std::string hash = stable_ckpt_.hash();
  std::set<uint32_t> senders;
  for (const auto& signature : stable_ckpt_.signatures()) {
    if (!verifier_->VerifyMessage(hash, signature)) {
      return false;
    }
    senders.insert(signature.node_id());
  }

  // Check if there are enough unique senders to meet the minimum requirement
  return (senders.size() >= config_.GetMinDataReceiveNum()) ||
         (stable_ckpt.seq() == 0 && senders.size() == 0);
}

// Process a checkpoint request
int CheckPointManager::ProcessCheckPoint(std::unique_ptr<Context> context,
                                         std::unique_ptr<Request> request) {
  // Parse checkpoint data from the request
  CheckPointData checkpoint_data;
  if (!checkpoint_data.ParseFromString(request->data())) {
    LOG(ERROR) << "parse checkpoint data fail:";
    return -2;
  }
  uint64_t checkpoint_seq = checkpoint_data.seq();
  uint32_t sender_id = request->sender_id();
  int water_mark = config_.GetCheckPointWaterMark();
  if (checkpoint_seq % water_mark) {
    LOG(ERROR) << "checkpoint seq not invalid:" << checkpoint_seq;
    return -2;
  }

  // Verify the checkpoint data signatures
  if (verifier_) {
    bool valid = verifier_->VerifyMessage(checkpoint_data.hash(),
                                          checkpoint_data.hash_signature());
    if (!valid) {
      LOG(ERROR) << "request is not valid:"
                 << checkpoint_data.hash_signature().DebugString();
      return -2;
    }
  }

  {
    std::lock_guard<std::mutex> lk(mutex_);
    auto res =
        sender_ckpt_[std::make_pair(checkpoint_seq, checkpoint_data.hash())]
            .insert(sender_id);
    if (res.second) {
      sign_ckpt_[std::make_pair(checkpoint_seq, checkpoint_data.hash())]
          .push_back(checkpoint_data.hash_signature());
      new_data_++;
    }
    if (sender_ckpt_[std::make_pair(checkpoint_seq, checkpoint_data.hash())]
            .size() == 1) {
      for (auto& hash_ : checkpoint_data.hashs()) {
        hash_ckpt_[std::make_pair(checkpoint_seq, checkpoint_data.hash())]
            .push_back(hash_);
      }
    }
    Notify();
  }
  return 0;
}

// Notify waiting threads
void CheckPointManager::Notify() {
  std::lock_guard<std::mutex> lk(cv_mutex_);
  cv_.notify_all();
}

// Wait for new data
bool CheckPointManager::Wait() {
  int timeout_ms = 1000;
  std::unique_lock<std::mutex> lk(cv_mutex_);
  return cv_.wait_for(lk, std::chrono::milliseconds(timeout_ms),
                      [&] { return new_data_ > 0; });
}

// Update the status of the stable checkpoint
void CheckPointManager::UpdateStableCheckPointStatus() {
  uint64_t last_committable_seq = 0;
  int water_mark = config_.GetCheckPointWaterMark();
  while (!stop_) {
    if (!Wait()) {
      continue;
    }
    uint64_t stable_seq = 0;
    std::string stable_hash;
    {
      std::lock_guard<std::mutex> lk(mutex_);
      for (auto it : sender_ckpt_) {
        if (it.second.size() >=
            static_cast<size_t>(config_.GetMinCheckpointReceiveNum())) {
          committable_seq_ = it.first.first;
          committable_hash_ = it.first.second;
          std::set<uint32_t> senders_ =
              sender_ckpt_[std::make_pair(committable_seq_, committable_hash_)];
          sem_post(&committable_seq_signal_);
          if (last_seq_ < committable_seq_ &&
              last_committable_seq < committable_seq_) {
            auto replicas_ = config_.GetReplicaInfos();
            for (auto& replica_ : replicas_) {
              std::string last_hash;
              uint64_t last_seq;
              {
                std::lock_guard<std::mutex> lk(lt_mutex_);
                last_hash = last_hash_;
                last_seq = last_seq_;
              }
              if (senders_.count(replica_.id()) &&
                  last_seq < committable_seq_) {
                auto requests = txn_accessor_.GetRequestFromReplica(
                    last_seq + 1, committable_seq_, replica_);
                if (requests.ok()) {
                  bool fail = false;
                  for (auto& request : *requests) {
                    if (SignatureVerifier::CalculateHash(request.data()) !=
                        request.hash()) {
                      LOG(ERROR)
                          << "The hash of the request does not match the data.";
                      fail = true;
                      break;
                    }
                    last_hash = GetHash(last_hash, request.hash());
                  }
                  if (fail) {
                    continue;
                  } else if (last_hash != committable_hash_) {
                    LOG(ERROR) << "The hash of requests returned do not match. "
                               << last_seq + 1 << " " << committable_seq_;
                  } else {
                    last_committable_seq = committable_seq_;
                    for (auto& request : *requests) {
                      if (executor_) {
                        executor_->Commit(std::make_unique<Request>(request));
                      }
                    }
                    SetHighestPreparedSeq(committable_seq_);
                    break;
                  }
                }
              }
            }
          }
        }
        if (it.second.size() >=
            static_cast<size_t>(config_.GetMinDataReceiveNum())) {
          stable_seq = it.first.first;
          stable_hash = it.first.second;
        }
      }
      new_data_ = 0;
    }

    std::vector<SignatureInfo> votes;
    if (current_stable_seq_ < stable_seq) {
      std::lock_guard<std::mutex> lk(mutex_);
      votes = sign_ckpt_[std::make_pair(stable_seq, stable_hash)];
      std::set<uint32_t> senders_ =
          sender_ckpt_[std::make_pair(stable_seq, stable_hash)];

      auto it = sender_ckpt_.begin();
      while (it != sender_ckpt_.end()) {
        if (it->first.first <= stable_seq) {
          sign_ckpt_.erase(sign_ckpt_.find(it->first));
          auto tmp = it++;
          sender_ckpt_.erase(tmp);
        } else {
          it++;
        }
      }
      stable_ckpt_.set_seq(stable_seq);
      stable_ckpt_.set_hash(stable_hash);
      stable_ckpt_.mutable_signatures()->Clear();
      for (auto vote : votes) {
        *stable_ckpt_.add_signatures() = vote;
      }
      current_stable_seq_ = stable_seq;
    }
    UpdateStableCheckPointCallback(current_stable_seq_);
  }
}

// Set a callback function for updating the stable checkpoint
void CheckPointManager::SetTimeoutHandler(std::function<void()> timeout_handler) {
  timeout_handler_ = timeout_handler;
}

// Execute the timeout handler
void CheckPointManager::TimeoutHandler() {
  if (timeout_handler_) {
    timeout_handler_();
  }
}

// Update the checkpoint status
void CheckPointManager::UpdateCheckPointStatus() {
  uint64_t last_ckpt_seq = 0;
  int water_mark = config_.GetCheckPointWaterMark();
  int timeout_ms = config_.GetViewchangeCommitTimeout();
  std::vector<std::string> stable_hashs;
  std::vector<uint64_t> stable_seqs;
  while (!stop_) {
    auto request = data_queue_.Pop(timeout_ms);
    if (request == nullptr) {
      continue;
    }
    std::string hash_ = request->hash();
    uint64_t current_seq = request->seq();
    if (current_seq != last_seq_ + 1) {
      LOG(ERROR) << "CheckPointManager: Seq invalid - last_seq: " << last_seq_ << " current_seq: " << current_seq;
      continue;
    }
    {
      std::lock_guard<std::mutex> lk(lt_mutex_);
      last_hash_ = GetHash(last_hash_, request->hash());
      last_seq_++;
    }
    txn_db_->Put(std::move(request));

    if (current_seq == last_ckpt_seq + water_mark) {
      last_ckpt_seq = current_seq;
      BroadcastCheckPoint(last_ckpt_seq, last_hash_, stable_hashs, stable_seqs);
      LOG(INFO) << "CheckPointManager: Broadcasted checkpoint - seq: " << last_ckpt_seq;
    }
  }
  return;
}

// Broadcast checkpoint data to replicas
void CheckPointManager::BroadcastCheckPoint(
    uint64_t seq, const std::string& hash,
    const std::vector<std::string>& stable_hashs,
    const std::vector<uint64_t>& stable_seqs) {
  CheckPointData checkpoint_data;
  std::unique_ptr<Request> checkpoint_request = NewRequest(
      Request::TYPE_CHECKPOINT, Request(), config_.GetSelfInfo().id());
  checkpoint_data.set_seq(seq);
  checkpoint_data.set_hash(hash);
  if (verifier_) {
    auto signature_or = verifier_->SignMessage(hash);
    if (!signature_or.ok()) {
      LOG(ERROR) << "Sign message fail";
      return;
    }
    *checkpoint_data.mutable_hash_signature() = *signature_or;
  }

  checkpoint_data.SerializeToString(checkpoint_request->mutable_data());
  replica_communicator_->BroadCast(*checkpoint_request);
}

// Wait for a signal
void CheckPointManager::WaitSignal() {
  std::unique_lock<std::mutex> lk(mutex_);
  signal_.wait(lk, [&] { return !stable_hash_queue_.Empty(); });
}

// Pop a stable sequence and hash from the queue
std::unique_ptr<std::pair<uint64_t, std::string>>
CheckPointManager::PopStableSeqHash() {
  return stable_hash_queue_.Pop();
}

// Get the highest prepared sequence
uint64_t CheckPointManager::GetHighestPreparedSeq() {
  std::lock_guard<std::mutex> lk(lt_mutex_);
  return highest_prepared_seq_;
}

// Set the highest prepared sequence
void CheckPointManager::SetHighestPreparedSeq(uint64_t seq) {
  std::lock_guard<std::mutex> lk(lt_mutex_);
  highest_prepared_seq_ = seq;
}

// Get the semaphore for committable sequence signaling
sem_t* CheckPointManager::CommitableSeqSignal() {
  std::lock_guard<std::mutex> lk(lt_mutex_);
  return &committable_seq_signal_;
}

// Get the committable sequence
uint64_t CheckPointManager::GetCommittableSeq() {
  std::lock_guard<std::mutex> lk(lt_mutex_);
  return committable_seq_;
}

}  // namespace resdb

