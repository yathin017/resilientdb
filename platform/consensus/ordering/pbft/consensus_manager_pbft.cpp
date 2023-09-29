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

#include "platform/consensus/ordering/pbft/consensus_manager_pbft.h"

#include <glog/logging.h>
#include <unistd.h>

#include "common/crypto/signature_verifier.h"

namespace resdb {

// Constructor for ConsensusManagerPBFT
ConsensusManagerPBFT::ConsensusManagerPBFT(
    const ResDBConfig& config, std::unique_ptr<TransactionManager> executor,
    std::unique_ptr<CustomQuery> query_executor)
    : ConsensusManager(config),
      system_info_(std::make_unique<SystemInfo>(config)),
      checkpoint_manager_(std::make_unique<CheckPointManager>(
          config, GetBroadCastClient(), GetSignatureVerifier())),
      message_manager_(std::make_unique<MessageManager>(
          config, std::move(executor), checkpoint_manager_.get(),
          system_info_.get())),
      commitment_(std::make_unique<Commitment>(config_, message_manager_.get(),
                                               GetBroadCastClient(),
                                               GetSignatureVerifier())),
      query_(std::make_unique<Query>(config_, message_manager_.get(),
                                     std::move(query_executor))),
      response_manager_(config_.IsPerformanceRunning()
                            ? nullptr
                            : std::make_unique<ResponseManager>(
                                  config_, GetBroadCastClient(),
                                  system_info_.get(), GetSignatureVerifier())),
      performance_manager_(config_.IsPerformanceRunning()
                               ? std::make_unique<PerformanceManager>(
                                     config_, GetBroadCastClient(),
                                     system_info_.get(), GetSignatureVerifier())
                               : nullptr),
      view_change_manager_(std::make_unique<ViewChangeManager>(
          config_, checkpoint_manager_.get(), message_manager_.get(),
          system_info_.get(), GetBroadCastClient(), GetSignatureVerifier())),
      recovery_(std::make_unique<Recovery>(config_, checkpoint_manager_.get(),
                                           system_info_.get(),
                                           message_manager_->GetStorage())) {
  LOG(INFO) << "is running is performance mode:"
            << config_.IsPerformanceRunning();
  global_stats_ = Stats::GetGlobalStats();

  view_change_manager_->SetDuplicateManager(commitment_->GetDuplicateManager());

  // Initialize the system info with data from log files if available.
  recovery_->ReadLogs(
      [&](const SystemInfoData& data) {
        system_info_->SetCurrentView(data.view());
        system_info_->SetPrimary(data.primary_id());
      },
      [&](std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
        return InternalConsensusCommit(std::move(context), std::move(request));
      });
}

// Set the need for commit QC (Quality of Commit) based on input.
void ConsensusManagerPBFT::SetNeedCommitQC(bool need_qc) {
  commitment_->SetNeedCommitQC(need_qc);
}

// Start the consensus manager.
void ConsensusManagerPBFT::Start() {
  LOG(INFO) << "ConsensusManagerPBFT is starting.";
  ConsensusManager::Start();
}

// Get a list of replicas.
std::vector<ReplicaInfo> ConsensusManagerPBFT::GetReplicas() {
  return message_manager_->GetReplicas();
}

// Get the primary replica.
uint32_t ConsensusManagerPBFT::GetPrimary() {
  return system_info_->GetPrimaryId();
}

// Get the current version or view.
uint32_t ConsensusManagerPBFT::GetVersion() {
  return system_info_->GetCurrentView();
}

// Set the primary replica and version.
void ConsensusManagerPBFT::SetPrimary(uint32_t primary, uint64_t version) {
  if (version > system_info_->GetCurrentView()) {
    system_info_->SetCurrentView(version);
    system_info_->SetPrimary(primary);
    LOG(INFO) << "Set primary to Replica ID: " << primary
              << ", View: " << version;
  }
}

// Add a pending request to the queue.
void ConsensusManagerPBFT::AddPendingRequest(std::unique_ptr<Context> context,
                                             std::unique_ptr<Request> request) {
  std::lock_guard<std::mutex> lk(mutex_);
  request_pending_.push(std::make_pair(std::move(context), std::move(request)));
}

// Add a complained request to the queue.
void ConsensusManagerPBFT::AddComplainedRequest(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  std::lock_guard<std::mutex> lk(mutex_);
  request_complained_.push(
      std::make_pair(std::move(context), std::move(request)));
}

// Pop a pending request from the queue.
absl::StatusOr<std::pair<std::unique_ptr<Context>, std::unique_ptr<Request>>>
ConsensusManagerPBFT::PopPendingRequest() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (request_pending_.empty()) {
    LOG(ERROR) << "PopPendingRequest: No pending requests available.";
    return absl::InternalError("No Data.");
  }
  auto new_request = std::move(request_pending_.front());
  request_pending_.pop();
  return new_request;
}

// Pop a complained request from the queue.
absl::StatusOr<std::pair<std::unique_ptr<Context>, std::unique_ptr<Request>>>
ConsensusManagerPBFT::PopComplainedRequest() {
  std::lock_guard<std::mutex> lk(mutex_);
  if (request_complained_.empty()) {
    LOG(ERROR) << "PopComplainedRequest: No complained requests available.";
    return absl::InternalError("No Data.");
  }
  auto new_request = std::move(request_complained_.front());
  request_complained_.pop();
  return new_request;
}

// The implementation of PBFT consensus.
int ConsensusManagerPBFT::ConsensusCommit(std::unique_ptr<Context> context,
                                          std::unique_ptr<Request> request) {
  recovery_->AddRequest(context.get(), request.get());

  // If view change is enabled, handle requests accordingly.
  if (config_.GetConfigData().enable_viewchange()) {
    view_change_manager_->MayStart();
    if (view_change_manager_->IsInViewChange()) {
      switch (request->type()) {
        case Request::TYPE_NEW_TXNS:
        case Request::TYPE_PRE_PREPARE:
        case Request::TYPE_PREPARE:
        case Request::TYPE_COMMIT:
          AddPendingRequest(std::move(context), std::move(request));
          return 0;
      }
    } else {
      while (true) {
        auto new_request = PopPendingRequest();
        if (!new_request.ok()) {
          break;
        }
        int ret = InternalConsensusCommit(std::move((*new_request).first),
                                          std::move((*new_request).second));
        LOG(INFO) << "Handling pending request of type: "
                  << (*new_request).second->type() << ", Result: " << ret;
      }
    }
  }

  int ret = InternalConsensusCommit(std::move(context), std::move(request));

  if (config_.GetConfigData().enable_viewchange()) {
    if (ret == -4) {
      while (true) {
        auto new_request = PopComplainedRequest();
        if (!new_request.ok()) {
          break;
        }
        int ret = InternalConsensusCommit(std::move((*new_request).first),
                                          std::move((*new_request).second));
        LOG(INFO) << "Handling complained request of type: "
                  << (*new_request).second->type() << ", Result: " << ret;
      }
    }
  }

  return ret;
}

// Internal implementation of PBFT consensus commit.
int ConsensusManagerPBFT::InternalConsensusCommit(
    std::unique_ptr<Context> context, std::unique_ptr<Request> request) {
  switch (request->type()) {
    case Request::TYPE_CLIENT_REQUEST:
      if (config_.IsPerformanceRunning()) {
        return performance_manager_->StartEval();
      }
      return response_manager_->NewUserRequest(std::move(context),
                                               std::move(request));
    case Request::TYPE_RESPONSE:
      if (config_.IsPerformanceRunning()) {
        return performance_manager_->ProcessResponseMsg(std::move(context),
                                                        std::move(request));
      }
      return response_manager_->ProcessResponseMsg(std::move(context),
                                                   std::move(request));
    case Request::TYPE_NEW_TXNS: {
      uint64_t proxy_id = request->proxy_id();
      std::string hash = request->hash();
      int ret = commitment_->ProcessNewRequest(std::move(context),
                                               std::move(request));
      if (ret == -3) {
        std::pair<std::unique_ptr<Context>, std::unique_ptr<Request>>
            request_complained;
        {
          std::lock_guard<std::mutex> lk(commitment_->rc_mutex_);

          request_complained =
              std::move(commitment_->request_complained_.front());
          commitment_->request_complained_.pop();
        }
        AddComplainedRequest(std::move(request_complained.first),
                             std::move(request_complained.second));
        view_change_manager_->AddComplaintTimer(proxy_id, hash);
      }
      return ret;
    }
    case Request::TYPE_PRE_PREPARE:
      return commitment_->ProcessProposeMsg(std::move(context),
                                            std::move(request));
    case Request::TYPE_PREPARE:
      return commitment_->ProcessPrepareMsg(std::move(context),
                                            std::move(request));
    case Request::TYPE_COMMIT:
      return commitment_->ProcessCommitMsg(std::move(context),
                                           std::move(request));
    case Request::TYPE_CHECKPOINT:
      return checkpoint_manager_->ProcessCheckPoint(std::move(context),
                                                    std::move(request));
    case Request::TYPE_VIEWCHANGE:
      return view_change_manager_->ProcessViewChange(std::move(context),
                                                     std::move(request));
    case Request::TYPE_NEWVIEW:
      return view_change_manager_->ProcessNewView(std::move(context),
                                                  std::move(request));
    case Request::TYPE_QUERY:
      return query_->ProcessQuery(std::move(context), std::move(request));
    case Request::TYPE_REPLICA_STATE:
      return query_->ProcessGetReplicaState(std::move(context),
                                            std::move(request));
    case Request::TYPE_CUSTOM_QUERY:
      return query_->ProcessCustomQuery(std::move(context), std::move(request));
    
    // yathin017
    case Request::TYPE_SYSTEM_INFO:
      system_info_->ProcessRequest(std::move(request));
      return 0;
  }
  return 0;
}

// Set a function to provide performance data.
void ConsensusManagerPBFT::SetupPerformanceDataFunc(
    std::function<std::string()> func) {
  performance_manager_->SetDataFunc(func);
}

// Set a function for pre-verification of requests.
void ConsensusManagerPBFT::SetPreVerifyFunc(
    std::function<bool(const Request&)> func) {
  commitment_->SetPreVerifyFunc(func);
}

}  // namespace resdb
