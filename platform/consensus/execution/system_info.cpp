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

#include "platform/consensus/execution/system_info.h"

#include <glog/logging.h>

namespace resdb {

SystemInfo::SystemInfo() : primary_id_(1), view_(1) {}

SystemInfo::SystemInfo(const ResDBConfig& config)
    : primary_id_(config.GetReplicaInfos()[0].id()), view_(1) {
  SetReplicas(config.GetReplicaInfos());
  LOG(ERROR) << "get primary id:" << primary_id_;
}

uint32_t SystemInfo::GetPrimaryId() const { return primary_id_; }

void SystemInfo::SetPrimary(uint32_t id) { primary_id_ = id; }

uint64_t SystemInfo::GetCurrentView() const { return view_; }

void SystemInfo::SetCurrentView(uint64_t view_id) { view_ = view_id; }

std::vector<ReplicaInfo> SystemInfo::GetReplicas() const { return replicas_; }

void SystemInfo::SetReplicas(const std::vector<ReplicaInfo>& replicas) {
  replicas_ = replicas;
}

// yathin017
void SystemInfo::AddReplica(const ReplicaInfo& replica) {
  LOG(INFO) << "#### ADD REPLICA ####";
  LOG(INFO) << "Existing replicas:\n";
  for (const auto& cur_replica : replicas_) {
    LOG(INFO) << "Replica ID:" << cur_replica.id() << " IP: " << cur_replica.ip();
  }

  // if (replica.id() == 0 || replica.ip().empty() || replica.port() == 0) {
  //   return;
  // }

  for (const auto& cur_replica : replicas_) {
    if (cur_replica.id() == replica.id()) {
      LOG(ERROR) << "Replica with id:" << replica.id() << " already exist.";
      return;
    }
  }
  LOG(ERROR) << "Adding new replica \n" << replica.DebugString();
  replicas_.push_back(replica);

  LOG(INFO) << "Existing replicas:\n";
  for (const auto& cur_replica : replicas_) {
    LOG(INFO) << "Replica ID:" << cur_replica.id() << " IP: " << cur_replica.ip();
  }
}

void SystemInfo::RemoveReplica(int64_t replica_id) {
  LOG(INFO) << "#### REMOVE REPLICA ####";
  LOG(INFO) << "Existing replicas:\n";
  for (const auto& cur_replica : replicas_) {
    LOG(INFO) << "Replica ID:" << cur_replica.id() << " IP: " << cur_replica.ip();
  }

  auto it = std::remove_if(replicas_.begin(), replicas_.end(),
      [replica_id](const ReplicaInfo& replica) { return replica.id() == replica_id; });

  if (it != replicas_.end()) {
    replicas_.erase(it, replicas_.end());
    LOG(ERROR) << "Removed replica with ID: " << replica_id;
  } else {
    LOG(ERROR) << "Replica with ID " << replica_id << " not found.";
  }

  LOG(INFO) << "Existing replicas:\n";
  for (const auto& cur_replica : replicas_) {
    LOG(INFO) << "Replica ID:" << cur_replica.id() << " IP: " << cur_replica.ip();
  }
}

void SystemInfo::ProcessRequest(const SystemInfoRequest& request) {
  switch (request.type()) {
    case SystemInfoRequest::ADD_REPLICA: {
      NewReplicaRequest info;
      if (info.ParseFromString(request.request())) {
        AddReplica(info.replica_info());
      }
    } break;
    case SystemInfoRequest::REMOVE_REPLICA: {
      RemoveReplicaRequest removeInfo;
      if (removeInfo.ParseFromString(request.request())) {
        RemoveReplica(removeInfo.replica_info());
      }
    } break;
    default:
      break;
  }
}

}  // namespace resdb
