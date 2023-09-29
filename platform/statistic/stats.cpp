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

#include "platform/statistic/stats.h"

#include <glog/logging.h>

#include "common/utils/utils.h"

namespace resdb {

std::mutex g_mutex;

// Get the global Stats instance with a specified update frequency.
// Uses lazy initialization to create a single instance.
Stats* Stats::GetGlobalStats(int seconds) {
  std::unique_lock<std::mutex> lk(g_mutex);
  static Stats stats(seconds);
  return &stats;
}

// Constructor for the Stats class.
Stats::Stats(int sleep_time) {
  monitor_sleep_time_ = sleep_time;
#ifdef TEST_MODE
  monitor_sleep_time_ = 1;
#endif
  // Initialize various counters and flags.
  num_call_ = 0;
  num_commit_ = 0;
  run_time_ = 0;
  run_call_ = 0;
  run_call_time_ = 0;
  server_call_ = 0;
  server_process_ = 0;
  run_req_num_ = 0;
  run_req_run_time_ = 0;
  seq_gap_ = 0;
  total_request_ = 0;
  total_geo_request_ = 0;
  geo_request_ = 0;

  stop_ = false;
  begin_ = false;

  socket_recv_ = 0;
  broad_cast_msg_ = 0;
  send_broad_cast_msg_ = 0;

  prometheus_ = nullptr;

  // Start a thread to monitor and log statistics.
  global_thread_ = std::thread(&Stats::MonitorGlobal, this);  // pass by reference
}

// Stop the monitoring thread.
void Stats::Stop() {
  stop_ = true;
}

// Destructor for the Stats class.
Stats::~Stats() {
  stop_ = true;
  if (global_thread_.joinable()) {
    global_thread_.join();
  }
}

// Monitor and log global statistics at regular intervals.
void Stats::MonitorGlobal() {
  LOG(ERROR) << "monitor:" << name_ << " sleep time:" << monitor_sleep_time_;

  uint64_t seq_fail = 0;
  uint64_t client_call = 0, socket_recv = 0;
  uint64_t num_client_req = 0, num_propose = 0, num_prepare = 0, num_commit = 0,
           pending_execute = 0, execute = 0, execute_done = 0;
  uint64_t broad_cast_msg = 0, send_broad_cast_msg = 0;
  uint64_t send_broad_cast_msg_per_rep = 0;
  uint64_t server_call = 0, server_process = 0;
  uint64_t seq_gap = 0;
  uint64_t total_request = 0, total_geo_request = 0, geo_request = 0;
  uint64_t run_req_num = 0, run_req_run_time = 0;

  uint64_t last_run_req_num = 0, last_run_req_run_time = 0;
  uint64_t last_seq_fail = 0;
  uint64_t last_num_client_req = 0, last_num_propose = 0, last_num_prepare = 0,
           last_num_commit = 0;
  uint64_t last_pending_execute = 0, last_execute = 0, last_execute_done = 0;
  uint64_t last_client_call = 0, last_socket_recv = 0;
  uint64_t last_broad_cast_msg = 0, last_send_broad_cast_msg = 0;
  uint64_t last_send_broad_cast_msg_per_rep = 0;
  uint64_t last_server_call = 0, last_server_process = 0;
  uint64_t last_total_request = 0, last_total_geo_request = 0,
           last_geo_request = 0;
  uint64_t time = 0;

  while (!stop_) {
    sleep(monitor_sleep_time_);
    time += monitor_sleep_time_;
    // Retrieve and log statistics.
    // (Note: Logic to calculate differences between successive statistics counts.)
    seq_fail = seq_fail_;
    socket_recv = socket_recv_;
    client_call = client_call_;
    num_client_req = num_client_req_;
    num_propose = num_propose_;
    num_prepare = num_prepare_;
    num_commit = num_commit_;
    pending_execute = pending_execute_;
    execute = execute_;
    execute_done = execute_done_;
    broad_cast_msg = broad_cast_msg_;
    send_broad_cast_msg = send_broad_cast_msg_;
    send_broad_cast_msg_per_rep = send_broad_cast_msg_per_rep_;
    server_call = server_call_;
    server_process = server_process_;
    seq_gap = seq_gap_;
    total_request = total_request_;
    total_geo_request = total_geo_request_;
    geo_request = geo_request_;

    run_req_num = run_req_num_;
    run_req_run_time = run_req_run_time_;

    LOG(ERROR) << "=========== monitor =========\n"
               << "server call:" << server_call - last_server_call
               << " server process:" << server_process - last_server_process
               << " socket recv:" << socket_recv - last_socket_recv
               << " "
                  "client call:"
               << client_call - last_client_call
               << " "
                  "client req:"
               << num_client_req - last_num_client_req
               << " "
                  "broad_cast:"
               << broad_cast_msg - last_broad_cast_msg
               << " "
                  "send broad_cast:"
               << send_broad_cast_msg - last_send_broad_cast_msg
               << " "
                  "per send broad_cast:"
               << send_broad_cast_msg_per_rep - last_send_broad_cast_msg_per_rep
               << " "
                  "propose:"
               << num_propose - last_num_propose
               << " "
                  "prepare:"
               << (num_prepare - last_num_prepare)
               << " "
                  "commit:"
               << (num_commit - last_num_commit)
               << " "
                  "pending execute:"
               << pending_execute - last_pending_execute
               << " "
                  "execute:"
               << execute - last_execute
               << " "
                  "execute done:"
               << execute_done - last_execute_done << " seq gap:" << seq_gap
               << " total request:" << total_request - last_total_request
               << " txn:" << (total_request - last_total_request) / 5
               << " total geo request:"
               << total_geo_request - last_total_geo_request
               << " total geo request per:"
               << (total_geo_request - last_total_geo_request) / 5
               << " geo request:" << (geo_request - last_geo_request)
               << " "
                  "seq fail:"
               << seq_fail - last_seq_fail << " time:" << time
               << " "
                  "\n--------------- monitor ------------";
    if (run_req_num - last_run_req_num > 0) {
      LOG(ERROR) << "  req client latency:"
                 << static_cast<double>(run_req_run_time -
                                        last_run_req_run_time) /
                        (run_req_num - last_run_req_num) / 1000000000.0;
    }

    last_seq_fail = seq_fail;
    last_socket_recv = socket_recv;
    last_client_call = client_call;
    last_num_client_req = num_client_req;
    last_num_propose = num_propose;
    last_num_prepare = num_prepare;
    last_num_commit = num_commit;
    last_pending_execute = pending_execute;
    last_execute = execute;
    last_execute_done = execute_done;

    last_broad_cast_msg = broad_cast_msg;
    last_send_broad_cast_msg = send_broad_cast_msg;
    last_send_broad_cast_msg_per_rep = send_broad_cast_msg_per_rep;

    last_server_call = server_call;
    last_server_process = server_process;

    last_run_req_num = run_req_num;
    last_run_req_run_time = run_req_run_time;
    last_total_request = total_request;
    last_total_geo_request = total_geo_request;
    last_geo_request = geo_request;
  }
}

// Increment the client call counter and update Prometheus metrics.
void Stats::IncClientCall() {
  if (prometheus_) {
    prometheus_->Inc(CLIENT_CALL, 1);
  }
  client_call_++;
}

// Increment the client request counter and update Prometheus metrics.
void Stats::IncClientRequest() {
  if (prometheus_) {
    prometheus_->Inc(CLIENT_REQ, 1);
  }
  num_client_req_++;
}

// Increment the propose counter and update Prometheus metrics.
void Stats::IncPropose() {
  if (prometheus_) {
    prometheus_->Inc(PROPOSE, 1);
  }
  num_propose_++;
}

// Increment the prepare counter and update Prometheus metrics.
void Stats::IncPrepare() {
  if (prometheus_) {
    prometheus_->Inc(PREPARE, 1);
  }
  num_prepare_++;
}

// Increment the commit counter and update Prometheus metrics.
void Stats::IncCommit() {
  if (prometheus_) {
    prometheus_->Inc(COMMIT, 1);
  }
  num_commit_++;
}

// Increment the pending execute counter.
void Stats::IncPendingExecute() {
  pending_execute_++;
}

// Increment the execute counter.
void Stats::IncExecute() {
  execute_++;
}

// Increment the execute done counter and update Prometheus metrics.
void Stats::IncExecuteDone() {
  if (prometheus_) {
    prometheus_->Inc(EXECUTE, 1);
  }
  execute_done_++;
}

// Increment the broadcast message counter and update Prometheus metrics.
void Stats::BroadCastMsg() {
  if (prometheus_) {
    prometheus_->Inc(BROAD_CAST, 1);
  }
  broad_cast_msg_++;
}

// Increment the send broadcast message counter.
void Stats::SendBroadCastMsg(uint32_t num) {
  send_broad_cast_msg_ += num;
}

// Increment the send broadcast message per replica counter.
void Stats::SendBroadCastMsgPerRep() {
  send_broad_cast_msg_per_rep_++;
}

// Increment the sequence fail counter.
void Stats::SeqFail() {
  seq_fail_++;
}

// Increment the total request counter and update Prometheus metrics.
void Stats::IncTotalRequest(uint32_t num) {
  if (prometheus_) {
    prometheus_->Inc(NUM_EXECUTE_TX, num);
  }
  total_request_ += num;
}

// Increment the total geo request counter.
void Stats::IncTotalGeoRequest(uint32_t num) {
  total_geo_request_ += num;
}

// Increment the geo request counter.
void Stats::IncGeoRequest() {
  geo_request_++;
}

// Increment the server call counter and update Prometheus metrics.
void Stats::ServerCall() {
  if (prometheus_) {
    prometheus_->Inc(SERVER_CALL_NAME, 1);
  }
  server_call_++;
}

// Increment the server process counter and update Prometheus metrics.
void Stats::ServerProcess() {
  if (prometheus_) {
    prometheus_->Inc(SERVER_PROCESS, 1);
  }
  server_process_++;
}

// Update the sequence gap.
void Stats::SeqGap(uint64_t seq_gap) {
  seq_gap_ = seq_gap;
}

// Add latency data for client requests.
void Stats::AddLatency(uint64_t run_time) {
  run_req_num_++;
  run_req_run_time_ += run_time;
}

// Set the Prometheus address for metrics export.
void Stats::SetPrometheus(const std::string& prometheus_address) {
  prometheus_ = std::make_unique<PrometheusHandler>(prometheus_address);
}

}  // namespace resdb
