#!/bin/sh

import sys
import json

from oracle_script.comm.utils import *
from oracle_script.comm.comm_config import *
from proto.replica_info_pb2 import ResConfigData,ReplicaInfo
from google.protobuf.json_format import MessageToJson
from google.protobuf.json_format import Parse, ParseDict

cert_path="pbft_cert/"
'''
{
  region : {
    replica_info : {
      id:1,
      ip:"10.2.0.64",
      port: 37001,
    },
    replica_info : {
      id:2,
      ip:"10.2.0.162",
      port: 37002,
    },
    replica_info : {
      id:3,
      ip:"10.2.0.115",
      port: 37003,
    },
    replica_info : {
      id:4,
      ip:"10.2.0.47",
      port: 37004,
    },
    region_id: 1,
  },
  self_region_id:1,
  not_need_signature: true,
  is_performance_running:true,
  view_change_timeout_ms:30000000,
}
'''

def gen_svr_config(config):
    iplist=get_ips(config["svr_ip_file"])

    info_list = []
    for idx,ip in iplist:
        port = int(config["base_port"]) + int(idx)
        info={}
        info["id"] = idx
        info["ip"] = ip
        info["port"] = port
        info_list.append(info)

    with open(config["svr_config_path"],"w") as f:
        config_data=ResConfigData() 
        region = config_data.region.add()
        region.region_id=1
        for info in info_list:
            replica = Parse(json.dumps(info), ReplicaInfo())
            region.replica_info.append(replica)

        config_data.self_region_id = 1
        config_data.not_need_signature = True
        config_data.is_performance_running = True
        config_data.view_change_timeout_ms = 300000000
        config_data.client_batch_num = 1000
        config_data.max_process_txn = 2048

        config_data.worker_num=8
        if "WORKER_NUM" in os.environ:
            print ("evn worker:",os.environ["WORKER_NUM"])
            config_data.worker_num=int(os.environ["WORKER_NUM"])
        config_data.input_worker_num = 2
        config_data.output_worker_num = 2
        json_obj = MessageToJson(config_data)
        f.write(json_obj)

def kill_svr(config):
    iplist=get_ips(config["svr_ip_file"])+get_ips(config["cli_ip_file"])
    cmd_list=[]
    for (idx,svr_ip) in iplist:
        cmd="killall -9 {};".format(config["svr_bin"])
        cmd_list.append((svr_ip,cmd))
        run_remote_cmd(svr_ip, cmd)
    #run_remote_cmd_list(cmd_list)

def run_svr(config):
    iplist=get_ips(config["svr_ip_file"])
    cli_iplist=get_ips(config["cli_ip_file"])
    for (idx,svr_ip) in iplist+cli_iplist:
        private_key="{}/node_{}.key.pri".format(cert_path,idx)
        cert="{}/cert_{}.cert".format(cert_path,idx)
        if [idx,svr_ip] in iplist:
            cmd="nohup {} {} {} {} > server{}.log 2>&1 &".format(
		    get_remote_file_name(config["svr_bin_bazel_path"]),
		    get_remote_file_name(config["svr_config_path"]), 
		    private_key, 
		    cert, idx)
        else:
            cmd="nohup {} {} {} {} > client{}.log 2>&1 &".format(
		    get_remote_file_name(config["svr_bin_bazel_path"]),
		    get_remote_file_name(config["svr_config_path"]), 
		    private_key, 
		    cert, idx)

        run_remote_cmd(svr_ip, cmd)

def upload_svr(config):
    iplist=get_ips(config["svr_ip_file"]) + get_ips(config["cli_ip_file"])
    cmd_list=[]
    for (idx,svr_ip) in iplist:
        run_remote_cmd(svr_ip, "rm -rf {}; rm server*.log; rm -rf server.config; rm -rf cert; mkdir -p {};".format(get_remote_file_name(config["svr_bin_bazel_path"]), cert_path))
        private_key=config["cert_path"]+"/"+ "node_"+idx+".key.pri"
        cert=config["cert_path"]+ "/"+ "cert_"+idx+".cert"

        run_cmd_with_resp("scp -i {} {} ubuntu@{}:/home/ubuntu".format(KEY,config["svr_bin_bazel_path"],svr_ip))
        run_cmd_with_resp("scp -i {} {} ubuntu@{}:/home/ubuntu".format(KEY,config["svr_config_path"],svr_ip))
        run_cmd_with_resp("scp -i {} {} ubuntu@{}:/home/ubuntu/{}".format(KEY,private_key, svr_ip, cert_path))
        run_cmd_with_resp("scp -i {} {} ubuntu@{}:/home/ubuntu/{}".format(KEY,cert, svr_ip, cert_path))
    #run_remote_cmd_list_raw(cmd_list)


if __name__ == '__main__':
    config_file=sys.argv[1]
    config = read_config(config_file)
    print("config:{}".format(config))
    gen_svr_config(config)
    #kill_svr(config)
    #upload_svr(config)
    #run_svr(config)
