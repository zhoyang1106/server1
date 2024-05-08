from aiohttp import web, ClientSession
from aiohttp.web_request import Request
import multiprocessing
import docker # type: ignore
import logging
import paramiko # type: ignore
import time
from typing import List
import requests
import random
import numpy as np
import gurobipy as gp # type: ignore
from gurobipy import GRB # type: ignore
import sys
import jsonlines

class WorkerNode:
    def __init__(self, container_url, node_ip, username, password, state, swarm_port, role="worker", container_id=None):
        self.container_id = container_id
        self.container_url = container_url
        self.node_ip = node_ip
        self.node_username = username
        self.node_password = password
        self.state = state
        self.swarm_port = swarm_port
        self.role = role

        


class RequestHandler:
    # with out add and minuse worker nodes until kill this program
    def __init__(self, worker_nodes:List[WorkerNode]):
        global monitor_dict
        # without node status check
        self.worker_nodes = worker_nodes
        self.session = None
        # prev data exists (True) or not (False) [Default == Fasle]
        self.flag = False

        self.alpha = 0.5
        self.beta = 0.3
        self.gamma = 0.2

        self.hdd_max_percent = 0.9875
        self.mem_max_percent = 1.0

        self.hdd_bytes = pow(2, 30) * 24
        self.mem_bytes = pow(2, 30) * 3.816 * self.mem_max_percent 

        # round robin algorithm initial vairable
        # TODO more algorithm ...
        self.current_index = 0

        # servers_max_data
        self.server_max_data = dict()
        for key in monitor_dict.keys():
            self.server_max_data[key] = {
                "hdd": 0.9875,
                "mem": 1.0
            }

        self.model = gp.Model("server_optimization")
        self.model.setParam('LogFile', "./gurobi.log")
        self.model.setParam('LogToConsole', 0)
        self.x = self.model.addVars(len(monitor_dict), vtype=GRB.BINARY, name="x")  # x = 0 or 1

    def LB_algorithm_Min(self, task_hdd_usage = 0, task_mem_usage = 0):
        global monitor_dict
        # servers_data
        servers_data = dict()
        for key in monitor_dict.keys():
            servers_data[key] = {
                "hdd": monitor_dict[key]['hdd'],
                "mem": monitor_dict[key]['mem'],
                "response-time": monitor_dict[key]['response-time']
            }

        b_hdd = self.model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_hdd")
        b_mem = self.model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_mem")
        num_servers = len(servers_data)
        self.x = self.model.addVars(num_servers, vtype=GRB.BINARY, name="x")  # x = 0 or 1

        for key in monitor_dict.keys():
            self.model.setObjective(
                (self.alpha * (b_hdd[0] / self.server_max_data[key]["hdd"])) +
                (self.beta * (b_mem[0] / self.server_max_data[key]['mem'])) +
                self.gamma * gp.quicksum(servers_data[key]['response-time'] * self.x[i] for i in range(num_servers)), GRB.MINIMIZE)   # task_response_time_predicted

        # Add constraint
        self.model.addConstr(gp.quicksum(self.x[i] for i in range(num_servers)) == 1, "c1")
        for i in range(num_servers):
            self.model.addConstr(servers_data[self.worker_nodes[i].node_ip]['hdd'] + task_hdd_usage * self.x[i] <= self.server_max_data[self.worker_nodes[i].node_ip]['hdd'], f"c2_{i + 1}")
            self.model.addConstr(servers_data[self.worker_nodes[i].node_ip]['mem'] + task_mem_usage * self.x[i] <= self.server_max_data[self.worker_nodes[i].node_ip]['mem'], f"c3_{i + 1}")
            # b_hdd
            self.model.addConstr(servers_data[self.worker_nodes[i].node_ip]['hdd'] + task_hdd_usage * self.x[i] <= b_hdd[0], f"c4_{i + 1}")
            # b_mem
            self.model.addConstr(servers_data[self.worker_nodes[i].node_ip]['mem'] + task_mem_usage * self.x[i] <= b_mem[0], f"c5_{i + 1}")

        # Optimize model
        self.model.optimize()

        # Get result
        selected_server_index = None
        if self.model.status == GRB.OPTIMAL:
            for i in range(num_servers):
                if self.x[i].X > 0.5:  # if x[i] is closer to 1, then the server is selected
                    selected_server_index = i
                    return self.worker_nodes[selected_server_index].container_url, self.worker_nodes[selected_server_index].node_ip



    async def start(self):
        self.session = ClientSession()

    def prev_request(self):
        global monitor_dict
        for node in self.worker_nodes:
            url = node.container_url
            start_time = time.time()
            data = {
                "number": 8000,
            }
            headers = {
                "task_type": "C",
            }
            response = requests.post(url=url, data=data, headers=headers)
            end_time = time.time()

            monitor_dict[node.node_ip]["response-time"] = end_time - start_time

            with jsonlines.open('./request-data.jsonl', 'a') as f:
                for key in monitor_dict.keys():
                    f.write({key: {
                        "response-time": monitor_dict[key]['response-time'],
                        "mem": monitor_dict[key]['mem'],
                        "hdd": monitor_dict[key]['hdd']
                        }
                    })

            print(response.json())

    async def forwarding(self, request: Request):
        global monitor_dict
        start_time = time.time()
        # collect resouces data before start request forwarding
        if not self.flag:
            self.prev_request()
            self.flag = True

        # round robin algorithm choose urls
        # TODO more algorithm ...
        data = await request.post()
        headers = request.headers
        task_hdd_usage = int(data.get('size')) if headers['task_type'] == 'H' else 0
        task_mem_usage = int(data.get('size')) if headers['task_type'] == 'M' else 0



        with jsonlines.open("./request-data.jsonl", "a") as f:
            f.write({"request data": {
                "task-type": headers["task_type"],
                "task-hdd-usage": task_hdd_usage / self.hdd_bytes,
                "task-mem-usage": task_mem_usage / self.mem_bytes,
            }})

        url, node_ip = self.LB_algorithm_Min(task_hdd_usage=task_hdd_usage / self.hdd_bytes, task_mem_usage=task_mem_usage / self.mem_bytes)
        # url = self.worker_nodes[self.current_index].container_url
        # self.current_index = (self.current_index + 1) % len(self.worker_nodes)

        resources_data = dict()
        for key in monitor_dict.keys():
            resources_data[key] = dict({
                "mem": monitor_dict[key]['mem'],
                "hdd": monitor_dict[key]['hdd'],
            })
            

        # fowarding request with a same session object
        async with self.session.post(url=url, data=data, headers=request.headers) as response:
            response_data = await response.json()
            end_time = time.time()
            monitor_dict[node_ip]['response-time'] = round(end_time - start_time, 4) # seconds
            print(response_data)
        
            return web.json_response({"sucess": 1, "response_data": response_data, "resources_data": resources_data})

    async def close(self):
        await self.session.close()


class LogHandler:
    def __init__(self, filepath):
        self.logger = logging.getLogger(__name__)
        self.handler = logging.FileHandler(filepath, mode='w')
        self.formatter = logging.Formatter("%(asctime)s - %(message)s")

        self.logger.setLevel(level=logging.INFO)
        self.handler.setFormatter(self.formatter)
        self.logger.addHandler(self.handler)

    def info(self, s):
        self.logger.info(s)
    
    def warning(self, s):
        self.logger.warning(s)
    
    def debug(self, s):
        self.logger.debug(s)

    def excpetion(self, e):
        self.logger.exception(e)


def init():
    manager_client = docker.APIClient(base_url="unix://var/run/docker.sock")

    worker_nodes = list()
    swarm_port = 2375
    container_port = 8080
    service_name = "server"

    services = manager_client.services(filters={'name': service_name})
    if services:
        service = services[0]
        tasks = manager_client.tasks(filters={'service': service["ID"], "desired-state": 'running'})
    else:
        raise RuntimeError

    if tasks:
        for task in tasks:
            container_id = task["Status"]["ContainerStatus"]["ContainerID"]
            node_id = task['NodeID']

            nodes = manager_client.nodes(filters={"id": node_id})

            node = nodes[0]

            if node:
                worker_nodes.append(
                    WorkerNode(container_url=f"http://{node['Status']['Addr']}:{container_port}", 
                               node_ip=node["Status"]["Addr"], 
                               username="duser", 
                               password="123456", 
                               state=node['Status']['State'], 
                               swarm_port=swarm_port, 
                               container_id=container_id
                ))
    else:
        raise RuntimeError
    
    return worker_nodes


def monitor_process(worker_nodes:List[WorkerNode]):
    def get_mem_usage(container_id, docker_client: docker.APIClient):
        global monitor_dict
        host = docker_client.info()['Swarm']["NodeAddr"]
        if container_id:
            container = docker_client.containers(filters={'id': container_id})
            
        if container:
            container = container[0]
            for stats in docker_client.stats(container, stream=True, decode=True):
                monitor_dict[host]['mem'] = float(round(stats['memory_stats']['usage'] / stats['memory_stats']['limit'], 3))
                logger.info(f"[{host}] mem usage: {float(round(stats['memory_stats']['usage'] / stats['memory_stats']['limit'], 3))}")
        else:
            logger.info("Container is not found")
        pass
    

    def get_hdd_usage(node:WorkerNode):
        global monitor_dict
        ssh_client = paramiko.SSHClient()
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(node.node_ip, username=node.node_username, password=node.node_password)

        command = r"df -h | grep ' /$' | awk '{print $5}'"
        stdin, stdout, stderr = ssh_client.exec_command(command=command)
        output = stdout.read().decode()
        error = stderr.read().decode()

        if error:
            ssh_client.close()
            raise RuntimeError
        else:
            # usage = int() / 100
            usage = int(output.strip(' ').replace('%', '')) / 100
            monitor_dict[node.node_ip]['hdd'] = usage
            ssh_client.close()
        return usage



    docker_worker_clients = list()

    for worker in worker_nodes:
        docker_worker_clients.append(
            docker.APIClient(base_url=f"tcp://{worker.node_ip}:{worker.swarm_port}")
        )

    logger = LogHandler("running_logs.log")
    

    # for every node with on process
    process_list:List[multiprocessing.Process] = list()
    for index in range(len(worker_nodes)):
        mem_process = multiprocessing.Process(target=get_mem_usage, args=(worker_nodes[index].container_id, docker_worker_clients[index],))
        process_list.append(mem_process)

    
    for p in process_list:
        p.start()

    try:
        while True:
            for index in range(len(worker_nodes)):
                logger.info(f"[{worker_nodes[index].node_ip}] hdd usage: {get_hdd_usage(worker_nodes[index])}")
    except Exception as e:
        logger.excpetion(e)
        for p in process_list:
            p.join()


def server_process(worker_nodes:List[WorkerNode]):
    app = web.Application()
    handler = RequestHandler(worker_nodes)
    app.router.add_post('/', handler.forwarding)
    app.on_startup.append(lambda app: handler.start())
    app.on_cleanup.append(lambda app: handler.close())

    web.run_app(app, host="10.0.10.4", port=8081)

    pass


if __name__ == "__main__":
    worker_nodes: List[WorkerNode] = init()
    process_manager = multiprocessing.Manager()
    monitor_dict = process_manager.dict()
    for node in worker_nodes:
        monitor_dict[node.node_ip] = process_manager.dict({
                "hdd": 0,
                "mem": 0,
                "response-time": 0,
            })
    # server process
    proc1 = multiprocessing.Process(target=server_process, args=(worker_nodes,))
    proc1.start()

    # monitor process
    monitor_process(worker_nodes=worker_nodes)

    proc1.join()
