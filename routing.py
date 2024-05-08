from aiohttp import web, ClientSession
from aiohttp.web_request import Request
import multiprocessing
import docker
import logging
import paramiko
import time
from typing import List



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
        self.worker_nodes = [node for node in worker_nodes if node.state == 'ready']
        self.session = None

        # round robin algorithm initial vairable
        # TODO more algorithm ...
        self.current_index = 0

    async def start(self):
        self.session = ClientSession()

    async def prev_request(self):
        global monitor_dict
        for node in self.worker_nodes:
            url = node.container_url
            host = node.node_ip
            start_time = time.time()
            data = {
                "number": 5000,
            }
            headers = {
                "tasks_type": "C",
            }
            async with self.session.post(url=url, data=data, headers=headers) as response:
                response_data = await response.json()
                end_time = time.time()

                monitor_dict[host]['hdd'] = 

                


    async def forwarding(self, request: Request):
        global monitor_dict
        # round robin algorithm choose urls
        # TODO more algorithm ...
        url = self.worker_nodes[self.current_index].container_url
        self.current_index = (self.current_index + 1) % len(self.worker_nodes)


        resources_data = dict()
        for key in monitor_dict.keys():
            resources_data[key] = dict({
                "mem": monitor_dict[key]['mem'],
                "hdd": monitor_dict[key]['hdd']
            })
        
        # fowarding request with a same session object
        async with self.session.post(url=url, data=await request.post(), headers=request.headers) as response:
            response_data = await response.json()

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
    global monitor_dict
    def get_mem_usage(container_id, docker_client: docker.APIClient):
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
    worker_nodes = init()
    process_manager = multiprocessing.Manager()
    monitor_dict = process_manager.dict({
        "10.0.10.5": process_manager.dict({
            "hdd": 0,
            "mem": 0
        }),
        "10.0.10.6": process_manager.dict({
            "hdd": 0,
            "mem": 0
        }),
        "10.0.10.7": process_manager.dict({
            "hdd": 0,
            "mem": 0
        }),
    })
    # server process
    proc1 = multiprocessing.Process(target=server_process, args=(worker_nodes,))
    proc1.start()

    # monitor process
    monitor_process(worker_nodes=worker_nodes)

    proc1.join()
    pass