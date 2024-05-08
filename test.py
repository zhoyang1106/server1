import numpy as np
import gurobipy as gp
from gurobipy import GRB

class RequestHandler:
    global monitor_dict
    def __init__(self, worker_nodes: List[workerNode], servers_data, servers_max_data, task_hdd_usage, task_mem_usage, task_response_time_predicted):
        self.worker_nodes = [node for node in worker_nodes if node.state == 'ready']
        self.session = None
        self.servers_data = None
        self.servers_max_data = None
        self.task_hdd_usage = None
        self.task_mem_usage = None
        self.task_response_time_predicted = None

        # Create model
        self.model = gp.Model("server_optimization")

        # # Create variables
        # b_hdd = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_hdd")
        # b_mem = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_mem")
        # num_servers = len(servers_data)
        # self.x = self.model.addVars(num_servers, vtype=GRB.BINARY, name="x")  # x = 0 or 1
        #
        # # Set objective function
        # alpha = 0.5
        # beta = 0.3
        # gamma = 0.2
        #
        # hdd_max = 0.9875
        # mem_max = 1.0
        #
        # self.model.setObjective((alpha * gp.quicksum(self.servers_data[i][0] * self.x[i] / hdd_max for i in range(num_servers))) +
        #                         (beta * gp.quicksum(self.servers_data[i][1] * self.x[i] / mem_max for i in range(num_servers))) +
        #                         gamma * gp.quicksum(self.task_response_time_predicted[i] * self.x[i] for i in range(num_servers)), GRB.MINIMIZE)
        #
        # # Add constraint
        # self.model.addConstr(gp.quicksum(self.x[i] for i in range(num_servers)) == 1, "c1")
        # for i in range(num_servers):
        #     self.model.addConstr(self.servers_data[i][0] + self.task_hdd_usage * self.x[i] <= hdd_max, f"c2_{i+1}")
        #     self.model.addConstr(self.servers_data[i][1] + self.task_mem_usage * self.x[i] <= mem_max, f"c3_{i + 1}")
        #     # b_hdd
        #     self.model.addConstr(self.servers_data[i][0] + self.task_hdd_usage * self.x[i] <= b_hdd[0], f"c4_{i + 1}")
        #     # b_mem
        #     self.model.addConstr(self.servers_data[i][1] + self.task_mem_usage * self.x[i] <= b_mem[0], f"c5_{i + 1}")
        #
        # # Optimize model
        # self.model.optimize()
        #
        # # Get result
        # self.selected_server = None
        # if self.model.status == GRB.OPTIMAL:
        #     for i in range(num_servers):
        #         if self.x[i].X > 0.5:  # if x[i] is closer to 1, then the server is selected
        #             self.selected_server = i
        #             break

    async def start(self):
        self.session = ClientSession()

    async def forwarding(self, request: Request):
        # Create variables
        b_hdd = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_hdd")
        b_mem = model.addVars(1, lb=0.0, ub=1.0, vtype=GRB.CONTINUOUS, name="b_mem")
        num_servers = len(servers_data)
        self.x = self.model.addVars(num_servers, vtype=GRB.BINARY, name="x")  # x = 0 or 1

        # Set objective function
        alpha = 0.5
        beta = 0.3
        gamma = 0.2

        hdd_max = 0.9875
        mem_max = 1.0

        self.model.setObjective(
            (alpha * gp.quicksum(self.servers_data[i][0] * self.x[i] / hdd_max for i in range(num_servers))) +
            (beta * gp.quicksum(self.servers_data[i][1] * self.x[i] / mem_max for i in range(num_servers))) +
            gamma * gp.quicksum(self.task_response_time_predicted[i] * self.x[i] for i in range(num_servers)),
            GRB.MINIMIZE)

        # Add constraint
        self.model.addConstr(gp.quicksum(self.x[i] for i in range(num_servers)) == 1, "c1")
        for i in range(num_servers):
            self.model.addConstr(self.servers_data[i][0] + self.task_hdd_usage * self.x[i] <= hdd_max, f"c2_{i + 1}")
            self.model.addConstr(self.servers_data[i][1] + self.task_mem_usage * self.x[i] <= mem_max, f"c3_{i + 1}")
            # b_hdd
            self.model.addConstr(self.servers_data[i][0] + self.task_hdd_usage * self.x[i] <= b_hdd[0], f"c4_{i + 1}")
            # b_mem
            self.model.addConstr(self.servers_data[i][1] + self.task_mem_usage * self.x[i] <= b_mem[0], f"c5_{i + 1}")

        # Optimize model
        self.model.optimize()

        # Get result
        self.selected_server = None
        if self.model.status == GRB.OPTIMAL:
            for i in range(num_servers):
                if self.x[i].X > 0.5:  # if x[i] is closer to 1, then the server is selected
                    self.selected_server = i
                    break

        if self.selected_server is not None:
            url = self.worker_nodes[self.selected_server].container_url

            try:
                async with self.session.post(url=url, data=await request.post(), headers=request.headers) as response:
                    response_data = await response.json()
                return web.json_response({"success": 1, "response_data": response_data})
            except Exception as e:
                return web.json_response({"success": 0, "error": str(e)})
        else:
            return web.json_response({"success": 0, "error": "No server available"})

    async def close(self):
        await self.session.close()
