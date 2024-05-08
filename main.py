class RequestHandler:
    # with out add and minuse worker nodes until kill this program
    def __init__(self, worker_nodes:List[workerNode]):
        self.worker_nodes = [node for node in worker_nodes if node.state == 'ready']
        self.session = None

        # round robin algorithm initial vairable
        # ToDo more algorithm ..
        self.current_index = 0

    async def start(self):
        self.session=lientsession()
    async def forwarding(self, request: Request):
        # round robin algorithm choose urls
        # ToDo more algorithm .
        url= self.worker_nodes[self.current_index].container_url
        self.current_index =(self.current_index + 1)% len(self.worker_nodes

        # fowarding request with a same session object
        async with self.session.post(url=url, data=await request.post(), headers=request.headers) as response:
            response_data = await response.json()
        return web.json_response({"sucess": 1, "response data": response_data})
    async def close(self):
        await self.session.close()