import docker

class dockerClients:
    def __init__(self, nodes_info):
        # add url into urls_list
        self.urls = list()
        self.worker_clients = list()
        self.manager_client = list()
        for node in nodes_info:
            url = node['Status']['Addr']
            self.urls.append(url)
            if node['Spec']['Role'] == "manager":
                self.manager_client = docker.APIClient(base_url=f"tcp://{url}:2375")
            else:
                if node['Status']['State'] == "ready":
                    self.worker_clients.append(docker.APIClient(base_url=f"tcp://{url}:2375"))


def test01():
    manager_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    clients = dockerClients(manager_client.nodes())

    print(clients.worker_clients[0].df())


def test02():
    manager_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    nodes = manager_client.nodes()
    for node in nodes:
        print(node['Status'])

    tasks = manager_client.tasks()

    for task in tasks:
        print(task)


def test03():
    manager_client = docker.APIClient(base_url="unix://var/run/docker.sock")
    service_name = "server"

    services = manager_client.services(filters={'name': service_name})
    
    if services:
        service = services[0]
        tasks = manager_client.tasks(filters={'service': service["ID"]})

        

if __name__ == "__main__":
    test03()