import requests
import time
import jsonlines
import random
import datetime
import concurrent.futures

def request_sample():
    start_time = time.time()
    # single request sample
    # don't change headers and data
    task_types = ['C', 'M', 'H']
    task_type = random.choice(task_types)
    headers = {
        "task_type": task_type,
    }
    data = None
    if task_type == "C":
        data = {
            "number": random.randint(10000, 50000)
        }
    elif task_type == 'M':
        # KBytes
        data = {
            "size": random.randint(200, 500) * pow(2, 20)
        }
    elif task_type == "H":
        # KBytes
        data = {
            "size": random.randint(2, 5) * pow(2, 20)
        }
    addr = "10.0.10.4"
    port = 8081

    response = requests.post(url=f"http://{addr}:{port}", data=data, headers=headers)

    end_time = time.time()
    if response.status_code:
        response_json = response.json()
        response_json['request-status'] = data
        response_json['task-type'] = task_type
        response_json["start-timestamp"] = start_time
        response_json["end-timestamp"] = end_time
        response_json['response-time'] = end_time - start_time
        star_time_obj = datetime.datetime.fromtimestamp(start_time)
        end_time_obj = datetime.datetime.fromtimestamp(end_time)
        response_json['request-start-asctime'] = star_time_obj.strftime("%Y-%m-%d %H:%M:%S") + ',%03d' % (star_time_obj.microsecond // 1000)
        response_json['request-end-asctime'] = end_time_obj.strftime("%Y-%m-%d %H:%M:%S") + ',%03d' % (end_time_obj.microsecond // 1000)
        return response_json


def request_process(n: int):
    processes = []
    results = []
    with concurrent.futures.ProcessPoolExecutor(max_workers=20) as executor:
        for i in range(n):
            processes.append(executor.submit(request_sample))
        
        for proc in concurrent.futures.as_completed(processes):
            results.append(proc.result())
    return results

if __name__ == "__main__":
    result_file_path = "data_json.jsonl"

    print("Request function running...")

    requests_sum = 100

    results_data = request_process(requests_sum)

    with jsonlines.open(result_file_path, 'w') as f:
        for res in results_data:
            f.write(res)

    print("Fetched all results!")
    