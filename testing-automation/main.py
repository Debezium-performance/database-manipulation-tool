import os
import pprint
import time
import json

from pexpect import pxssh
from collections import OrderedDict
from dotenv import load_dotenv
import requests

load_dotenv()
REMOTE_HOSTNAME = os.getenv("REMOTE_HOSTNAME")
REMOTE_USERNAME = os.getenv("REMOTE_USERNAME")
SSH_KEY = os.getenv("SSH_KEY")
DMT_LOCATION = os.getenv("DMT_LOCATION")
OUTPUT_FILE = os.getenv("OUTPUT_FILE")
# COUNT = [100, 200, 500, 1000]
# ROWS = [20, 100, 2000]
# REPETITIONS = 5
# THREADS = [1, 5, 10, 30, 50, 100]
COUNT = [100, 200]
ROWS = [20, 100]
REPETITIONS = 5
THREADS = [1, 5, 10]
DMT_START_WAIT_TIME = 50  # In ms. Should be aproximately how long it takes for dmt to establish one jdbc connection
HEAP_SIZE = "50G"


def complete_testing(ssh_client, threads, counts, max_rows):
    total_results = OrderedDict()
    for i in threads:
        print("Starting tests with {} threads".format(i))
        pid = start_dmt(ssh_client, i)
        result = execute_tests(counts, max_rows)
        total_results["Threads: {}".format(i)] = result
        stop_dmt(ssh_client, pid)
        print("Finished tests with {} threads".format(i))
    return total_results


def execute_tests(counts, max_rows):
    total_results = OrderedDict()
    for i in counts:
        count_result = OrderedDict()
        for j in max_rows:
            print("Starting tests for count: {}, rows: {}".format(i, j))
            result = multiple_generate_requests(i, j, REPETITIONS)
            count_result["Rows: {}".format(j)] = result
            send_reset_database_request()
            print("Finished tests for count: {}, rows: {}".format(i, j))
        total_results["Count: {}".format(i)] = count_result
    return total_results


def get_ssh_client():
    s = pxssh.pxssh()
    s.login(server=REMOTE_HOSTNAME, username=REMOTE_USERNAME, ssh_key=SSH_KEY)
    s.prompt()
    print("Logged in successfully")
    return s


def send_generate_request(count, max_rows):
    print("Sending generate request")

    params = {
        'count': count,
        'maxRows': max_rows,
    }
    url = "http://{}:8080/Main/GenerateBatchLoad"
    for i in range(5):
        try:
            response = requests.post(url.format(REMOTE_HOSTNAME), params=params)
        except requests.exceptions.ConnectionError:
            if i != 5:
                print("Connection error when sending generate request, will retry")
                time.sleep(5)
                continue
            raise
        break
    # print("Results for count: {} and max rows: {}".format(count, max_rows))
    # print(response.json()["last executor started (ms)"])
    # print(response.json()["last executor finished (ms)"])
    # print(response.json()["total time (ms)"])
    return response.json()


def multiple_generate_requests(count, max_rows, repeat):
    best_response = {}
    for i in range(repeat):
        response = send_generate_request(count, max_rows)
        if len(best_response) == 0\
                or best_response["lef"] > response["lef"]:
            best_response = response
    return best_response


def send_reset_database_request():
    print("Sending reset database request")
    url = "http://{}:8080/Main/ResetDatabase"
    for i in range(5):
        try:
            response = requests.get(url.format(REMOTE_HOSTNAME))
        except requests.exceptions.ConnectionError:
            if i != 5:
                print("Connection error when sending reset database request, will retry")
                time.sleep(5)
                continue
            raise
    if response.status_code == 200:
        print("Database restarted successfully")
    else:
        print("Error when restarting database")
        raise Exception("Error when restarting database")


def start_dmt(ssh_client, threads):
    print("Starting dmt")
    set_executors_and_connections(ssh_client, threads)
    start_command = "java -jar -Xms{} {}/target/quarkus-app/quarkus-run.jar &"
    ssh_client.sendline(start_command.format(HEAP_SIZE, DMT_LOCATION))
    ssh_client.prompt()
    ssh_client.sendline("echo $!")
    ssh_client.prompt()
    __pid = ssh_client.before.decode('UTF-8').splitlines()[-1]
    time.sleep(DMT_START_WAIT_TIME/1000*threads + 1)
    print("Dmt started with pid: {}".format(__pid))
    # Waits 1 second and then milliseconds specified in DMT_START_WAIT_TIME per connection
    return __pid


def set_executor_size(ssh_client, exec_size):
    ssh_client.sendline("export EXECUTOR_SIZE={}".format(exec_size))
    ssh_client.prompt()


def set_min_jdbc_connections(ssh_client, connections):
    ssh_client.sendline("export QUARKUS_DATASOURCE_MYSQL_JDBC_MIN_SIZE={}".format(connections))
    ssh_client.prompt()


def set_max_jdbc_connections(ssh_client, connections):
    ssh_client.sendline("export QUARKUS_DATASOURCE_MYSQL_JDBC_MAX_SIZE={}".format(connections))
    ssh_client.prompt()


def set_init_jdbc_connections(ssh_client, connections):
    ssh_client.sendline("export QUARKUS_DATASOURCE_MYSQL_JDBC_INITIAL_SIZE={}".format(connections))
    ssh_client.prompt()


def set_exact_jdbc_connections(ssh_client, connections):
    set_min_jdbc_connections(ssh_client, connections)
    set_max_jdbc_connections(ssh_client, connections)
    set_init_jdbc_connections(ssh_client, connections)


def set_executors_and_connections(ssh_client, count):
    set_exact_jdbc_connections(ssh_client, count)
    set_executor_size(ssh_client, count)


def stop_dmt(ssh_client, process):
    kill_command = "kill -INT {}"
    ssh_client.sendline(kill_command.format(process))
    ssh_client.prompt()
    print("Stopped dmt")


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    client = get_ssh_client()
    results = complete_testing(client, THREADS, COUNT, ROWS)
    pprint.pprint(json.dumps(results))
    with open(OUTPUT_FILE, 'w') as f:
        print(json.dumps(results), file=f)







