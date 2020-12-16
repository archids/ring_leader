### Distributed Systems Lab 03, 2020 - Ring Leader-Election Algorithm ###
### Leader Election Script ###
### By arc.hids ###

import socket
import threading
from queue import Queue
import time
import pickle
from prettytable import PrettyTable
#import logging
# logging.basicConfig(level=logging.DEBUG)

# ASCII color codes
R = "\033[1;31m"        # Red on Black background
G = "\033[1;32m"        # Green on Black background
Y = "\033[1;33m"        # Yellow on Black background
C = "\033[1;36m"        # Cyan on Black background
N = "\033[0m"           # Reset
WonR = "\033[1;37;41m"  # White on Red background
WonY = "\033[1;33;41m"  # White on Red background


# List of all machines taking part in the snapshotting
MEMBERSHIP_LIST = [
    {"Name": "P5", "IP": "10.128.0.13", "Port": 2025},
    {"Name": "P3", "IP": "10.128.0.11", "Port": 2023},
    {"Name": "P4", "IP": "10.128.0.12", "Port": 2024},
    {"Name": "P1", "IP": "10.128.0.9", "Port": 2021},
    {"Name": "P6", "IP": "10.128.0.14", "Port": 2026},
    {"Name": "P2", "IP": "10.128.0.10", "Port": 2022}
]

# Local machine information
LOCAL_IP = socket.gethostbyname(socket.gethostname())
LOCAL_ID = [MEMBERSHIP_LIST[idx]["Name"] for idx, process in enumerate(
    MEMBERSHIP_LIST) if process["IP"] == LOCAL_IP][0]
LOCAL_PORT = [MEMBERSHIP_LIST[idx]["Port"] for idx, process in enumerate(
    MEMBERSHIP_LIST) if process["IP"] == LOCAL_IP][0]

# Initializing index of the current host process and the next process in the ring
default_index = [idx for idx, process in enumerate(
    MEMBERSHIP_LIST) if process["Name"] == LOCAL_ID][0]
current_index = default_index
NEXT_PROCESS = MEMBERSHIP_LIST[current_index + 1] if current_index < len(
    MEMBERSHIP_LIST) - 1 else MEMBERSHIP_LIST[0]

# Leader Lease information
LEASH_INTERVAL = 10
LEASH_TIMEOUT = 30
# Set a buffer size for send and receive data
BUFFER = 512

# Record self as leader or not.  Default is not.
# current_leader = {"Name": "P3", "Value": 3}  # Test Case - Preselected Leader
current_leader = {}  # Test Case - No Leader on startup
# Test Case - Preselected Leader
# iam_leader = True if current_leader["Name"] == LOCAL_ID else False
iam_leader = False  # Test Case - No Leader on startup

# Variables to hold Lists of messages for electing leader, circulating results # and lease renewals
relayed_message = []
default_relay_counter = {"Lease": 0, "Vote": 0, "Elected": 0}
relay_counter = default_relay_counter
last_lease_time = time.time()

# If the queue can be processed or not
halt_process_data = False


# Utility function to handle repetitive pre-flight message prep
def prep_message():
    global relayed_message
    global current_index
    global halt_process_data

    halt_process_data = False
    current_index = default_index
    relayed_message.append({"Name": LOCAL_ID, "Value": int(LOCAL_ID[-1])})
    message = pickle.dumps(relayed_message)
    return message


# Utility function to handle repetitive pre-voting prep
def prep_voting():
    global halt_process_data
    # global relay_counter
    global relayed_message
    global iam_leader
    global last_lease_time
    global current_leader

    # halt_process_data = True

    current_leader.clear()
    relayed_message.clear()
    iam_leader = False
    last_lease_time = None
    relayed_message.append({"Header": "Vote"})


# Function to send out new Lease Renewal Message to the next process.
def renew_lease():
    message = prep_message()
    handle_outgoing(message, "Lease Renewal")


# Function to send out new Leader Voting Call to the next process
def leader_vote_process():
    message = prep_message()
    handle_outgoing(message, "Voting")


# Function send out new Leader Elected Call to the next process
def leader_elect_process():
    message = prep_message()
    handle_outgoing(message, "Coordinator Message")


# Function to handle outgoing markers or messages
def handle_outgoing(message, purpose):

    global current_index
    global NEXT_PROCESS
    attempts = 2

    while attempts:
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as outgoing_conn:
                print(
                    f"[{LOCAL_ID}] Connecting to {NEXT_PROCESS['Name']} on Port {NEXT_PROCESS['Port']}")
                outgoing_conn.settimeout(3)
                outgoing_conn.connect(
                    (NEXT_PROCESS['IP'], NEXT_PROCESS['Port']))
                outgoing_conn.settimeout(None)

                print(
                    f"[{LOCAL_ID}] Sending {purpose} to {NEXT_PROCESS['Name']}")

                status_report(purpose)

                outgoing_conn.sendall(message)
            attempts = 2
            #current_index = default_index
            break

        except ConnectionError:
            print(
                f"[{LOCAL_ID}] Retrying connection to {NEXT_PROCESS['Name']} on Port {NEXT_PROCESS['Port']}")
            attempts -= 1

    else:
        print(
            f"[{LOCAL_ID}] Sending {purpose} to {NEXT_PROCESS['Name']} failed.")
        print(f"[{LOCAL_ID}] Will move on to the next process.\n")

        attempts = 2
        if current_index < len(MEMBERSHIP_LIST) - 1:
            current_index += 1
        else:
            current_index = 0

        NEXT_PROCESS = MEMBERSHIP_LIST[current_index + 1] if current_index < len(
            MEMBERSHIP_LIST) - 1 else MEMBERSHIP_LIST[0]

        handle_outgoing(message, purpose)

    outgoing_conn.close()


# Function to handle initiate sockets for listening for incoming connections
def handle_incoming(task_queue):

    # Initiate Server socket
    incoming_conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    incoming_conn.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    incoming_conn.bind((LOCAL_IP, LOCAL_PORT))
    incoming_conn.listen()
    print(f"[{LOCAL_ID}] LISTENING ON PORT {LOCAL_PORT}\n")

    # Collect all incoming connections
    incoming_threads = []
    while True:
        try:
            client_socket, address = incoming_conn.accept()
            newthread = threading.Thread(
                target=handle_clients, args=(client_socket, address, task_queue))
            newthread.daemon = True
            newthread.start()
            # incoming_threads.append(newthread)

        except:
            print(f"[{LOCAL_ID}] Error accepting connection")

    # for connection in incoming_threads:
    #     connection.join()

    incoming_conn.close()


# Handle incoming clients
def handle_clients(client_socket, address, task_queue):

    client_name = [MEMBERSHIP_LIST[idx]["Name"] for idx, process in enumerate(
        MEMBERSHIP_LIST) if process["IP"] == address[0]][0]

    if iam_leader:
        print(f"[{LOCAL_ID}] Lease looped back successfully\n")
    else:
        print(f"[{LOCAL_ID}] {client_name} connected")

    try:
        data = client_socket.recv(BUFFER)
    except EOFError as msg:
        print(f"[{LOCAL_ID}] Error receiving data")

    received_time = time.time()
    received_data = pickle.loads(data)
    task_queue.put([received_data, received_time])


# Process received data, read headers and act accordingly
def process_data(task_queue):

    global iam_leader
    global relayed_message
    global last_lease_time
    global halt_process_data
    global current_leader

    incoming_queue = task_queue.get()
    received_data = incoming_queue[0]
    received_time = incoming_queue[1]
    client_name = received_data[-1]["Name"]

    try:
        # Case-1 - Lease Renewal Messages
        if received_data[0]['Header'] == "Lease":
            print(
                f"[{LOCAL_ID}] Received new lease from {client_name}\n")

            # If the received message was not initiated by self
            if received_data[1]['Name'] != LOCAL_ID:
                last_lease_time = received_time
                relayed_message.clear()
                relayed_message = received_data
                renew_lease()

        elif received_data[0]['Header'] == "Vote":

            print(
                f"[{LOCAL_ID}] Received new leader voting message from {client_name}\n")

            # If the received message was not initiated by self
            if received_data[1]['Name'] != LOCAL_ID:
                #last_lease_time = None
                relayed_message.clear()
                relayed_message = received_data
                leader_vote_process()

            else:
                print(WonR +
                      f"\n[{LOCAL_ID}] LEADER ELECTED!!!" + N)

                relayed_message.clear()
                elected_leader = max(
                    received_data[1:], key=lambda x: x['Value'])
                print(C + f"[{LOCAL_ID}] NEW LEADER: {elected_leader}" + N)
                current_leader.update(elected_leader)
                iam_leader = True if current_leader['Name'] == LOCAL_ID else False
                relayed_message.append(
                    {"Header": "Elected", "Leader": elected_leader})
                leader_elect_process()

        # Case-3 - Leader Elected Messages
        elif received_data[0]['Header'] == "Elected":

            print(
                f"[{LOCAL_ID}] Received new leader elected message from {client_name}\n")
            # If the received message was not initiated by self
            if received_data[1]['Name'] != LOCAL_ID:
                #last_lease_time = None
                current_leader = received_data[0]['Leader']
                iam_leader = True if current_leader['Name'] == LOCAL_ID else False
                relayed_message = received_data
                leader_elect_process()

            else:
                if iam_leader:
                    relayed_message.clear()
                    relayed_message.append({"Header": "Lease"})
                    renew_lease()

    except Exception as msg:
        print(msg)


# Utility function to check lease timeouts
def check_timeout():
    while not iam_leader:
        time.sleep(5)
        # if last_lease_time is not None:
        if last_lease_time:
            elapsed_time = int(time.time()) - int(last_lease_time)
            if elapsed_time > LEASH_TIMEOUT:
                print(WonY +
                      f"\n[{LOCAL_ID}] No lease received in {LEASH_TIMEOUT}s. Initiating voting process\n" + N)
                prep_voting()
                leader_vote_process()


# Print some pretty tables of running status
def status_report(event_name):
    table = PrettyTable()
    processes = [process['Name'] for process in relayed_message[1:]]
    values = [process['Value'] for process in relayed_message[1:]]

    if event_name == "Lease Renewal":
        table.title = G + "LEASE" + N
        table.field_names = processes
        table.add_row(values)

    elif event_name == "Voting":
        table.title = Y + "VOTE" + N
        table.field_names = processes
        table.add_row(values)

    elif event_name == "Coordinator Message":
        table.title = C + "LEADER" + N
        table.field_names = processes
        table.add_row(values)

    print(table)
    print()


# Main function
def main():

    global relayed_message

    lock = threading.Lock()
    task_queue = Queue()

    print(f"CREATING PROCESS: {LOCAL_ID}")

    # hold list of threads for joining
    running_threads = []

    # Spawn a worker thread for server functions to handle all
    # incoming data
    incoming_thread = threading.Thread(
        target=handle_incoming, args=(task_queue,))
    incoming_thread.daemon = True
    incoming_thread.start()
    running_threads.append(incoming_thread)

    # Spawn a worker thread to check leader liveness.  Check current time against
    # last received lease
    timeout_thread = threading.Thread(target=check_timeout)
    timeout_thread.daemon = True
    timeout_thread.start()
    running_threads.append(timeout_thread)

    # Check to see if self is leader and if so, send out lease renewal
    # messages at regular interval

    while True:
        if iam_leader:
            print(WonR + f"\n[{LOCAL_ID}] CURRENT LEADER\n" + N)
            time.sleep(LEASH_INTERVAL)
            relayed_message.clear()
            relayed_message.append({"Header": "Lease"})
            renew_lease()
    # check for last received lease renewal message and if delayed
    # initiate leader voting process
        else:
            time.sleep(5)
            if not halt_process_data or not task_queue.empty():
                process_data(task_queue)

    for thread in running_threads:
        thread.join()


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print(R + f"\n[{LOCAL_ID}] SHUTTING DOWN PROCESS" + N)
