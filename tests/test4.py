from storage_node import StorageNode, accept_connections_async, find_successor
import os
import time
import socket

node1 = StorageNode(port=5000)
node2 = StorageNode(port=5001)

node1.verbose = False
node2.verbose = False

node1.predecessor = node2.identifier, node2.host, node2.port
node2.predecessor = node1.identifier, node1.host, node1.port

node1.successor = node2.identifier, node2.host, node2.port
node2.successor = node1.identifier, node1.host, node1.port

accept_connections_async(node1)
accept_connections_async(node2)

node1.update_thread.start()
node2.update_thread.start()

app_path = os.path.normpath('/app')

while True:
    ip, port = find_successor(app_path, node1.host, node1.port)

    app_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    app_socket.connect((ip, port))

    app_socket.sendall(f"MKD {app_path}".encode())

    response = app_socket.recv(1024).decode().strip()

    if response.startswith("220"):
        app_socket.close()
        break

while True:
    time.sleep(20)

    print("-------------------------------------------------")
    print()

    while node1.updating:
        pass

    print("Node 1")
    print(node1.predecessor)
    print(node1.successors)
    print(node1.finger_table_bigger)
    print(node1.finger_table_smaller)

    while node2.updating:
        pass

    print("------------------------")
    print("Node 2")
    print(node2.predecessor)
    print(node2.successors)
    print(node2.finger_table_bigger)
    print(node2.finger_table_smaller)