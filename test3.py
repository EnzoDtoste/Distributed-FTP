from storage_node import StorageNode, accept_connections_async, request_join, find_successor, hash_function
from utils import hash_function
import os
import time
import datetime
import threading
import random

node1 = StorageNode(port=5000)
node2 = StorageNode(port=5001)
node3 = StorageNode(port=5002) 

node1.predecessor = node2.identifier, node2.host, node2.port
node2.predecessor = node1.identifier, node1.host, node1.port

node1.successor = node2.identifier, node2.host, node2.port
node2.successor = node1.identifier, node1.host, node1.port


accept_connections_async(node1)
accept_connections_async(node2)
accept_connections_async(node3)

node1.update_thread.start()
node2.update_thread.start()

time.sleep(10)

request_join(node3, node1.host , node1.port)

nodes = [StorageNode(port=i) for i in range(50003, 50010)]

for node in nodes:
    accept_connections_async(node)
    request_join(node, node3.host , node3.port)

time.sleep(90)

node1.stop_update = True
node2.stop_update = True
node3.stop_update = True

for node in nodes:
    node.stop_update = True

time.sleep(20)

print("Node 1")
print(node1.predecessor)
print(node1.successors)
print(node1.finger_table_bigger)
print(node1.finger_table_smaller)

print("------------------------")
print("Node 2")
print(node2.predecessor)
print(node2.successors)
print(node2.finger_table_bigger)
print(node2.finger_table_smaller)

print("------------------------")
print("Node 3")
print(node3.predecessor)
print(node3.successors)
print(node3.finger_table_bigger)
print(node3.finger_table_smaller)

print("----------------------------")
for node in nodes:
    print(f"Node {node.port}")
    print(node.predecessor)
    print(node.successors)
    print(node.finger_table_bigger)
    print(node.finger_table_smaller)
    print("----------------------------")