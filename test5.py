from storage_node import StorageNode, accept_connections_async, request_join, find_successor, hash_function
from utils import hash_function
import os
import time
import datetime
import threading
import random

node3 = StorageNode(port=5002) 
accept_connections_async(node3)
request_join(node3, '172.17.0.2', 5000)

node3.update_thread.start()

time.sleep(60)
node3.stop_update = True
print("Node 3")
print(node3.predecessor)
print(node3.successors)
print(node3.finger_table_bigger)
print(node3.finger_table_smaller)