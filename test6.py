from storage_node import StorageNode, accept_connections_async, request_join, find_successor, hash_function
from utils import hash_function
import os
import time
import datetime
import threading
import random

node4 = StorageNode(port=5003) 
accept_connections_async(node4)
request_join(node4, '172.17.0.2', 5000)



time.sleep(20)
node4.stop_update = True

time.sleep(6)
print("Node 4")
print(node4.predecessor)
print(node4.successors)
print(node4.finger_table_bigger)
print(node4.finger_table_smaller)