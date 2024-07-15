from storage_node import StorageNode, accept_connections_async, request_join, find_successor, hash_function
from utils import hash_function
import os
import time
import datetime
import threading
import random

node4 = StorageNode(port=5003) 
node4.verbose = False

accept_connections_async(node4)
request_join(node4, '172.17.0.2', 5000)


while True:
    time.sleep(20)

    print("-------------------------------------------------")
    print()

    while node4.updating:
        pass

    print("Node 4")
    print(node4.predecessor)
    print(node4.successors)
    print(node4.finger_table_bigger)
    print(node4.finger_table_smaller)