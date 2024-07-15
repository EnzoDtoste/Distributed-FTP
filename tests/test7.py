from storage_node import StorageNode, accept_connections_async, request_join, find_successor, hash_function
from utils import hash_function
import os
import time
import datetime
import threading
import random

node5 = StorageNode(port=5004) 
node5.verbose = False

accept_connections_async(node5)
request_join(node5, '172.17.0.2', 5000)


while True:
    time.sleep(20)

    print("-------------------------------------------------")
    print()

    while node5.updating:
        pass

    print("Node 5")
    print(node5.predecessor)
    print(node5.successors)
    print(node5.finger_table_bigger)
    print(node5.finger_table_smaller)