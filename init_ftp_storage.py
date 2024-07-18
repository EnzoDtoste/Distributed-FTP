
from storage_node import StorageNode, accept_connections_async
import os
import datetime

node = StorageNode()
node.verbose = False

app_path = os.path.normpath('/app')
node.data[app_path] = {}, 0

accept_connections_async(node)
node.update_thread.start()

while True:
    input()

    print("-------------------------------------------------")
    print()

    while node.updating:
        pass

    print(f"Node {node.port}")
    print(node.predecessor)
    print(node.successors)
    print(node.finger_table_bigger)
    print(node.finger_table_smaller)