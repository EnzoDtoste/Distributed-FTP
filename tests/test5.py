from storage_node import StorageNode, accept_connections_async, auto_request_join
import time

node3 = StorageNode(port=5002) 
node3.verbose = False

accept_connections_async(node3)
auto_request_join(node3)

while True:
    time.sleep(20)

    print("-------------------------------------------------")
    print()

    while node3.updating:
        pass

    print("Node 3")
    print(node3.predecessor)
    print(node3.successors)
    print(node3.finger_table_bigger)
    print(node3.finger_table_smaller)