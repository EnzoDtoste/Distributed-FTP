from storage_node import StorageNode, accept_connections_async, auto_request_join
import time

node5 = StorageNode(port=5004) 
node5.verbose = False

accept_connections_async(node5)
auto_request_join(node5)

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