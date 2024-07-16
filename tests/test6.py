from storage_node import StorageNode, accept_connections_async, auto_request_join
import time

node4 = StorageNode(port=5003) 
node4.verbose = False

accept_connections_async(node4)
auto_request_join(node4)

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