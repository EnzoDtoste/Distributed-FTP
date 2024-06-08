from storage_node import StorageNode, accept_connections_async

def main():
    node1 = StorageNode()
    node2 = StorageNode(port=56)

    node1.predecessor = ('127.0.0.1', 56)
    node2.successor = ('127.0.0.1', 50)

    node2.data['/1_5483277136397598732.mp4'] = '/1_5483277136397598732.mp4'

    accept_connections_async(node1)
    accept_connections_async(node2)

    while True:
        input()

if __name__ == "__main__":
    main()