from storage_node import StorageNode, accept_connections_async, hash_function
import os

def main():
    nodes = [StorageNode(port = i) for i in range(50, 100, 1)]
    nodes.sort(key = lambda sn: sn.identifier)

    def get_closest_up(id):
        for node in nodes:
            if node.identifier > id:
                return node
            
        return None

    for node in nodes:
        for i in range(160):
            successor = get_closest_up(node.identifier + 2 ** i)
    
            if successor and (len(node.finger_table_bigger) == 0 or node.finger_table_bigger[-1][0] != successor.identifier):
                node.finger_table_bigger.append((successor.identifier, '127.0.0.1', successor.socket.getsockname()[1]))
            elif not successor:
                successor = get_closest_up(2 ** i)

                if len(node.finger_table_smaller) == 0 or node.finger_table_smaller[-1][0] != successor.identifier:
                    node.finger_table_smaller.append((successor.identifier, '127.0.0.1', successor.socket.getsockname()[1]))


    root_path = "/[Cine Clasico] Red Planet (2000) DUAL"
    
    for root, dirs, files in os.walk(root_path):
        for file in files:
            path = os.path.join(root, file)
            node = get_closest_up(hash_function(path))
            node.data[path] = path

            if path == '/[Cine Clasico] Red Planet (2000) DUAL\\[HD] Red Planet (2000) DUAL.png':
                print(node.socket.getsockname()[1])

    for node in nodes:
        accept_connections_async(node)
    
    while True:
        input()

if __name__ == "__main__":
    main()