from storage_node import StorageNode, accept_connections_async, request_join
from utils import hash_function
import os
import time
import datetime

def main():
    nodes = [StorageNode(port = i) for i in range(50, 56, 2)]
    nodes.sort(key = lambda sn: sn.identifier)

    def get_closest_up(id):
        for node in nodes:
            if node.identifier >= id:
                return node
            
        return None


    ##########   Build Finger Table   ###########

    k = 3

    for n, node in enumerate(nodes):
        node.predecessor = nodes[n - 1].identifier, nodes[n - 1].host, nodes[n - 1].port
        node.successors = [(nodes[i].identifier, nodes[i].host, nodes[i].port) for i in ((list(range(n + 1, min(n + 1 + k, len(nodes)))) if n + 1 < len(nodes) else []) + list(range(k - len(nodes) + n + 1)))]
        node.successor = node.successors[0]

        for i in range(160):
            successor = get_closest_up(node.identifier + 2 ** i)
    
            if successor and (len(node.finger_table_bigger) == 0 or node.finger_table_bigger[-1][0] != successor.identifier):
                node.finger_table_bigger.append((successor.identifier, successor.host, successor.port))
            elif not successor:
                for j in range(160 - i):
                    successor = get_closest_up(2 ** j)
                    
                    if successor.identifier >= node.identifier:
                        break

                    if len(node.finger_table_smaller) == 0 or node.finger_table_smaller[-1][0] != successor.identifier:
                        node.finger_table_smaller.append((successor.identifier, successor.host, successor.port))

                break



    ##########   Read & Replicate Data   ###########

    def replicate(node : StorageNode, key, value):
        time = datetime.datetime.now()
        node.data[key] = (value, time)

        for successor_id, _, _ in node.successors:
            successor = get_closest_up(successor_id)
            successor.data[key] = (value, time)

    
    try:
        root_path = os.path.normpath("/[Cine Clasico] Red Planet (2000) DUAL")
        entries = os.listdir(root_path)
    except:
        root_path = os.path.normpath("/app")
        entries = os.listdir(root_path)

    inner_dirs = {}
    for entry in entries:
        try:
            filepath = os.path.normpath(os.path.join(root_path, entry))
            stats = os.stat(filepath)
            file_info = {
                'permissions': 'drwxr-xr-x' if os.path.isdir(filepath) else '-rw-r--r--',
                'links': stats.st_nlink,
                'owner': stats.st_uid,
                'group': stats.st_gid,
                'size': stats.st_size,
                'mtime': time.strftime("%b %d %H:%M", time.gmtime(stats.st_mtime)),
                'name': entry
            }
            
            inner_dirs[filepath] = "{permissions} {links} {owner} {group} {size} {mtime} {name}".format(**file_info)
        except:
            print("Could not read: " + str(entry))

    node = get_closest_up(hash_function(root_path))

    if not node:
        node = nodes[0]

    replicate(node, root_path, inner_dirs)
    
    for root, dirs, files in os.walk(root_path):
        for file in files:
            path = os.path.normpath(os.path.join(root, file))
            node = get_closest_up(hash_function(path))

            if not node:
                node = nodes[0]

            replicate(node, path, path)

        for dir in dirs:
            path = os.path.normpath(os.path.join(root, dir))
            entries = os.listdir(path)
        
            inner_dirs = {}
            for entry in entries:
                try:
                    filepath = os.path.normpath(os.path.join(path, entry))
                    stats = os.stat(filepath)
                    file_info = {
                        'permissions': 'drwxr-xr-x' if os.path.isdir(filepath) else '-rw-r--r--',
                        'links': stats.st_nlink,
                        'owner': stats.st_uid,
                        'group': stats.st_gid,
                        'size': stats.st_size,
                        'mtime': time.strftime("%b %d %H:%M", time.gmtime(stats.st_mtime)),
                        'name': entry
                    }
                    
                    inner_dirs[filepath] = "{permissions} {links} {owner} {group} {size} {mtime} {name}".format(**file_info)
                except:
                    print("Could not read: " + str(entry))

            node = get_closest_up(hash_function(path))
            
            if not node:
                node = nodes[0]

            replicate(node, path, inner_dirs)



    ##########   Accept Connections  ###########

    for node in nodes:
        if node.port != -1:
            accept_connections_async(node)
        else:
            node.socket.close()
    
    for node in nodes:
        node.update_thread.start()
    #     print('----------------')
    #     print(node.finger_table_bigger)
    #     print(node.finger_table_smaller)
    #     print(node.successors)
    #     print(node.successor)
    #     print('-----------------')

    # for node in nodes[-10:]:
    #     print('--------------')
    #     print(node.host + ":" + str(node.port))
    #     print(node.identifier)
    #     print('---------------')

    # new_node = StorageNode(port = 205)

    while True:
        input()
        #request_join(new_node)

if __name__ == "__main__":
    main()