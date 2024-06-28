from storage_node import StorageNode, accept_connections_async, hash_function, setup_control_socket
import os
import time

def main():
    nodes = [StorageNode(port = i, setup_socket = False) for i in range(50, 200, 2)]
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
        node.succesors = [(nodes[i].identifier, nodes[i].host, nodes[i].port) for i in (list(range(min(n + 1, len(nodes) - 1), min(n + 1 + k, len(nodes)))) + list(range(k - len(nodes) + n)))]

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
        node.data[key] = value

        for successor_id, _, _ in node.succesors:
            successor = get_closest_up(successor_id)
            successor.data[key] = value



    root_path = os.path.normpath("/[Cine Clasico] Red Planet (2000) DUAL")
    
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

    node = get_closest_up(hash_function("127.0.0.1:128"))

    node.socket = setup_control_socket(port = 128)
    accept_connections_async(node)
    
    while True:
        input()

if __name__ == "__main__":
    main()