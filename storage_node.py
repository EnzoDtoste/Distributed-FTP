import threading
import socket
import os
import hashlib
import json
from datetime import datetime

def hash_function(key):
    """ Retorna un hash entero de 160 bits del input key """
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)


def setup_control_socket(host='127.0.0.1', port=50):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")
    return server_socket

def getId(host, port):
    return host + ':' + str(port)

class StorageNode:
    def __init__(self, host='127.0.0.1', port=50, setup_socket=True):
        self.host = host
        self.port = port
        self.identifier = hash_function(getId(host, port))
        self.socket = setup_control_socket(host, port) if setup_socket else None
        self.data = {}
        self.finger_table_bigger = []
        self.finger_table_smaller = []
        self.predecessor = None
        self.succesors = []
        self.succesor = None
        self.k_succesors = 3
        self.reading_finger_table = False
        self.updating = False

def get_table_successor(finger_table, id):
    for i in range(len(finger_table)):
        if finger_table[i][0] > id:
            index = max(i - 1, 0)
            return (finger_table[index][1], finger_table[index][2])

    index = len(finger_table) - 1
    return (finger_table[index][1], finger_table[index][2])

def find_table_successor(storageNode : StorageNode, id):
    while storageNode.updating:
        pass

    storageNode.reading_finger_table = True

    if len(storageNode.finger_table_bigger) == 0 and len(storageNode.finger_table_smaller) == 0:
        result = storageNode.succesor[1], storageNode.succesor[2]

    elif len(storageNode.finger_table_bigger) > 0 and id > storageNode.identifier:
        result = get_table_successor(storageNode.finger_table_bigger, id)
    
    elif len(storageNode.finger_table_smaller) > 0:
        result = get_table_successor(storageNode.finger_table_smaller, id)
    
    else:
        index = len(storageNode.finger_table_bigger) - 1
        result = (storageNode.finger_table_bigger[index][1], storageNode.finger_table_bigger[index][2])

    storageNode.reading_finger_table = False
    return result


def get_k_successors(node : StorageNode):
    k = node.k_succesors
    result = []

    for _, ip, port in node.succesors:
        if k > 0:
            result.append(f"{ip}:{port}") 
            k -= 1
        else:
            break

    return result

def handle_gs_command(storageNode : StorageNode, id_key, client_socket):
    if (storageNode.predecessor[0] > storageNode.identifier and (id_key <= storageNode.identifier or id_key > storageNode.predecessor[0])) or (storageNode.predecessor[0] < id_key and id_key <= storageNode.identifier):
        client_socket.send(f"220".encode())
    else:
        ip, port = find_table_successor(storageNode, id_key)
        client_socket.send(f"550 {ip}:{port}".encode())

def find_successor(id_key, node_ip='127.0.0.1', node_port=50, hash = False):
    if not hash:
        id_key = hash_function(id_key)
    
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.connect((node_ip, node_port))
    
    print(f"Connected to {node_ip}:{node_port}")
    
    node_socket.sendall(f"GS {id_key}".encode())

    response = node_socket.recv(1024).decode().strip()

    if response.startswith("220"):
        node_socket.close()
        return node_ip, node_port
    
    ip, port = response.split(" ")[1].split(":")
    node_socket.close()
    return find_successor(id_key, ip, int(port), True)


def check_successors(storageNode : StorageNode):
    try:
        new_successors = [storageNode.succesor]

        while len(new_successors) < storageNode.k_succesors:
            ip, port = find_successor(new_successors[-1][0] + 1, new_successors[-1][1], new_successors[-1][2], True)
            new_successors.append((hash_function(getId(ip, port)), ip, port))

        for new_succesor in set(new_successors) - set(storageNode.succesors):
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_socket.connect((new_succesor[1], new_succesor[2]))
            
            print(f"Replicating in {new_succesor[1]}:{new_succesor[2]}")
            
            node_socket.sendall(f"RP".encode())

            response = node_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                for key, value in storageNode.data.items():
                    if isinstance(value[0], dict):
                        
                        data = f"{json.dumps(value)}".encode()
                        node_socket.sendall(f"Folder {value[1].strftime('%Y%m%d%H%M%S')} {len(data)} {key}".encode())

                        response = node_socket.recv(1024).decode().strip()

                        if response.startswith("220"):
                            node_socket.sendall(data)

                            response = node_socket.recv(1024).decode().strip()

                            if (not response) or not response.startswith("220"):
                                raise Exception(f"Something went wrong replicating {new_succesor[1]}:{new_succesor[2]} {key}")

                            print(f"Transfer complete {key}")

                    else:
                        node_socket.sendall(f"File {value[1].strftime('%Y%m%d%H%M%S')} {os.stat(value[0]).st_size} {key}".encode())

                        response = node_socket.recv(1024).decode().strip()

                        if response.startswith("220"):
                            with open(value[0], "rb") as file:
                                data = file.read(4096)
                                count = 0
                                while data:
                                    node_socket.sendall(data)
                                    count += len(data)
                                    data = file.read(4096)

                            response = node_socket.recv(1024).decode().strip()

                            if (not response) or not response.startswith("220"):
                                raise Exception(f"Something went wrong replicating {new_succesor[1]}:{new_succesor[2]} {key}")

                            print(f"Transfer complete {key}")

                node_socket.send(b"226 Transfer complete.\r\n")
            
            node_socket.close()

        storageNode.updating = True

        while storageNode.reading_finger_table:
            pass

        storageNode.succesors = new_successors

        storageNode.updating = False

    except Exception as e:
        storageNode.updating = False
        print(f"Error: {e}")

def handle_rp_command(storageNode : StorageNode, client_socket):
    try:
        client_socket.send(f"220".encode())

        while(True):
            response = client_socket.recv(1024).decode().strip()

            if response.startswith("Folder"):
                args = response[7:].strip().split(" ")
                
                version = datetime.strptime(args[0], '%Y%m%d%H%M%S')
                size = args[1]
                path = " ".join(args[2:])

                if path not in storageNode.data or storageNode.data[path][1] < version:
                    client_socket.send(f"220".encode())
                        
                    data_json = ""

                    count = 0
                    while count < size:
                        data = client_socket.recv(4096)
                        data_json += data.decode()
                        count += len(data)

                    storageNode.data[path] = json.loads(data_json), version
                    client_socket.send(f"220".encode())
                    print(f"Transfer complete {path}")

                else:
                    client_socket.send(f"403".encode())

            elif response.startswith("File"):
                args = response[5:].strip().split(" ")
                
                version = datetime.strptime(args[0], '%Y%m%d%H%M%S')
                size = args[1]
                key = " ".join(args[2:])

                if key not in storageNode.data or storageNode.data[key][1] < version:
                    os.makedirs(os.path.dirname(key), exist_ok=True)

                    with open(key, "wb") as file: # binary mode
                        client_socket.send(f"220".encode())

                        count = 0
                        while count < size:
                            data = client_socket.recv(4096)
                            file.write(data)
                            count += len(data)
                        
                        storageNode.data[key] = key, version
                        client_socket.send(f"220".encode())
                        print(f"Transfer complete {key}")

                else:
                    client_socket.send(f"403".encode())

            elif response.startswith("226"):
                break

    except Exception as e:
        print(f"Error: {e}")

def update_finger_table(storageNode : StorageNode):
    try:
        new_finger_table_bigger = []
        new_finger_table_smaller = []
        
        request_node_ip, request_node_port = storageNode.succesors[0][1], storageNode.succesors[0][2]

        for i in range(160):
            ip, port = find_successor(storageNode.identifier + 2 ** i, request_node_ip, request_node_port, True)
            id = hash_function(getId(ip, port))

            request_node_ip, request_node_port = ip, port

            if id > storageNode.identifier and (len(new_finger_table_bigger) == 0 or new_finger_table_bigger[-1][0] != id):
                new_finger_table_bigger.append((id, ip, port))
            
            elif id < storageNode.identifier:
                for j in range(160 - i):
                    ip, port = find_successor(2 ** j, request_node_ip, request_node_port, True)
                    id = getId(ip, port)

                    request_node_ip, request_node_port = ip, port
                    
                    if id >= storageNode.identifier:
                        break

                    if len(new_finger_table_smaller) == 0 or new_finger_table_smaller[-1][0] != id:
                        new_finger_table_smaller.append((id, ip, port))

                break
            
            elif id == storageNode.identifier:
                break
                

        storageNode.updating = True

        while storageNode.reading_finger_table:
            pass

        storageNode.finger_table_bigger = new_finger_table_bigger
        storageNode.finger_table_smaller = new_finger_table_smaller

        storageNode.updating = False

    except Exception as e:
        storageNode.updating = False
        print(f"Error: {e}")

def request_join(storageNode : StorageNode, node_ip='127.0.0.1', node_port=50):
    try:
        node_ip, node_port = find_successor(getId(storageNode.host, storageNode.port), node_ip, node_port)

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"JOIN {storageNode.host}:{storageNode.port}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            storageNode.succesor = hash_function(getId(node_ip, node_port)), node_ip, node_port

            predecessor_ip, predecessor_port = response[4:].split(":")
            predecessor_port = int(predecessor_port)

            storageNode.predecessor = hash_function(getId(predecessor_ip, predecessor_port)), predecessor_ip, predecessor_port

            node_socket.send(f"220".encode())

            while(True):
                response = node_socket.recv(1024).decode().strip()

                if response.startswith("Folder"):
                    args = response[7:].strip().split(" ")
                    
                    version = datetime.strptime(args[0], '%Y%m%d%H%M%S')
                    size = args[1]
                    path = " ".join(args[2:])

                    if path not in storageNode.data or storageNode.data[path][1] < version:
                        node_socket.send(f"220".encode())
                            
                        data_json = ""

                        count = 0
                        while count < size:
                            data = node_socket.recv(4096)
                            data_json += data.decode()
                            count += len(data)

                        storageNode.data[path] = json.loads(data_json), version
                        node_socket.send(f"220".encode())
                        print(f"Transfer complete {path}")

                    else:
                        node_socket.send(f"403".encode())

                elif response.startswith("File"):
                    args = response[5:].strip().split(" ")
                    
                    version = datetime.strptime(args[0], '%Y%m%d%H%M%S')
                    size = args[1]
                    key = " ".join(args[2:])

                    if key not in storageNode.data or storageNode.data[key][1] < version:
                        os.makedirs(os.path.dirname(key), exist_ok=True)

                        with open(key, "wb") as file: # binary mode
                            node_socket.send(f"220".encode())

                            count = 0
                            while count < size:
                                data = node_socket.recv(4096)
                                file.write(data)
                                count += len(data)
                            
                            storageNode.data[key] = key, version
                            node_socket.send(f"220".encode())
                            print(f"Transfer complete {key}")

                    else:
                        node_socket.send(f"403".encode())

                elif response.startswith("226"):
                    break

            node_socket.close()

            predecessor_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            predecessor_socket.connect((predecessor_ip, predecessor_port))

            predecessor_socket.sendall(f"SS {storageNode.host}:{storageNode.port}".encode())
            print(f"Notified Predecessor {predecessor_ip}:{predecessor_port}")

            check_successors(storageNode)
            update_finger_table(storageNode)

            print("---------------")
            print(storageNode.succesors)
            print("---------------")
            print(storageNode.finger_table_bigger)
            print(storageNode.finger_table_smaller)
            print("---------------")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            request_join(storageNode, ip, int(port))
            return

    except Exception as e:
        print(f"Error: {e}")

def handle_join_command(storageNode : StorageNode, ip, port, client_socket):
    join_node_id = hash_function(getId(ip, port))

    try:
        client_socket.send(f"220 {storageNode.predecessor[1]}:{storageNode.predecessor[2]}".encode())

        response = client_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            for key, value in storageNode.data.items():
                id_key = hash_function(key)

                if id_key > storageNode.predecessor[0] and id_key <= join_node_id:
                    if isinstance(value[0], dict):
                        
                        data = f"{json.dumps(value)}".encode()
                        client_socket.sendall(f"Folder {value[1].strftime('%Y%m%d%H%M%S')} {len(data)} {key}".encode())

                        response = client_socket.recv(1024).decode().strip()

                        if response.startswith("220"):
                            client_socket.sendall(data)

                            response = client_socket.recv(1024).decode().strip()

                            if (not response) or not response.startswith("220"):
                                raise Exception(f"Something went wrong sending data {ip}:{port} {key}")

                            print(f"Transfer complete {key}")

                    else:
                        client_socket.sendall(f"File {value[1].strftime('%Y%m%d%H%M%S')} {os.stat(value[0]).st_size} {key}".encode())

                        response = client_socket.recv(1024).decode().strip()

                        if response.startswith("220"):
                            with open(value[0], "rb") as file:
                                data = file.read(4096)
                                count = 0
                                while data:
                                    client_socket.sendall(data)
                                    count += len(data)
                                    data = file.read(4096)

                            response = client_socket.recv(1024).decode().strip()

                            if (not response) or not response.startswith("220"):
                                raise Exception(f"Something went wrong replicating {ip}:{port} {key}")

                            print(f"Transfer complete {key}")

            client_socket.send(b"226 Transfer complete.\r\n")

            storageNode.predecessor = join_node_id, ip, port

    except Exception as e:
        print(f"Error: {e}")
        
def handle_ss_command(storageNode : StorageNode, ip, port, client_socket):
    new_successor_id = hash_function(getId(ip, port))
    storageNode.succesor = new_successor_id, ip, port

def handle_list_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        dirs = storageNode.data[key][0]

        try:
            client_socket.send(f"220".encode())
            response = client_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                client_socket.sendall('\n'.join([info for info in dirs.values()]).encode('utf-8'))
                print("Transfer complete")

        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    

def handle_mkd_command(storageNode : StorageNode, key, client_socket):
    if key not in storageNode.data:
        time = datetime.now()
        storageNode.data[key] = {}, time

        try:
            client_socket.send(f"220".encode())

        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"403 Already exists".encode())
    

def handle_rmd_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        dirs = storageNode.data.pop(key)[0]

        try:
            folders = [folder for folder, info in dirs.items() if info.startswith('drwxr-xr-x')]
            files = [file for file, info in dirs.items() if info.startswith('-rw-r--r--')]

            client_socket.send(f"220 {"\n".join([str(len(folders))] + folders + files)}".encode())

        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    

def handle_stor_dir_command(storageNode : StorageNode, folder, dirname, info, client_socket):
    if folder in storageNode.data:
        time = datetime.now()
        dirs = storageNode.data[folder][0]
        dirs[dirname] = info
        storageNode.data[folder] = dirs, time

        try:
            client_socket.send(f"220".encode())
            
        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    

def handle_dele_dir_command(storageNode : StorageNode, folder, dirname, client_socket):
    if folder in storageNode.data:
        time = datetime.now()
        dirs = storageNode.data[folder][0]
        dirs.pop(dirname)
        storageNode.data[folder] = dirs, time

        print(f"Pop {dirname}")
        
        try:
            client_socket.send(f"220".encode())
            
        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    

def handle_retr_command(storageNode : StorageNode, key, idx, client_socket):
    if key in storageNode.data:
        path = storageNode.data[key][0]

        try:
            with open(path, "rb") as file: # binary mode
                size = os.stat(path).st_size
                print(f"File size: {size} bytes")

                client_socket.send(f"220 {size} {" ".join(get_k_successors(storageNode))}".encode())

                response = client_socket.recv(1024).decode().strip()

                if response.startswith("220"):
                    file.seek(idx)
                    data = file.read(4096)
                    count = 0
                    while data:
                        client_socket.sendall(data)
                        count += len(data)
                        #print(f"{count} / {size}")
                        data = file.read(4096)

                    print("Transfer complete")
        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    

def handle_stor_command(storageNode : StorageNode, key, client_socket):
    try:
        os.makedirs(os.path.dirname(key), exist_ok=True)

        with open(key, "wb") as file: # binary mode
            client_socket.send(f"220".encode())

            response = client_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                while True:
                    data = client_socket.recv(4096)

                    if not data:
                        break

                    file.write(data)
                
                time = datetime.now()
                storageNode.data[key] = key, time

    except Exception as e:
        print(f"Error: {e}")


def handle_dele_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        path = storageNode.data[key][0]

        try:
            storageNode.data.pop(key)
            os.remove(path)
            client_socket.send(f"220".encode())

        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    


def handle_client(storageNode, client_socket):
    try:
        command = client_socket.recv(1024).decode().strip()
        print(f"Received command: {command}")

        if command.startswith('GS'):
            key = command[3:].strip()
            handle_gs_command(storageNode, int(key), client_socket)

        elif command.startswith('RP'):
            handle_rp_command(storageNode, client_socket)

        elif command.startswith('SS'):
            ip, port = command[3:].strip().split(":")
            handle_ss_command(storageNode, ip, int(port), client_socket)

        elif command.startswith('JOIN'):
            ip, port = command[5:].strip().split(":")
            handle_join_command(storageNode, ip, int(port), client_socket)

        elif command.startswith('LIST'):
            key = command[5:].strip()
            handle_list_command(storageNode, key, client_socket)

        elif command.startswith('MKD'):
            key = command[4:].strip()
            handle_mkd_command(storageNode, key, client_socket)

        elif command.startswith('STORDIR'):
            args = command[8:].strip().split(" ")
            idx_dirname, idx_info = [int(idx) for idx in args[:2]]

            args = " ".join(args[2:])
            handle_stor_dir_command(storageNode, args[:idx_dirname - 1], args[idx_dirname : idx_info - 1], args[idx_info:], client_socket)

        elif command.startswith('DELEDIR'):
            args = command[8:].strip().split(" ")
            idx_dirname = int(args[0])

            args = " ".join(args[1:])
            handle_dele_dir_command(storageNode, args[:idx_dirname - 1], args[idx_dirname:], client_socket)

        elif command.startswith('RETR'):
            args = command[5:].strip().split(" ")
            
            idx = int(args[0])
            key = " ".join(args[1:])

            handle_retr_command(storageNode, key, idx, client_socket)

        elif command.startswith('STOR'):
            key = command[5:].strip()
            handle_stor_command(storageNode, key, client_socket)

        elif command.startswith('DELE'):
            key = command[5:].strip()
            handle_dele_command(storageNode, key, client_socket)

        elif command.startswith('RMD'):
            key = command[4:].strip()
            handle_rmd_command(storageNode, key, client_socket)

    except ConnectionResetError:
        print("Connection reset by peer")
    finally:
        client_socket.close()

def accept_connections(storageNode):
    while True:
        client_socket, addr = storageNode.socket.accept()
        print(f"Accepted connection from {addr}")
        client_thread = threading.Thread(target=handle_client, args=(storageNode, client_socket,))
        client_thread.start()    

def accept_connections_async(storageNode):
    thread = threading.Thread(target=accept_connections, args=(storageNode,))
    thread.start()    

def main():
    accept_connections(StorageNode())

if __name__ == "__main__":
    main()