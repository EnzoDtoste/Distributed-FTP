import threading
import socket
import os
import hashlib
import json
from datetime import datetime
import time
from utils import hash_function, getId, find_successor, get_host_ip, ping_node

def setup_control_socket(port=50):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('0.0.0.0', port))
    server_socket.listen(5)
    ip = get_host_ip()
    print(f"Listening on {ip}:{port}")
    return server_socket

class StorageNode:
    def __init__(self, port=50, setup_socket=True):
        self.socket = setup_control_socket(port) if setup_socket else None
        self.host = get_host_ip()
        self.port = port
        self.identifier = hash_function(getId(self.host, self.port))
        self.data = {}
        self.finger_table_bigger = []
        self.finger_table_smaller = []
        self.predecessor = None
        self.successors = []
        self.successor = None
        self.k_successors = 3
        self.reading_mutex = threading.Lock()
        self.reading_count = 0
        self.updating = False
        self.join_mutex = threading.Lock()
        self.update_thread = threading.Thread(target=update, args=(self,))
        self.stop_update = False

#Find the id of the succesor of a node in a single finger table
def get_table_successor(finger_table, id):
    for i in range(len(finger_table)):
        if finger_table[i][0] > id:
            index = max(i - 1, 0)
            return (finger_table[index][1], finger_table[index][2])

    index = len(finger_table) - 1
    return (finger_table[index][1], finger_table[index][2])

#Fin
def find_table_successor(storageNode : StorageNode, id):
    while storageNode.updating:
        pass

    storageNode.reading_mutex.acquire()
    storageNode.reading_count += 1
    storageNode.reading_mutex.release()

    if len(storageNode.finger_table_bigger) == 0 and len(storageNode.finger_table_smaller) == 0:
        result = storageNode.successor[1], storageNode.successor[2]

    elif len(storageNode.finger_table_bigger) > 0 and id > storageNode.identifier:
        result = get_table_successor(storageNode.finger_table_bigger, id)
    
    elif len(storageNode.finger_table_smaller) > 0 and id > storageNode.identifier:
        result = storageNode.finger_table_smaller[0][1], storageNode.finger_table_smaller[0][2]

    elif len(storageNode.finger_table_smaller) > 0:
        result = get_table_successor(storageNode.finger_table_smaller, id)
    
    else:
        index = len(storageNode.finger_table_bigger) - 1
        result = (storageNode.finger_table_bigger[index][1], storageNode.finger_table_bigger[index][2])

    storageNode.reading_mutex.acquire()
    storageNode.reading_count -= 1
    storageNode.reading_mutex.release()
    return result


#Get a list of strings ip:port of the k sucessors of a node 
def get_k_successors(storageNode : StorageNode):
    k = storageNode.k_successors
    result = []

    while storageNode.updating:
        pass

    storageNode.reading_mutex.acquire()
    storageNode.reading_count += 1
    storageNode.reading_mutex.release()

    for _, ip, port in storageNode.successors:
        if k > 0:
            result.append(f"{ip}:{port}") 
            k -= 1
        else:
            break

    storageNode.reading_mutex.acquire()
    storageNode.reading_count -= 1
    storageNode.reading_mutex.release()
    return result

#Handle et sucessor command, 
def handle_gs_command(storageNode : StorageNode, id_key, client_socket):
    if (storageNode.predecessor[0] > storageNode.identifier and (id_key <= storageNode.identifier or id_key > storageNode.predecessor[0])) or (storageNode.predecessor[0] < id_key and id_key <= storageNode.identifier):
        client_socket.send(f"220".encode())
    else:
        ip, port = find_table_successor(storageNode, id_key)
        client_socket.send(f"550 {ip}:{port}".encode())


#Handle get k successors command
def handle_gk_command(storageNode : StorageNode, client_socket):
    client_socket.send(f"220 {' '.join(get_k_successors(storageNode))}".encode())

#Send the accepting message 220
def handle_ping_command(client_socket):
    client_socket.send(f"220".encode())


def check_successors(storageNode : StorageNode):
    """Update sucesors list and replicates data if its necesary"""
    try:
        new_successors = []

        # Find first successor
        for successor in [storageNode.successor] + storageNode.successors:
            if ping_node(successor[1], successor[2]):
                new_successors.append(successor)
                break

        if len(new_successors) == 0:
            return False

        while len(new_successors) < storageNode.k_successors:
            ip, port = find_successor(new_successors[-1][0] + 1, new_successors[-1][1], new_successors[-1][2], True)
            new_successors.append((hash_function(getId(ip, port)), ip, port))

        for new_successor in new_successors:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            node_socket.connect((new_successor[1], new_successor[2]))
            
            print(f"Replicating in {new_successor[1]}:{new_successor[2]}")
            
            node_socket.sendall(f"RP".encode())

            response = node_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                items = storageNode.data.items()

                for i in range(len(items)):
                    try:
                        key, value = items[i]
                    except:
                        break

                    id_key = hash_function(key)

                    if (storageNode.predecessor[0] > storageNode.identifier and (id_key <= storageNode.identifier or id_key > storageNode.predecessor[0])) or (storageNode.predecessor[0] < id_key and id_key <= storageNode.identifier):
                        if isinstance(value[0], dict):
                            
                            data = f"{json.dumps(value[0])}".encode()
                            node_socket.sendall(f"Folder {value[1].strftime('%Y%m%d%H%M%S%f')} {len(data)} {key}".encode())

                            response = node_socket.recv(1024).decode().strip()

                            if response.startswith("220"):
                                node_socket.sendall(data)

                                response = node_socket.recv(1024).decode().strip()

                                if (not response) or not response.startswith("220"):
                                    raise Exception(f"Something went wrong replicating {new_successor[1]}:{new_successor[2]} {key}")

                                print(f"Transfer complete {key}")

                        else:
                            node_socket.sendall(f"File {value[1].strftime('%Y%m%d%H%M%S%f')} {os.stat(value[0]).st_size} {key}".encode())

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
                                    raise Exception(f"Something went wrong replicating {new_successor[1]}:{new_successor[2]} {key}")

                                print(f"Transfer complete {key}")

                node_socket.send(b"226 Transfer complete.\r\n")
            
            node_socket.close()

        storageNode.updating = True

        while storageNode.reading_count > 0:
            pass

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((new_successors[0][1], new_successors[0][2]))

        print(f"Notify Successor {new_successors[0][1]}:{new_successors[0][2]}")
        
        node_socket.sendall(f"SP {storageNode.host}:{storageNode.port}".encode())
        node_socket.close()

        storageNode.successor = new_successors[0]
        storageNode.successors = new_successors  

        storageNode.updating = False

        return True

    except Exception as e:
        storageNode.updating = False
        print(f"Checking Successors Error: {e}")

        return False


def update_finger_table(storageNode : StorageNode):
    try:
        new_finger_table_bigger = []
        new_finger_table_smaller = []
        
        request_node_ip, request_node_port = storageNode.successors[0][1], storageNode.successors[0][2]

        for i in range(161):
            ip, port = find_successor(storageNode.identifier + 2 ** i, request_node_ip, request_node_port, True)
            id = hash_function(getId(ip, port))

            request_node_ip, request_node_port = ip, port

            if id > storageNode.identifier and (len(new_finger_table_bigger) == 0 or new_finger_table_bigger[-1][0] != id):
                new_finger_table_bigger.append((id, ip, port))
            
            elif id < storageNode.identifier:
                for j in range(161 - i):
                    ip, port = find_successor(2 ** j, request_node_ip, request_node_port, True)
                    id = hash_function(getId(ip, port))

                    request_node_ip, request_node_port = ip, port
                    
                    if id >= storageNode.identifier:
                        break

                    if len(new_finger_table_smaller) == 0 or new_finger_table_smaller[-1][0] != id:
                        new_finger_table_smaller.append((id, ip, port))

                break
            
            elif id == storageNode.identifier:
                break
                

        storageNode.updating = True

        while storageNode.reading_count > 0:
            pass

        storageNode.finger_table_bigger = new_finger_table_bigger
        storageNode.finger_table_smaller = new_finger_table_smaller

        storageNode.updating = False

        return True

    except Exception as e:
        storageNode.updating = False
        print(f"Error: {e}")
        return False


def update(storageNode : StorageNode):
    while not storageNode.stop_update:
        
        storageNode.join_mutex.acquire()

        check_successors(storageNode)
        update_finger_table(storageNode)
        
        storageNode.join_mutex.release()

        time.sleep(5)
        

def request_join(storageNode : StorageNode, node_ip, node_port):
    try:
        node_ip, node_port = find_successor(getId(storageNode.host, storageNode.port), node_ip, node_port)

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"JOIN {storageNode.host}:{storageNode.port}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            storageNode.stop_update = True
            
            while storageNode.update_thread.is_alive():
                pass

            storageNode.successor = hash_function(getId(node_ip, node_port)), node_ip, node_port

            predecessor_ip, predecessor_port = response[4:].split(":")
            predecessor_port = int(predecessor_port)

            storageNode.predecessor = hash_function(getId(predecessor_ip, predecessor_port)), predecessor_ip, predecessor_port

            node_socket.send(f"220".encode())

            while(True):
                response = node_socket.recv(1024).decode().strip()

                if response.startswith("Folder"):
                    args = response[7:].strip().split(" ")
                    
                    version = datetime.strptime(args[0], '%Y%m%d%H%M%S%f')
                    size = int(args[1])
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
                    
                    version = datetime.strptime(args[0], '%Y%m%d%H%M%S%f')
                    size = int(args[1])
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

            storageNode.stop_update = False
            storageNode.update_thread.start()

            print("---------------")
            print(storageNode.successors)
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
    storageNode.join_mutex.acquire()

    join_node_id = hash_function(getId(ip, port))

    try:
        if (storageNode.predecessor[0] > storageNode.identifier and (join_node_id <= storageNode.identifier or join_node_id > storageNode.predecessor[0])) or (storageNode.predecessor[0] < join_node_id and join_node_id <= storageNode.identifier):

            client_socket.send(f"220 {storageNode.predecessor[1]}:{storageNode.predecessor[2]}".encode())

            response = client_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                items = storageNode.data.items()

                for i in range(len(items)):
                    try:
                        key, value = items[i]
                    except:
                        break

                    id_key = hash_function(key)

                    if (storageNode.predecessor[0] > join_node_id and (id_key <= join_node_id or id_key > storageNode.predecessor[0])) or (storageNode.predecessor[0] < id_key and id_key <= join_node_id):
                        if isinstance(value[0], dict):
                            
                            data = f"{json.dumps(value[0])}".encode()
                            client_socket.sendall(f"Folder {value[1].strftime('%Y%m%d%H%M%S%f')} {len(data)} {key}".encode())

                            response = client_socket.recv(1024).decode().strip()

                            if response.startswith("220"):
                                client_socket.sendall(data)

                                response = client_socket.recv(1024).decode().strip()

                                if (not response) or not response.startswith("220"):
                                    raise Exception(f"Something went wrong sending data {ip}:{port} {key}")

                                print(f"Transfer complete {key}")

                        else:
                            client_socket.sendall(f"File {value[1].strftime('%Y%m%d%H%M%S%f')} {os.stat(value[0]).st_size} {key}".encode())

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

        else:
            client_socket.send(f"550 {storageNode.predecessor[1]}:{storageNode.predecessor[2]}".encode())

    except Exception as e:
        print(f"Error: {e}")

    storageNode.join_mutex.release()
        
def handle_ss_command(storageNode : StorageNode, ip, port, client_socket):
    new_successor_id = hash_function(getId(ip, port))
    storageNode.successor = new_successor_id, ip, port

def handle_sp_command(storageNode : StorageNode, ip, port, client_socket):
    storageNode.join_mutex.acquire()

    new_predecessor_id = hash_function(getId(ip, port))
    
    if (storageNode.identifier < storageNode.predecessor[0] and (storageNode.predecessor[0] < new_predecessor_id or new_predecessor_id < storageNode.identifier)) or (storageNode.predecessor[0] < new_predecessor_id and new_predecessor_id < storageNode.identifier):
        storageNode.predecessor = new_predecessor_id, ip, port

    else:
        if not ping_node(storageNode.predecessor[1], storageNode.predecessor[2]):
            storageNode.predecessor = new_predecessor_id, ip, port

    storageNode.join_mutex.release()




def handle_rp_command(storageNode : StorageNode, client_socket):
    try:
        client_socket.send(f"220".encode())

        while(True):
            response = client_socket.recv(1024).decode().strip()

            if response.startswith("Folder"):
                args = response[7:].strip().split(" ")
                
                version = datetime.strptime(args[0], '%Y%m%d%H%M%S%f')
                size = int(args[1])
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
                
                version = datetime.strptime(args[0], '%Y%m%d%H%M%S%f')
                size = int(args[1])
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

def handle_list_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        dirs = storageNode.data[key][0]

        try:
            client_socket.send(f"220 {' '.join(get_k_successors(storageNode))}".encode())
            response = client_socket.recv(1024).decode().strip()

            if response.startswith("220"):
                values = dirs.values()
                result = []

                for i in range(len(values)):
                    try:
                        info = values[i]
                    except:
                        break
                    
                    result.append(info)

                client_socket.sendall('\n'.join(result).encode('utf-8'))
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
            items = dirs.items()

            folders = []
            files = []

            for i in range(len(items)):
                try:
                    directory, info = items[i]
                except:
                    break

                if info.startswith('drwxr-xr-x'):
                    folders.append(directory)
                else:
                    files.append(directory)

            directories = '\n'.join([str(len(folders))] + folders + files)
            client_socket.send(f"220 {directories}".encode())

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

                client_socket.send(f"220 {size} {' '.join(get_k_successors(storageNode))}".encode())

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
            #os.remove(path)
            client_socket.send(f"220".encode())

        except Exception as e:
            print(f"Error: {e}")
    else:
        client_socket.send(f"404 Not Found".encode())
    
def handle_rnfr_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        path = storageNode.data[key]


def handle_client(storageNode, client_socket):
    try:
        command = client_socket.recv(1024).decode().strip()
        print(f"Received command: {command}")

        if command.startswith('PING'):
            handle_ping_command(client_socket)

        elif command.startswith('GS'):
            key = command[3:].strip()
            handle_gs_command(storageNode, int(key), client_socket)

        elif command.startswith('GK'):
            handle_gk_command(storageNode, client_socket)

        elif command.startswith('RP'):
            handle_rp_command(storageNode, client_socket)

        elif command.startswith('SS'):
            ip, port = command[3:].strip().split(":")
            handle_ss_command(storageNode, ip, int(port), client_socket)

        elif command.startswith('SP'):
            ip, port = command[3:].strip().split(":")
            handle_sp_command(storageNode, ip, int(port), client_socket)

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

        elif command.startswith('RNFR'):
            key = command[5:].strip()
            handle_rnfr_command(storageNode, key, client_socket)

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