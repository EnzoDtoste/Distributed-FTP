import socket
import threading
import os
from datetime import datetime
from utils import find_successor, ping_node, get_host_ip
import random
import time
import json

storage_nodes = [('10.2.0.2', 50), ('172.17.0.2', 59887)]
updating_list_storage_nodes = False
reading_list_storage_nodes = 0
reading_lock = threading.Lock()

def get_storage_node():
    """Gets a node from the Ring"""
    global storage_nodes, updating_list_storage_nodes, reading_list_storage_nodes

    while updating_list_storage_nodes:
        pass

    reading_lock.acquire()
    reading_list_storage_nodes += 1
    reading_lock.release()

    indexes = list(range(len(storage_nodes)))
    
    while len(indexes) > 0:
        index_indexes = random.randrange(0, len(indexes), 1)

        if ping_node(*storage_nodes[indexes[index_indexes]]):
            reading_list_storage_nodes -= 1
            return storage_nodes[indexes[index_indexes]]

        else:
            indexes.pop(index_indexes)

    reading_lock.acquire()
    reading_list_storage_nodes -= 1
    reading_lock.release()

def broadcast_storage_node():
    broadcast_ip = '<broadcast>'
    broadcast_port = 37020
    message = json.dumps({'action': 'report'})
    
    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_BROADCAST, 1)
        sock.settimeout(5)
        
        try:
            sock.sendto(message.encode(), (broadcast_ip, broadcast_port))

            print(f"Broadcast message sent: {message}")
            
            while True:
                try:
                    response, _ = sock.recvfrom(1024)
                    response_data = json.loads(response.decode())
                    
                    if response_data.get('action') == 'reporting':
                        ip = response_data.get('ip')
                        port = response_data.get('port')

                        try:
                            print(f"Detected {ip}:{port}")
                            return ip, port
                        except:
                            pass
                
                except socket.timeout:
                    print("Broadcast request timed out")
                    break
        except Exception as e:
            print(f"Exception in broadcast_request_join: {e}")

def update_list_storage_nodes():
    """Refreesh the StorageNodes list"""
    global storage_nodes, updating_list_storage_nodes, reading_list_storage_nodes

    while True:
        new_storage_nodes = set()

        for ip, port in storage_nodes:
            node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            
            try:    
                node_socket.connect((ip, port))

                node_socket.sendall(f"GK".encode())
                response = node_socket.recv(1024).decode().strip()

                if response.startswith("220"):
                    new_storage_nodes.add((ip, port))

                    for address in response.split(" ")[1:]:
                        new_storage_nodes.add((address.split(":")[0], int(address.split(":")[1])))
            except:
                pass

            finally:
                node_socket.close()

        if len(new_storage_nodes) == 0:
            storage_node = broadcast_storage_node()

            if storage_node:
                new_storage_nodes.add(storage_node)

        if len(new_storage_nodes) > 0:
            updating_list_storage_nodes = True

            while reading_list_storage_nodes > 0:
                pass

            storage_nodes = list(new_storage_nodes)
            print(storage_nodes)

            updating_list_storage_nodes = False

        time.sleep(5)


def setup_control_socket(host='0.0.0.0', port=21):
    """The clients must connect to this socket"""
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind((host, port))
    except:
        server_socket.bind((host, 0))

    server_socket.listen(5)
    port = server_socket.getsockname()[1]
    print(f"Listening on {host}:{port}")
    return server_socket

def handle_pasv_command(client_socket, port_range=(50000, 50100)):
    """"Response for PASV command, creates a socket within a range and connect it with the client"""
    for port in random.sample(range(*port_range), port_range[1] - port_range[0]):
        try:
            data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            data_socket.bind(('0.0.0.0', port))
            break
        except OSError:
            continue
    
    else:
        raise IOError("No ports available in the range specified")
    
    data_socket.listen(1)
    port = data_socket.getsockname()[1]
    print(port)

    # Inform the client about the connection port
    ip = '127,0,0,1'#client_socket.getsockname()[0].replace('.', ',')
    p1, p2 = divmod(port, 256)  # Calculate port bytes
    response = f"227 Entering Passive Mode ({ip},{p1},{p2}).\r\n"
    client_socket.send(response.encode('utf-8'))
    
    # Accept data connection with client
    data_client, addr = data_socket.accept()
    return data_client

def handle_port_command(command, client_socket):
    """Response for PORT command, stablish a connection between a client and the server, returns the socket with the conection""" 
    parts = command.split()
    address_parts = parts[1].split(',') 
    ip_address = '.'.join(address_parts[:4])  # get ip address
    port = int(address_parts[4]) * 256 + int(address_parts[5])  # Calculates port

    # Create data socket with client
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect((ip_address, port))
    return data_socket

def send_directory_listing(client_socket, data_socket, current_dir, node_ip=None, node_port=None):
    """Finds the node where its located a file and lists its content"""
    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(current_dir, node_ip, node_port)
        except:
            send_directory_listing(client_socket, data_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"LIST {current_dir}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            client_socket.send(b"150 Here comes the directory listing.\r\n")

            addresses = response[4:].split(" ") if len(response) > 4 else []
            aux_nodes = [(address.split(":")[0], int(address.split(":")[1])) for address in addresses]

            node_socket.send(b"220 Ok")

            data = ""

            while True:
                try:
                    chunck = node_socket.recv(4096).decode('utf-8')
                except:
                    if len(aux_nodes) == 0:
                        node_socket.close()
                        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
                        return

                    while len(aux_nodes) > 0:
                        node_socket.close()

                        ip, port = aux_nodes.pop(0)

                        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            node_socket.connect((ip, port))

                            print(f"Aux-connected to {ip}:{port}")
            
                            node_socket.sendall(f"LIST {current_dir}".encode())
                            response = node_socket.recv(1024).decode().strip()

                            if response.startswith("220"):
                                addresses = response[4:].split(" ") if len(response) > 4 else []
                                aux_nodes += [(address.split(":")[0], int(address.split(":")[1])) for address in addresses]
                                
                                node_socket.send(b"220 Ok")

                                data = ""
                                break

                            elif response.startswith("550"):
                                ip, port = response.split(" ")[1].split(":")
                                aux_nodes.append((ip, int(port)))

                        except:
                            pass

                    continue


                if chunck:
                    data += chunck
                else:
                    break
            
            data_socket.sendall(data.encode('utf-8'))

            node_socket.close()
            client_socket.send(b"226 Directory send OK.\r\n")
            print("Transfer complete")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            send_directory_listing(client_socket, data_socket, current_dir, ip, int(port))
            return
        
        else:
            client_socket.send(b"550 Failed to list directory.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def send_stor_dir_command(dirname, info, current_dir, node_ip=None, node_port=None):
    """Insert a file in the list of the parent folder"""
    dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(current_dir, node_ip, node_port)
        except:
            return send_stor_dir_command(dirname, info, current_dir)

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"STORDIR {len(current_dir) + 1} {len(current_dir) + 1 + len(dir_path) + 1} {current_dir} {dir_path} {info}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()
            return True

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            return send_stor_dir_command(dirname, info, current_dir, ip, int(port))
        
        else:
            return False

    except Exception as e:
        print(f"Error: {e}")
        return False
    
def send_dele_dir_command(dirname, current_dir, node_ip=None, node_port=None):
    """Erase a file from the list of the parent folder"""
    dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(current_dir, node_ip, node_port)
        except:
            return send_dele_dir_command(dirname, current_dir)

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"DELEDIR {len(current_dir) + 1} {current_dir} {dir_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()
            return True

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            return send_dele_dir_command(dirname, current_dir, ip, int(port))
        
        else:
            return False

    except Exception as e:
        print(f"Error: {e}")
        return False

def handle_mkd_command(dirname, client_socket, current_dir, node_ip=None, node_port=None):
    """Response for MKD command, it creates a new directory in the server in the current path with the name specified in dirname.
      This data is stored in the apropiated node"""
    new_dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(new_dir_path, node_ip, node_port)
        except:
            handle_mkd_command(dirname, client_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"MKD {new_dir_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()

            if send_stor_dir_command(dirname, f"drwxr-xr-x 1 0 0 0 {datetime.now().strftime('%b %d %H:%M')} {os.path.basename(dirname)}", current_dir):
                if client_socket:
                    client_socket.send(f'257 "{new_dir_path}" created.\r\n'.encode())
            else:
                if client_socket:
                    client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_mkd_command(dirname, client_socket, current_dir, ip, int(port))
            return
        
        else:
            if client_socket:
                client_socket.send(b"550 Directory already exists.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        if client_socket:
            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_rmd_command(dirname, client_socket, current_dir, node_ip=None, node_port=None):
    """Response for RMD command, it finds the node where the requested directory should be located, and if it finds it, it is removed from the node"""
    dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(dir_path, node_ip, node_port)
        except:
            handle_rmd_command(dirname, client_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"RMD {dir_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()

            lines = response[4:].split("\n")
            folder_count = int(lines[0])

            folders = lines[1:folder_count + 1] if folder_count > 0 else []
            files = lines[folder_count + 1:] if len(lines) > folder_count + 1 else []

            for folder in folders:
                print(f"RMD {folder}")
                handle_rmd_command(folder, None, os.path.normpath(os.path.dirname(folder)))
                
            for file in files:
                print(f"DELE {file}")
                handle_dele_command(file, None, os.path.normpath(os.path.dirname(file)))

            if send_dele_dir_command(dir_path, current_dir) and client_socket:
                client_socket.send(f'250 "{dir_path}" deleted.\r\n'.encode())

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_rmd_command(dirname, client_socket, current_dir, ip, int(port))
            return
        
        else:
            if client_socket:
                client_socket.send(b"550 Directory do not exists.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        if client_socket:
            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_retr_command(filename, client_socket, data_socket, current_dir, node_ip=None, node_port=None):
    """Response for RETR command, it finds the node where the requested file should be located, and if it finds it, it is sended to the client"""
    file_path = os.path.join(current_dir, filename)
    
    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(file_path, node_ip, node_port)
        except:
            handle_retr_command(filename, client_socket, data_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"RETR 0 {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            args = response[4:].strip().split(" ")

            size = int(args[0])

            aux_nodes = [(address.split(":")[0], int(address.split(":")[1])) for address in args[1:]] if len(args) > 1 else []

            if client_socket:
                client_socket.send(b"150 Opening binary mode data connection.\r\n")

            count = 0
            node_socket.send(b"220 Ok")

            while count < size:
                try:
                    data = node_socket.recv(4096)
                except:
                    if len(aux_nodes) == 0:
                        node_socket.close()
                        if client_socket:
                            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
                        return

                    while len(aux_nodes) > 0:
                        node_socket.close()

                        ip, port = aux_nodes.pop(0)

                        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

                        try:
                            node_socket.connect((ip, port))

                            print(f"Aux-connected to {ip}:{port}")
            
                            node_socket.sendall(f"RETR {count} {file_path}".encode())
                            response = node_socket.recv(1024).decode().strip()

                            if response.startswith("220"):
                                args = response[4:].strip().split(" ")
                                aux_nodes += [(address.split(":")[0], int(address.split(":")[1])) for address in args[1:]] if len(args) > 1 else []
                                
                                node_socket.send(b"220 Ok")
                                break

                            elif response.startswith("550"):
                                ip, port = response.split(" ")[1].split(":")
                                aux_nodes.append((ip, int(port)))

                        except:
                            pass

                    continue

                data_socket.sendall(data)
                count += len(data)
                #print(f"{count} / {size}")

            node_socket.close()
            if client_socket:
                client_socket.send(b"226 Transfer complete.\r\n")
            print("Transfer complete")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_retr_command(filename, client_socket, data_socket, current_dir, ip, int(port))
            return
        
        else:
            client_socket.send(b"550 File not found.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        if client_socket:
            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
        else:
            raise e


def handle_stor_command(filename, client_socket, data_socket, current_dir, node_ip=None, node_port=None):
    """Response for STOR command, it is used to upload a copy of a local file to the server, it finds where the file should be saved and send the data to that node"""
    file_path = os.path.join(current_dir, filename)
    
    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(file_path, node_ip, node_port)
        except:
            handle_stor_command(filename, client_socket, data_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"STOR {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            if client_socket:
                client_socket.send(b"150 Opening binary mode data connection for file transfer.\r\n")
            
            node_socket.send(b"220 Ok")

            count = 0
            while True:
                data = data_socket.recv(4096)
                node_socket.sendall(data)

                if not data:
                    break

                count += len(data)
            
            node_socket.close()

            if send_stor_dir_command(filename, f"-rw-r--r-- 1 0 0 {count} {datetime.now().strftime('%b %d %H:%M')} {os.path.basename(filename)}", current_dir):
                if client_socket:
                    client_socket.send(b"226 Transfer complete.\r\n")
            else:
                if client_socket:
                    client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_stor_command(filename, client_socket, data_socket, current_dir, ip, int(port))
            return

    except Exception as e:
        print(f"Error: {e}")
        if client_socket:
            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
        else:
            raise e

def handle_dele_command(filename, client_socket, current_dir, node_ip=None, node_port=None):
    """Response for DELE command, search for the node where the selected file must be, and if it finds it, removes the file from that node"""
    file_path = os.path.join(current_dir, filename)
    
    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(file_path, node_ip, node_port)
        except:
            handle_dele_command(filename, client_socket, current_dir)
            return

        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"DELE {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()

            if send_dele_dir_command(filename, current_dir) and client_socket:
                client_socket.send(b"250 File deleted successfully.\r\n")
            elif client_socket:
                client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_dele_command(filename, client_socket, current_dir, ip, int(port))
            return
        
        elif client_socket:
            client_socket.send(b"550 File not found.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        if client_socket:
            client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def copy_folder(folder_path, new_folder_path):
    handle_mkd_command(os.path.basename(new_folder_path), None, os.path.dirname(new_folder_path))

    try:
        node_ip, node_port = None, None

        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(folder_path, node_ip, node_port)
        except:
            return copy_folder(folder_path, new_folder_path)    
        
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"READ {folder_path}".encode())

        response = node_socket.recv(1024).decode().strip()
        print(response)

        node_socket.close()

        if response.startswith("220"):
            lines = response[4:].split("\n")
            folder_count = int(lines[0])

            folders = lines[1:folder_count + 1] if folder_count > 0 else []
            files = lines[folder_count + 1:] if len(lines) > folder_count + 1 else []

            for folder in folders:
                print(f"Copy {folder} to {os.path.join(new_folder_path, os.path.basename(folder))}")

                if not copy_folder(folder, os.path.join(new_folder_path, os.path.basename(folder))):
                    return False
                
            for file in files:
                print(f"Copy {file} to {os.path.join(new_folder_path, os.path.basename(file))}")
                
                if not copy_file(file, os.path.join(new_folder_path , os.path.basename(file))):
                    return False    
                
            return True

    except Exception as e:
        print(f"Error: {e}")
        return False

        


def copy_file(file_path, new_file_path):
    try:
        bind_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        bind_socket.bind(('0.0.0.0', 0))
        bind_socket.listen(1)

        output_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pack_input = [None]

        def accept_conn():
            pack_input[0] = bind_socket.accept()[0]

        threading.Thread(target=accept_conn, args=()).start()

        output_socket.connect((get_host_ip(), bind_socket.getsockname()[1]))

        time.sleep(1)
        bind_socket.close()
        input_socket = pack_input[0]

        retr_finished = [False]
        stor_finished = [False]

        def retr():
            handle_retr_command(os.path.basename(file_path), None, input_socket, os.path.dirname(file_path))
            input_socket.close()
            retr_finished[0] = True

        def stor():
            handle_stor_command(os.path.basename(new_file_path), None, output_socket, os.path.dirname(new_file_path))
            output_socket.close()
            stor_finished[0] = True

        threading.Thread(target=retr, args=()).start()
        threading.Thread(target=stor, args=()).start()
    
        while (not retr_finished[0]) or (not stor_finished[0]):
            pass

        return True
    except:
        return False

def handle_rnfr_command(directory_name, client_socket, current_dir, node_ip=None, node_port=None):
    dir_path = os.path.join(current_dir, directory_name)

    try:
        while node_ip is None or node_port is None:
            node_ip, node_port = get_storage_node()

        try:
            node_ip, node_port = find_successor(dir_path, node_ip, node_port)
        except:
            handle_rnfr_command(directory_name, client_socket, current_dir)
            return
        
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"ED {dir_path}".encode())

        response = node_socket.recv(1024).decode().strip()
        
        node_socket.close()

        if response.startswith("220"):
            type = response[4:]
            
            client_socket.send(b"350 Requested file action pending further information.\r\n")

            response = client_socket.recv(1024).decode().strip()

            if response.startswith("RNTO"):
                new_dir_name = response[5:]
                new_dir_path = os.path.join(current_dir, new_dir_name)
                
                if type == "File":
                    if copy_file(dir_path, new_dir_path):
                        handle_dele_command(os.path.basename(dir_path), None, os.path.normpath(os.path.dirname(dir_path)))
                        client_socket.send(b"250 Requested file action okay, completed.\r\n")
                    else:
                        client_socket.send(b"550 Requested action aborted: local error in processing.\r\n")
                
                elif type == "Folder":
                    if copy_folder(dir_path, new_dir_path):
                        handle_rmd_command(os.path.basename(dir_path), None, os.path.normpath(os.path.dirname(dir_path)))
                        client_socket.send(b"250 Requested file action okay, completed.\r\n")
                    else:
                        client_socket.send(b"550 Requested action aborted: local error in processing.\r\n")

                    
        else:
            client_socket.send(b"550 Requested action not taken.\r\n")

        

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_client(client_socket):
    """Manages the request sended by the client according to the header"""
    current_dir = os.path.normpath("/app")  # Working directory
    data_socket = None

    try:
        while True:
            command = client_socket.recv(1024).decode().strip()
            print(f"Received command: {command}")

            if command.startswith('USER'): 
                # No Authentication
                client_socket.send(b'230 User logged in.\r\n')
            
            elif command.startswith('AUTH TLS') or command.startswith('AUTH SSL'):
                # Reject TLS/SSL for now
                client_socket.send(b'500 Command not implemented.\r\n')
            
            elif command.startswith('SYST'):
                client_socket.send(b'215 UNIX Type: L8\r\n')
            
            elif command.startswith('FEAT'):
                # Some Features
                features = "211-Features:\r\n PASV\r\n UTF8\r\n211 End\r\n"
                client_socket.send(features.encode())
            
            elif command.startswith('PWD'):
                # Print Working Directory
                client_socket.send(f'257 "{current_dir}" is the current directory.\r\n'.encode())
            
            elif command.startswith('OPTS UTF8 ON'):
                # Set UTF8
                client_socket.send(b'200 UTF8 set to on\r\n')
            
            elif command.startswith('PORT'):
                data_socket = handle_port_command(command, client_socket)
                client_socket.send(b'200 PORT command successful.\r\n')
            
            elif command.startswith('PASV'):
                data_socket = handle_pasv_command(client_socket)

            elif command.startswith('NLST'):
                if data_socket:
                    args = command.split()
                    send_directory_listing(client_socket, data_socket, current_dir if len(args) == 1 else os.path.normpath(args[1]))
                    data_socket.close()
                    data_socket = None  # Reset data_socket after use
            
            elif command.startswith('TYPE I'):
                client_socket.send(b'200 Type set to I.\r\n')
            
            elif command.startswith('TYPE A'):
                client_socket.send(b'200 Type set to A.\r\n')

            elif command.startswith('LIST'):
                if data_socket:
                    send_directory_listing(client_socket, data_socket, current_dir)
                    data_socket.close()
                    data_socket = None  # Reset data_socket after use

            elif command.startswith('CWD'):
                new_path = os.path.normpath(command[4:].strip())
                
                if current_dir != os.path.normpath("/app") or new_path != "..":
                    current_dir = os.path.normpath(os.path.join(current_dir, new_path))

                client_socket.send(b'250 Directory successfully changed.\r\n')

            elif command.startswith('RETR'):
                filename = command[5:].strip()
                if data_socket:
                    handle_retr_command(filename, client_socket, data_socket, current_dir)
                    data_socket.close()
                    data_socket = None

            elif command.startswith('STOR'):
                filename = command[5:].strip()
                if data_socket:
                    handle_stor_command(filename, client_socket, data_socket, current_dir)
                    data_socket.close()
                    data_socket = None

            elif command.startswith('DELE'):
                filename = command[5:].strip()
                handle_dele_command(filename, client_socket, current_dir)

            elif command.startswith('MKD'):
                dirname = command[4:].strip()
                handle_mkd_command(dirname, client_socket, current_dir)

            elif command.startswith('RMD'):
                dirname = command[4:].strip()
                handle_rmd_command(dirname, client_socket, current_dir)
            
            elif command.startswith('RNFR'):
                dirname = command[5:].strip()
                handle_rnfr_command(dirname, client_socket, current_dir)

            else:
                client_socket.send(b'500 Syntax error, command unrecognized.\r\n')
    except ConnectionAbortedError:
        print("Connection aborted by peer")
    except ConnectionResetError:
        print("Connection reset by peer")
    finally:
        if data_socket:
            data_socket.close()
        client_socket.close()

def accept_connections(server_socket):
    """Accepts the request for contections from the client"""
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_socket.send(b"220 Welcome to the FTP server.\r\n")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

def main():
    server_socket = setup_control_socket()

    update_list_storage_nodes_thread = threading.Thread(target=update_list_storage_nodes, args=())
    update_list_storage_nodes_thread.start()

    accept_connections(server_socket)

if __name__ == "__main__":
    main()