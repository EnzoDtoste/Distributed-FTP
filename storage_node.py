import threading
import socket
import os
import hashlib

def hash_function(key):
    """ Retorna un hash entero de 160 bits del input key """
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)


def setup_control_socket(host='0.0.0.0', port=50):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")
    return server_socket

def getId(host, port):
    return host + ':' + str(port)

class StorageNode:
    def __init__(self, host='0.0.0.0', port=50):
        self.identifier = hash_function(getId(host, port))
        self.socket = setup_control_socket(host, port)
        self.data = {}
        self.finger_table_bigger = []
        self.finger_table_smaller = []

def get_successor(finger_table, id):
    for i in range(len(finger_table)):
        if finger_table[i][0] > id:
            index = max(i - 1, 0)
            return (finger_table[index][1], finger_table[index][2])

    index = len(finger_table) - 1
    return (finger_table[index][1], finger_table[index][2])

def find_successor(storageNode : StorageNode, id):
    if id > storageNode.identifier:
        return get_successor(storageNode.finger_table_bigger, id)
    
    elif len(storageNode.finger_table_smaller) > 0:
        return get_successor(storageNode.finger_table_smaller, id)
    
    else:
        index = len(storageNode.finger_table_bigger) - 1
        return (storageNode.finger_table_bigger[index][1], storageNode.finger_table_bigger[index][2])

def handle_retr_command(storageNode : StorageNode, key, client_socket):
    if key in storageNode.data:
        path = storageNode.data[key]

        try:
            with open(path, "rb") as file: # binary mode
                size = os.stat(path).st_size
                print(f"File size: {size} bytes")

                client_socket.send(f"220 {size}".encode())

                response = client_socket.recv(1024).decode().strip()

                if response.startswith("220"):
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
        id_key = hash_function(key)
        ip, port = find_successor(storageNode, id_key)
        
        client_socket.send(f"550 {ip}:{port}".encode())


def handle_client(storageNode, client_socket):
    try:
        command = client_socket.recv(1024).decode().strip()
        print(f"Received command: {command}")

        if command.startswith('RETR'):
            key = command[5:].strip()
            handle_retr_command(storageNode, key, client_socket)

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