import hashlib
import socket

def hash_function(key):
    """ Retorna un hash entero de 160 bits del input key """
    return int(hashlib.sha1(key.encode('utf-8')).hexdigest(), 16)

def get_host_ip():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.connect(("8.8.8.8", 80))
        ip = s.getsockname()[0]
    finally:
        s.close()
    return ip

def getId(host, port):
    return host + ':' + str(port)

def find_successor(id_key, node_ip, node_port, hash = False):
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