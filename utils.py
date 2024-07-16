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

def ping_node(node_ip, node_port, verbose=True):
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.settimeout(2)

    try:
        node_socket.connect((node_ip, node_port))

        node_socket.sendall(f"PING".encode())
        
        if verbose:
            print(f"Ping to {node_ip}:{node_port}")

        response = node_socket.recv(1024).decode().strip()
        node_socket.close()

        return response.startswith("220")
    except:
        node_socket.close()
        return False

def find_successor(id_key, node_ip, node_port, hash = False, verbose=True):
    if not hash:
        id_key = hash_function(id_key)
    
    node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    node_socket.connect((node_ip, node_port))

    if verbose:
        print(f"Connected to {node_ip}:{node_port}")
    
    node_socket.sendall(f"GS {id_key}".encode())

    response = node_socket.recv(1024).decode().strip()

    if response.startswith("220"):
        node_socket.close()
        return node_ip, node_port
    
    ip, port = response.split(" ")[1].split(":")
    node_socket.close()
        
    return find_successor(id_key, ip, int(port), True, verbose)