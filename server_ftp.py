import socket
import threading
import os
from datetime import datetime

def setup_control_socket(host='0.0.0.0', port=21):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")
    return server_socket

def handle_pasv_command(client_socket):
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # Assign any port free
    data_socket.bind(('', 0))
    data_socket.listen(1)
    port = data_socket.getsockname()[1]
    
    # Inform the client about the connection port
    ip = client_socket.getsockname()[0].replace('.', ',')
    p1, p2 = divmod(port, 256)  # Calculate port bytes
    response = f"227 Entering Passive Mode ({ip},{p1},{p2}).\r\n"
    client_socket.send(response.encode('utf-8'))
    
    # Accept data connection with client
    data_client, addr = data_socket.accept()
    return data_client

def handle_port_command(command, client_socket):
    parts = command.split()
    address_parts = parts[1].split(',') 
    ip_address = '.'.join(address_parts[:4])  # get ip address
    port = int(address_parts[4]) * 256 + int(address_parts[5])  # Calculates port

    # Create data socket with client
    data_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    data_socket.connect((ip_address, port))
    return data_socket

def send_directory_listing(client_socket, data_socket, current_dir, node_ip='127.0.0.1', node_port=50):
    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"LIST {current_dir}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            client_socket.send(b"150 Here comes the directory listing.\r\n")

            node_socket.send(b"220 Ok")

            data = ""

            while True:
                chunck = node_socket.recv(4096).decode('utf-8')

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


def send_stor_dir_command(dirname, info, current_dir, node_ip='127.0.0.1', node_port=50):
    dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
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
    
def send_dele_dir_command(dirname, current_dir, node_ip='127.0.0.1', node_port=50):
    dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"STORDIR {len(current_dir) + 1} {len(current_dir) + 1 + len(dir_path) + 1} {current_dir} {dir_path}".encode())

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

def handle_mkd_command(dirname, client_socket, current_dir, node_ip='127.0.0.1', node_port=50):
    new_dir_path = os.path.normpath(os.path.join(current_dir, dirname))

    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"MKD {new_dir_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()

            if send_stor_dir_command(dirname, f"drwxr-xr-x 1 0 0 0 {datetime.now().strftime("%b %d %H:%M")} {os.path.basename(dirname)}", current_dir):
                client_socket.send(f'257 "{new_dir_path}" created.\r\n'.encode())
            else:
                client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_mkd_command(dirname, client_socket, current_dir, ip, int(port))
            return
        
        else:
            client_socket.send(b"550 Directory already exists.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

def handle_retr_command(filename, client_socket, data_socket, current_dir, node_ip='127.0.0.1', node_port=50):
    file_path = os.path.join(current_dir, filename)
    
    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"RETR {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            size = int(response[4:].strip())

            client_socket.send(b"150 Opening binary mode data connection.\r\n")

            count = 0
            node_socket.send(b"220 Ok")

            while count < size:
                data = node_socket.recv(4096)
                data_socket.sendall(data)
                count += len(data)
                #print(f"{count} / {size}")

            node_socket.close()
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
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_stor_command(filename, client_socket, data_socket, current_dir, node_ip='127.0.0.1', node_port=50):
    file_path = os.path.join(current_dir, filename)
    
    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"STOR {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
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

            if send_stor_dir_command(filename, f"-rw-r--r-- 1 0 0 {count} {datetime.now().strftime("%b %d %H:%M")} {os.path.basename(filename)}", current_dir):
                client_socket.send(b"226 Transfer complete.\r\n")
            else:
                client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_stor_command(filename, client_socket, data_socket, current_dir, ip, int(port))
            return

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

def handle_dele_command(filename, client_socket, current_dir, node_ip='127.0.0.1', node_port=50):
    file_path = os.path.join(current_dir, filename)
    
    try:
        node_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        node_socket.connect((node_ip, node_port))
        
        print(f"Connected to {node_ip}:{node_port}")
        
        node_socket.sendall(f"DELE {file_path}".encode())

        response = node_socket.recv(1024).decode().strip()

        if response.startswith("220"):
            node_socket.close()

            if send_dele_dir_command(filename, current_dir):
                client_socket.send(b"250 File deleted successfully.\r\n")
            else:
                client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_dele_command(filename, client_socket, current_dir, ip, int(port))
            return
        
        else:
            client_socket.send(b"550 File not found.\r\n")

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_client(client_socket):
    current_dir = os.path.normpath("/[Cine Clasico] Red Planet (2000) DUAL")  # Working directory
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
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_socket.send(b"220 Welcome to the FTP server.\r\n")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

def main():
    server_socket = setup_control_socket()
    accept_connections(server_socket)

if __name__ == "__main__":
    main()