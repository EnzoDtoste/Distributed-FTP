import socket
import threading
import os
import time

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

def send_directory_listing_nlst(client_socket, data_socket, path):
    try:
        listing = '\n'.join(os.listdir(path))
        if len(listing) > 0:
            listing += '\n'

        client_socket.send(b"150 Here comes the directory listing.\r\n")
        data_socket.sendall(listing.encode('utf-8'))
        data_socket.close()
        client_socket.send(b"226 Directory send OK.\r\n")
    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"550 Failed to list directory.\r\n")

def send_directory_listing(client_socket, data_socket, path):
    try:
        client_socket.send(b"150 Here comes the directory listing.\r\n")
        entries = os.listdir(path)
        
        listing = []
        for entry in entries:
            try:
                filepath = os.path.join(path, entry)
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
                
                listing.append("{permissions} {links} {owner} {group} {size} {mtime} {name}".format(**file_info))
            
            except:
                print("Could not read: " + str(entry))

        data_socket.sendall('\n'.join(listing).encode('utf-8'))
        data_socket.close()
        client_socket.send(b"226 Directory send OK.\r\n")
    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"550 Failed to list directory.\r\n")

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
            data_socket.close()
            client_socket.send(b"226 Transfer complete.\r\n")
            print("Transfer complete")

        elif response.startswith("550"):
            ip, port = response.split(" ")[1].split(":")
            node_socket.close()
            handle_retr_command(filename, client_socket, data_socket, current_dir, ip, int(port))
            return

    except Exception as e:
        print(f"Error: {e}")
        client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")


def handle_client(client_socket):
    current_dir = "/[Cine Clasico] Red Planet (2000) DUAL"  # Working directory
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
                    send_directory_listing_nlst(client_socket, data_socket, current_dir if len(args) == 1 else args[1])
                    data_socket.close()
                    data_socket = None  # Reset data_socket after use
            
            elif command.startswith('TYPE I'):
                client_socket.send(b'200 Type set to I.\r\n')

            elif command.startswith('LIST'):
                if data_socket:
                    send_directory_listing(client_socket, data_socket, current_dir)
                    data_socket = None  # Reset data_socket after use

            elif command.startswith('CWD'):
                new_dir = command[4:].strip()

                new_path = os.path.join(current_dir, new_dir)
                
                if os.path.exists(new_path) and os.path.isdir(new_path):
                    current_dir = new_path
                    client_socket.send(b'250 Directory successfully changed.\r\n')
                else:
                    client_socket.send(b'550 Failed to change directory.\r\n')

            elif command.startswith('RETR'):
                filename = command[5:].strip()
                handle_retr_command(filename, client_socket, data_socket, current_dir)

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