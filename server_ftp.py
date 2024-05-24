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
    # Asignar cualquier puerto libre del sistema
    data_socket.bind(('', 0))
    data_socket.listen(1)
    port = data_socket.getsockname()[1]
    
    # Informar al cliente sobre el puerto donde debe conectarse
    ip = client_socket.getsockname()[0].replace('.', ',')
    p1, p2 = divmod(port, 256)  # Calcular los bytes del puerto
    response = f"227 Entering Passive Mode ({ip},{p1},{p2}).\r\n"
    client_socket.send(response.encode('utf-8'))
    
    # Aceptar la conexión de datos del cliente
    data_client, addr = data_socket.accept()
    return data_client

def handle_port_command(command, client_socket):
    parts = command.split()  # Divide el comando completo en partes
    address_parts = parts[1].split(',')  # Toma la segunda parte y divide por comas
    ip_address = '.'.join(address_parts[:4])  # Reconstituye la dirección IP
    port = int(address_parts[4]) * 256 + int(address_parts[5])  # Calcula el puerto

    # Crear un socket de datos para conectarse al cliente
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

def handle_client(client_socket):
    current_dir = "/"  # Working directory
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
            
            else:
                client_socket.send(b'500 Syntax error, command unrecognized.\r\n')
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