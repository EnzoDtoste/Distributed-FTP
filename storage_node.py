import threading
import socket
import os

def setup_control_socket(host='0.0.0.0', port=50):
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind((host, port))
    server_socket.listen(5)
    print(f"Listening on {host}:{port}")
    return server_socket

def handle_retr_command(path, client_socket):
    if os.path.exists(path) and os.path.isfile(path):
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
            #client_socket.send(b"451 Requested action aborted: local error in processing.\r\n")
    else:
        client_socket.send(b"550 File not found.\r\n")

def handle_client(client_socket):
    try:
        command = client_socket.recv(1024).decode().strip()
        print(f"Received command: {command}")

        if command.startswith('RETR'):
            path = command[5:].strip()
            handle_retr_command(path, client_socket)

    except ConnectionResetError:
        print("Connection reset by peer")
    finally:
        client_socket.close()

def accept_connections(server_socket):
    while True:
        client_socket, addr = server_socket.accept()
        print(f"Accepted connection from {addr}")
        client_thread = threading.Thread(target=handle_client, args=(client_socket,))
        client_thread.start()

def main():
    server_socket = setup_control_socket()
    accept_connections(server_socket)

if __name__ == "__main__":
    main()