<table style="border-collapse: collapse; width: 100%;">
  <tr>
    <th style="border: 1px solid black; background-color: gray; color: black;">Nombre</th>
    <th style="border: 1px solid black; background-color: gray; color: black;">Grupo</th>
  </tr>
  <tr>
    <td style="border: 1px solid black; background-color: gray; color: black;">Enzo Rojas D´toste</td>
    <td style="border: 1px solid black; background-color: gray; color: black;">C411</td>
  </tr>
  <tr>
    <td style="border: 1px solid black; background-color: gray; color: black;">Daniel Abad Fundora</td>
    <td style="border: 1px solid black; background-color: gray; color: black;">C411</td>
  </tr>
</table>

# Distributed FTP System Using Chord Protocol

## Overview
The project develops a distributed FTP system designed to enhance file transfer reliability and scalability by leveraging the Chord distributed hash table (DHT) protocol.

## Architecture
- **Nodes**: The system incorporates two types of nodes: data storage nodes and order routing nodes.
- **Chord Ring**: All data storage nodes are organized in a Chord ring, facilitating efficient data location and retrieval.

## Features
- **Data Replication**: Implements data replication across storage nodes to ensure high availability and fault tolerance.
- **Dynamic Scalability**: Supports dynamic addition and removal of nodes with minimal disruption to the system.

## Technologies
- **Python**: The primary programming language used for developing the system.
- **Distributed Architecture**: Utilizes a ring-based network topology typical of Chord systems for optimal data distribution and redundancy.

## Execution Instructions

### Initial Setup with Zero Nodes
- **Local Execution**: If there are no nodes in the ring, run:

`python init_ftp_storage.py`

- **Docker Execution**: Build the Docker image if necessary, and run the container:

`docker build -f dockerfile_init_ftp_storage -t dist_init .`\
`docker run --rm -it dist_init`

This will create the root path `/app`. Note: Just because this node was the first created does not mean it is essential; it can be removed if more nodes are present, and the system will still function.

### Adding Storage Nodes
- **Local Execution**: If at least one node exists, execute:

`python storage_node.py`

- **Docker Execution**: Build the Docker image if necessary, and run the container. You can repeat this process to create as many storage nodes as needed:

`docker build -f dockerfile_storage -t dist_storage .`\
`docker run --rm -it dist_storage`


### Starting Routing Nodes
- **Local Execution**: For routing nodes, which establish FTP client connections, execute:

`python server_ftp.py`


- **Docker Execution**: Build the Docker image if necessary, and run the container:

`docker build -f dockerfile_routing -t dist_routing .`\
`docker run --rm -p x:21 -p 50000-50100:50000-50100 -it dist_routing`


Replace `x` in `x:21` with the port you want to expose to the FTP client.

### Client Setup
- **FTP Client**: Use any software that implements the RFC 959 FTP protocol, such as FileZilla.
- **Server Configuration**: For local setups, use `127.0.0.1`. Username and password can be any text. The port should be one of those used by the routing nodes. If routing nodes are local check the "Listening" status printed on the routing node consoles for the correct port.
