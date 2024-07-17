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
- **Chord Ring**: All nodes are organized in a Chord ring, facilitating efficient data location and retrieval.

## Features
- **Data Replication**: Implements data replication across storage nodes to ensure high availability and fault tolerance.
- **Dynamic Scalability**: Supports dynamic addition and removal of nodes with minimal disruption to the system.

## Technologies
- **Python**: The primary programming language used for developing the system.
- **Distributed Architecture**: Utilizes a ring-based network topology typical of Chord systems for optimal data distribution and redundancy.

