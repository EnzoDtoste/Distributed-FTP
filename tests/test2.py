from storage_node import StorageNode, find_successor, hash_function
import threading

# Paso 1: Probar la lógica del problema

# Inicializar nodos
node1 = StorageNode(host='127.0.0.1', port=5000)
node2 = StorageNode(host='127.0.0.1', port=5001)
node3 = StorageNode(host='127.0.0.1', port=5002)

# Iniciar los nodos
threading.Thread(target=listen, args=(node1,)).start()
threading.Thread(target=listen, args=(node2,)).start()
threading.Thread(target=listen, args=(node3,)).start()

# Unir nodos al DHT
join('127.0.0.1', 5000, node2)
join('127.0.0.1', 5000, node3)

# Paso 2: Insertar un dato
key = 'my_file.txt'
value = 'This is a test file.'
put(node1, key, value)
print(f"Inserted key: {key} with value: {value}")

# Paso 3: Buscar el nodo principal donde se guardó y eliminarlo
# Suponiendo que el método get_node_for_key te da el nodo responsable del key
responsible_node_ip, responsible_node_port = find_successor(hash_function(key), node1.host, node1.port)
responsible_node = StorageNode(host=responsible_node_ip, port=responsible_node_port, setup_socket=False)
print(f"Responsible node for key {key} is {responsible_node_ip}:{responsible_node_port}")

# Paso 4: Consultar el dato insertado
retrieved_value = get(node1, key)
print(f"Retrieved value for key {key}: {retrieved_value}")

# Paso 5: Agregar un nuevo nodo y verificar la réplica
new_node = StorageNode(host='127.0.0.1', port=5003)
threading.Thread(target=listen, args=(new_node,)).start()
join('127.0.0.1', 5000, new_node)

# Paso 6: Eliminar los nodos viejos con la réplica
shutdown_node(node2)
shutdown_node(node3)

# Paso 7: Consultar el dato insertado
final_retrieved_value = get(new_node, key)
print(f"Final retrieved value for key {key} after shutdowns: {final_retrieved_value}")

# Parar el hilo de actualización y sockets de escucha
stop_update(node1)
stop_update(new_node)
node1.socket.close()
new_node.socket.close()
