# Usa la imagen base oficial de Python
FROM python:3.11

# Establece el directorio de trabajo en el contenedor
WORKDIR /app


# Copia el código de la aplicación en el contenedor
COPY . .

# Expone el puerto que usará el servidor
EXPOSE 21

# Define el comando por defecto para ejecutar el servidor
CMD ["python", "test.py"]
