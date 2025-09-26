from asyncio import sleep
import logging
import socket
import os
import json
import signal
import sys
from configparser import ConfigParser
from protocol import ServerProtocol
from rabbitmq.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

server_socket = None
middleware = None

def initialize_config():
    config = ConfigParser(os.environ)
    config.read("config.ini")

    config_params = {}
    try:
        config_params["port"] = int(os.getenv('SERVER_PORT', config["DEFAULT"]["SERVER_PORT"]))
    except KeyError as e:
        raise KeyError("Key was not found. Error: {} .Aborting server".format(e))
    except ValueError as e:
        raise ValueError("Key could not be parsed. Error: {}. Aborting server".format(e))

    return config_params

def handle_shutdown(signal_received, frame):
    global server_socket, middleware
    logger.info("Recibiendo señal de terminación. Cerrando servidor...")
    if server_socket:
        server_socket.close()
        logger.info("Socket del servidor cerrado.")
    if middleware:
        middleware.close()
        logger.info("Conexión con RabbitMQ cerrada.")
    logger.info("Servidor detenido correctamente.")
    sys.exit(0)

def main():
    global server_socket, middleware

    # Registrar señales para el shutdown
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    config = initialize_config()
    logger.info("Iniciando Servidor...")
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name="raw_data")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", config["port"]))
    server_socket.listen(5)
    logger.info(f"Servidor escuchando en el puerto {config['port']}")

    try:
        while True:
            conn, addr = server_socket.accept()
            logger.info(f"Conexión aceptada desde {addr}")

            protocol = ServerProtocol(conn)
            try:
                while True:
                    transaction_items = protocol.receive_batch_message()
                    if not transaction_items:
                        break 

                    for item in transaction_items:
                        item_json = json.dumps(item)
                        middleware.send(item_json)
                        logger.info(f"Item publicado en raw_data: {item['transaction_id']}")

            finally:
                protocol.close()
                logger.info(f"Conexión cerrada con {addr}")
                sleep(60)

    except KeyboardInterrupt:
        logger.info("Servidor detenido manualmente")
    finally:
        handle_shutdown(None, None)

if __name__ == "__main__":
    main()