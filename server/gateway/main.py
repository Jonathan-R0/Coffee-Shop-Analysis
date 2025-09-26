import logging
import socket
import logging
import os
from configparser import ConfigParser
from protocol import ServerProtocol
from rabbitmq.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)


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


def main():
    config = initialize_config()
    logging.info("Iniciando Servidor...")
    rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
    middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name="data_queue")

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(("0.0.0.0", config["port"]))
    server_socket.listen(5)
    
    try:
        while True:
            conn, addr = server_socket.accept()
            logging.info(f"Conexión aceptada desde {addr}")

            protocol = ServerProtocol(conn)
            try:
                while True:
                    transaction_items = protocol.receive_client_data()
                    if not transaction_items:
                        break 

                    for item in transaction_items:
                        middleware.send(str(item))  
                        logging.info(f"Item publicado en RabbitMQ: {item}")

            finally:
                protocol.close()
                logging.info(f"Conexión cerrada con {addr}")

    except KeyboardInterrupt:
        logging.info("Servidor detenido manualmente")
    finally:
        server_socket.close()
        middleware.close()
        logging.info("Servidor cerrado")


if __name__ == "__main__":
    main()