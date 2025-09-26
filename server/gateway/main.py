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
                # Use the generator to receive messages
                for message in protocol.receive_messages():
                    logging.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")
                    
                    if message.action == "EXIT":
                        logging.info("EXIT message received, closing connection")
                        break
                    elif message.action == "SEND":
                        if message.file_type == 'A':
                            # Parse as transaction items for file type A
                            for item in protocol.parse_transaction_items(message):
                                middleware.send(str(item))  
                                logging.info(f"TransactionItem publicado en RabbitMQ: {item}")
                        else:
                            # For other file types, send raw data lines
                            for data_line in protocol.parse_data_by_type(message):
                                middleware.send(f"FILE_TYPE_{message.file_type}|{data_line}")
                                logging.info(f"Data line publicado en RabbitMQ: FILE_TYPE_{message.file_type}|{data_line}")
                    else:
                        logging.warning(f"Unknown action: {message.action}")

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