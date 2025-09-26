import time
import logging
import socket
import os
import json
import signal
import sys
from configparser import ConfigParser
from protocol import ServerProtocol
from rabbitmq.middleware import MessageMiddlewareQueue
from dataclasses import asdict 


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

def process_transaction_items(items):
    """Agrupa items por transaction_id y calcula subtotales"""
    
    transactions = {}
    
    for item in items:
        item_dict = asdict(item)
        
        tx_id = item_dict['transaction_id'] 
        if tx_id not in transactions:
            transactions[tx_id] = {
                'transaction_id': tx_id,
                'subtotal': 0.0,
                'created_at': item_dict['created_at']
            }
        transactions[tx_id]['subtotal'] += float(item_dict['subtotal'])
    
    return list(transactions.values())

def main():
    global server_socket, middleware

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
            all_items = []
            
            try:
                # Use the generator to receive messages
                for message in protocol.receive_messages():
                    logging.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")
                    
                    if message.action == "EXIT":
                        logging.info("EXIT message received, closing connection")
                        break
                    elif message.action == "SEND":
                        # Parse entities using the universal parser
                        entity_count = 0
                        entity_type_name = protocol.get_entity_type_name(message.file_type)
                        
                        for entity in protocol.parse_entities(message):
                            # Send entity with type information for downstream processing
                            entity_data = f"{message.file_type}|{str(entity)}"
                            middleware.send(entity_data)
                            entity_count += 1
                            logging.info(f"{entity_type_name} published to RabbitMQ: {entity}")
                        
                        logging.info(f"Total {entity_type_name} entities processed: {entity_count}")
                    else:
                        logging.warning(f"Unknown action: {message.action}")

            except Exception as e:
                logger.error(f"Error procesando conexión: {e}")
            finally:
                protocol.close()
                logger.info(f"Conexión cerrada con {addr}")

    except KeyboardInterrupt:
        logger.info("Servidor detenido manualmente")
    finally:
        handle_shutdown(None, None)

if __name__ == "__main__":
    main()