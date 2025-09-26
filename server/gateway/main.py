import time
import socket
import os
import json
import signal
import sys
from configparser import ConfigParser
from protocol import ServerProtocol
from rabbitmq.middleware import MessageMiddlewareQueue
from dataclasses import asdict 
from logger import get_logger

logger = get_logger(__name__)

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
    """Procesa cada item individualmente sin agrupar por transaction_id"""
    
    transaction_items = []
    
    for item in items:
        item_dict = asdict(item)
        
        # Crear un registro individual para cada item
        transaction_item = {
            'transaction_id': item_dict['transaction_id'],
            'item_id': item_dict['item_id'],
            'quantity': item_dict['quantity'],
            'unit_price': item_dict['unit_price'],
            'subtotal': float(item_dict['subtotal']),
            'created_at': item_dict['created_at']
        }
        transaction_items.append(transaction_item)
    
    return transaction_items

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
            
            try:
                # Use the generator to receive messages
                for message in protocol.receive_messages():
                    logger.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")
                    
                    if message.action == "EXIT":
                        logger.info("EXIT message received, closing connection")
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
                            logger.info(f"{entity_type_name} published to RabbitMQ: {entity}")
                        
                        logger.info(f"Total {entity_type_name} entities processed: {entity_count}")
                    else:
                        logger.warning(f"Unknown action: {message.action}")

                total_items_sent = 0
                
                # Procesar y enviar cada batch inmediatamente
                # while True:
                #     batch_result = protocol.receive_batch_message()
                #     if not batch_result:
                #         break
                    
                #     transaction_items, is_eof = batch_result
                #     logger.info(f"Batch recibido: {len(transaction_items)} items")
                    
                #     # Procesar y enviar este batch inmediatamente
                #     batch_processed = process_transaction_items(transaction_items)
                    
                #     for item in batch_processed:
                #         item_json = json.dumps(item)
                #         middleware.send(item_json)
                #         total_items_sent += 1
                #         logger.info(f"Item enviado a RabbitMQ: {item['transaction_id']} - Item {item['item_id']} - ${item['subtotal']}")
                    
                #     logger.info(f"Batch procesado y enviado: {len(batch_processed)} items")
                    
                #     if is_eof:
                #         logger.info("EOF recibido, terminando recepción de batches")
                #         break
                
                # # Enviar señal de finalización solo al final
                # if total_items_sent > 0:
                #     finish_signal = {"type": "FINISH", "total_items": total_items_sent}
                #     middleware.send(json.dumps(finish_signal))
                #     logger.info(f"Señal de finalización enviada con {total_items_sent} items totales") 


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