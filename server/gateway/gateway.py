import time
import socket
import os
import json
import signal
import sys
from common.protocol import Protocol, ProtocolMessage
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dataclasses import asdict
from logger import get_logger
from dtos.dto import TransactionBatchDTO, BatchType, StoreBatchDTO

logger = get_logger(__name__)

class Gateway:
    def __init__(self, port, listener_backlog, rabbitmq_host, output_queue, store_exchange):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self._middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue)
        self._store_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=store_exchange,
            route_keys=['stores.data', 'stores.eof']
        )

    def handle_connection(self, client_sock):
        """Maneja una conexión entrante."""
        logger.info(f"Conexión aceptada desde {client_sock.getpeername()}")
        protocol = Protocol(client_sock)

        try:
            for message in protocol.receive_messages():
                logger.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")

                if message.action == "EXIT":
                    break
                
                elif message.action == "FINI": 
                    logger.info(f"FINISH received for file_type: {message.file_type}")
                    self._handle_finish(message.file_type)
                
                if message.action == "SEND":
                    if message.file_type == "D":
                        self.process_type_d_message(message)
                    elif message.file_type == "S":
                        self.process_type_s_message(message)
                    else:
                        self._middleware.send(asdict(message))
                else:
                    logger.warning(f"Unknown action: {message.action}")

        except Exception as e:
            logger.error(f"Error procesando conexión: {e}")
        finally:
            protocol.close()
            logger.info(f"Conexión cerrada con {client_sock.getpeername()}")
            
    def _handle_finish(self, file_type: str):
        """Maneja el mensaje FINISH enviando EOF al pipeline correspondiente."""
        try:
            if file_type == "D":
                # EOF para transactions
                eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
                self._middleware.send(eof_dto.to_bytes_fast())
                logger.info("EOF:1 enviado para tipo D (transactions)")
                
            elif file_type == "S":
                # EOF para stores
                eof_dto = StoreBatchDTO("EOF:1", batch_type=BatchType.EOF)
                self._store_middleware.send(eof_dto.to_bytes_fast(), routing_key='stores.eof')
                logger.info("EOF:1 enviado para tipo S (stores)")
            
        except Exception as e:
            logger.error(f"Error manejando FINISH: {e}")
            
    def process_type_d_message(self, message: ProtocolMessage):
        """
        Procesa un mensaje de tipo 'D', utilizando el DTO para manejar los datos en formato binario.
        """
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message.data.encode('utf-8'))

            serialized_data = dto.to_bytes_fast()

            self._middleware.send(serialized_data)
            logger.info(f"Mensaje enviado a RabbitMQ: Tipo: {dto.batch_type}")

        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'D': {e}")
            
    def process_type_s_message(self, message: ProtocolMessage):
        """
        Procesa un mensaje de tipo 'U' (stores), utilizando UserBatchDTO.
        """
        try:
            bytes_data = message.data.encode('utf-8')
            dto = StoreBatchDTO.from_bytes_fast(bytes_data)
            serialized_data = dto.to_bytes_fast()


            self._store_middleware.send(serialized_data, routing_key='stores.data')


            line_count = len([line for line in dto.data.split('\n') if line.strip()])
            logger.info(f"Stores enviados a store_data queue: Líneas: {line_count}")
            
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'U': {e}")

    def start(self):
        """Inicia el loop principal del servidor."""
        self._is_running = True
        try:
            while self._is_running:
                client_sock = self.__accept_new_connection()
                self.handle_connection(client_sock)
        except KeyboardInterrupt:
            logger.info("Servidor detenido manualmente")
        finally:
            self.shutdown(None, None)
            
    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """

        # Connection arrived
        logger.info('Accepting new connection')
        c, addr = self._server_socket.accept()
        logger.info(f'Accepted new connection from {addr[0]}')
        return c


    def shutdown(self, signal_received, frame):
        logger.info("Recibiendo señal de terminación. Cerrando servidor...")
        self._is_running = False
        if self._server_socket:
            self._server_socket.close()
            logger.info("Socket del servidor cerrado.")
        if self._middleware:
            self._middleware.close()
            logger.info("Conexión con RabbitMQ cerrada.")
        if self._store_middleware:
            self._store_middleware.close()
            logger.info("Conexión con RabbitMQ cerrada.")
        logger.info("Servidor detenido correctamente.")
        sys.exit(0)
