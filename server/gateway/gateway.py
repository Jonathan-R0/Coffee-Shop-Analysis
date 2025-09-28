import time
import socket
import os
import json
import signal
import sys
from common.protocol import Protocol, ProtocolMessage
from rabbitmq.middleware import MessageMiddlewareQueue
from dataclasses import asdict
from logger import get_logger
from dtos.dto import TransactionBatchDTO

logger = get_logger(__name__)

class Gateway:
    def __init__(self, port, listener_backlog, rabbitmq_host, output_queue):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self._middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue)

    def handle_connection(self, client_sock):
        """Maneja una conexión entrante."""
        logger.info(f"Conexión aceptada desde {client_sock.getpeername()}")
        protocol = Protocol(client_sock)

        try:
            for message in protocol.receive_messages():
                logger.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")

                if message.action == "EXIT":
                    logger.info("EXIT message received, sending EOF:1 to pipeline")
                    # Enviar EOF:1 para iniciar la secuencia de finalización sincronizada
                    eof_dto = TransactionBatchDTO("EOF:1", batch_type="EOF")
                    self._middleware.send(eof_dto.to_bytes_fast())
                    logger.info("EOF:1 enviado a RabbitMQ")
                    break
                elif message.action == "SEND":
                    if message.file_type == "D":
                        self.process_type_d_message(message)
                    else:
                        self._middleware.send(asdict(message))
                else:
                    logger.warning(f"Unknown action: {message.action}")

        except Exception as e:
            logger.error(f"Error procesando conexión: {e}")
        finally:
            protocol.close()
            logger.info(f"Conexión cerrada con {client_sock.getpeername()}")
            
    def process_type_d_message(self, message: ProtocolMessage):
        """
        Procesa un mensaje de tipo 'D', utilizando el DTO para manejar los datos en formato binario.
        """
        try:
            if message.action == "FINISH":
                logger.info("Mensaje de finalización recibido. Creando DTO de tipo CONTROL.")
                dto = TransactionBatchDTO([], batch_type="CONTROL")
            else:
                dto = TransactionBatchDTO.from_bytes(message.data.encode('utf-8'))

            serialized_data = dto.to_bytes()

            self._middleware.send(serialized_data)
            logger.info(f"Mensaje enviado a RabbitMQ: Tipo: {dto.batch_type}, Transacciones: {len(dto.transactions)}")

        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'D': {e}")

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
        logger.info("Servidor detenido correctamente.")
        sys.exit(0)
