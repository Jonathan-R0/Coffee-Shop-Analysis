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
from dtos.dto import TransactionBatchDTO

logger = get_logger(__name__)

class Gateway:
    def __init__(self, port, listener_backlog, rabbitmq_host, output_queue, reports_exchange=None):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self._middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue)
        self._reports_exchange = reports_exchange

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
                    
                    # Esperar por el reporte y enviarlo al cliente
                    self._wait_and_send_report(protocol)
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

    def _wait_and_send_report(self, protocol):
        """
        Espera por el reporte del report generator y lo envía al cliente.
        """
        try:
            logger.info("Esperando reporte del pipeline...")
            
            # Crear middleware para recibir reportes
            report_middleware = MessageMiddlewareQueue(
                host=self._middleware.host,
                queue_name="report_queue"
            )
            
            # Recibir batches del reporte
            report_transactions = []
            
            def report_callback(ch, method, properties, body):
                try:
                    # Usar DTO para procesar el mensaje
                    dto = TransactionBatchDTO.from_bytes_fast(body)
                    
                    if dto.batch_type == "EOF":
                        logger.info("EOF del reporte recibido")
                        # Detener consumo cuando recibimos EOF
                        report_middleware.stop_consuming()
                        return
                    elif dto.batch_type == "DATA":  
                        # Agregar transacciones del batch
                        report_transactions.extend(dto.transactions)
                        logger.info(f"Batch del reporte recibido: {len(dto.transactions)} transacciones")
                    elif dto.batch_type == "RAW_CSV":
                        # Procesar CSV raw del reporte (ya viene con transaction_id,final_amount)
                        lines = dto.transactions.strip().split('\n')
                        for line in lines:
                            if line.strip():
                                values = line.split(',')
                                if len(values) >= 2:
                                    report_transactions.append({
                                        "transaction_id": values[0],
                                        "final_amount": values[1]
                                    })
                        logger.info(f"Batch CSV del reporte procesado: {len(lines)} líneas")
                    
                except Exception as e:
                    logger.error(f"Error procesando batch del reporte: {e}")
            
            # Consumir batches hasta recibir EOF
            report_middleware.start_consuming(report_callback)
            
            # Una vez que terminamos, convertir y enviar al cliente
            if report_transactions:
                csv_content = self._convert_transactions_to_csv(report_transactions)
                self._send_report_via_protocol(protocol, csv_content)
                logger.info(f"Reporte enviado al cliente: {len(report_transactions)} transacciones")
            else:
                logger.warning("No se recibieron transacciones del reporte")
                self._send_error_to_client(protocol, "No report data received")
                
            report_middleware.close()
                
        except Exception as e:
            logger.error(f"Error esperando reporte: {e}")
            self._send_error_to_client(protocol, f"Error processing report: {e}")

    def _convert_transactions_to_csv(self, transactions):
        """
        Convierte lista de transacciones a formato CSV.
        """
        try:
            # Crear CSV
            csv_lines = ["transaction_id,final_amount"]
            for transaction in transactions:
                csv_lines.append(f"{transaction['transaction_id']},{transaction['final_amount']}")
            
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo transacciones a CSV: {e}")
            return "transaction_id,final_amount\nERROR,0"

    def _send_report_via_protocol(self, protocol, csv_content):
        """
        Envía el reporte usando el protocolo existente en batches.
        """
        try:
            # Usar el método de batches del protocolo para enviar respuesta
            success = protocol.send_response_batches("RPRT", "R", csv_content)
            if success:
                logger.info(f"Reporte enviado exitosamente en batches: {len(csv_content)} bytes")
            else:
                logger.error("Error enviando reporte en batches")
            
        except Exception as e:
            logger.error(f"Error enviando reporte via protocolo: {e}")

    def _send_error_to_client(self, protocol, error_message):
        """
        Envía un mensaje de error al cliente.
        """
        try:
            # Usar el método público del protocolo para enviar error
            protocol.send_response_message("ERRO", "E", error_message)
        except Exception as e:
            logger.error(f"Error enviando mensaje de error: {e}")

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
