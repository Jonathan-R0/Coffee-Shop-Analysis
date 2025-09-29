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
from dtos    import TransactionBatchDTO, BatchType, StoreBatchDTO

logger = get_logger(__name__)

class Gateway:

    def __init__(self, port, listener_backlog, rabbitmq_host, output_queue, store_exchange,reports_exchange=None):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self._middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue)
        self._reports_exchange = reports_exchange
        self._store_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=store_exchange,
            route_keys=['stores.data', 'stores.eof']
        )
        
        self.report_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=self._reports_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof']
        )

    def handle_connection(self, client_sock):
        """Maneja una conexión entrante."""
        logger.info(f"Conexión aceptada desde {client_sock.getpeername()}")
        protocol = Protocol(client_sock)

        should_send_report = False
        
        try:
            for message in protocol.receive_messages():
                logger.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")

                if message.action == "EXIT":
                    should_send_report = True
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
            # Enviar reporte si fue solicitado después del ACK
            if should_send_report:
                self._wait_and_send_report(protocol)
                # Dar tiempo para que el cliente reciba el reporte completo
                import time
                time.sleep(2)
                
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

    def _wait_and_send_report(self, protocol):
        """
        Espera por reportes del report generator y los envía al cliente tan pronto como estén listos.
        """
        try:
            logger.info("Esperando reportes del pipeline...")
            
            # Acumuladores para cada reporte
            q1_transactions = []
            q3_transactions = []
            eof_received = set()
            reports_sent = set()
            
            def report_callback(ch, method, properties, body):
                nonlocal eof_received, reports_sent
                
                try:
                    dto = TransactionBatchDTO.from_bytes_fast(body)
                    routing_key = method.routing_key
                    
                    logger.info(f"Recibido mensaje con routing_key: {routing_key}, batch_type: {dto.batch_type}")
                    
                    # Detectar EOF y enviar reporte inmediatamente si está listo
                    if dto.batch_type == BatchType.EOF:
                        logger.info(f"Procesando EOF para routing_key: {routing_key}")
                        if routing_key == "q1.eof":
                            eof_received.add("q1")
                            logger.info("EOF recibido para Q1")
                            
                            # Enviar Q1 inmediatamente
                            if "q1" not in reports_sent and q1_transactions:
                                raw_q1_data = '\n'.join(q1_transactions)
                                self._send_report_via_protocol(protocol, raw_q1_data, "Q1")
                                reports_sent.add("q1")
                                logger.info(f"Reporte Q1 enviado inmediatamente: {len(q1_transactions)} chunks")
                            else:
                                logger.warning(f"Q1 no enviado: ya enviado={'q1' in reports_sent}, chunks_vacios={not q1_transactions}")
                            
                        elif routing_key == "q3.eof":
                            eof_received.add("q3")
                            logger.info("EOF recibido para Q3")
                            
                            # Enviar Q3 inmediatamente
                            if "q3" not in reports_sent and q3_transactions:
                                raw_q3_data = '\n'.join(q3_transactions)
                                self._send_report_via_protocol(protocol, raw_q3_data, "Q3")
                                reports_sent.add("q3")
                                logger.info(f"Reporte Q3 enviado inmediatamente: {len(q3_transactions)} chunks")
                        
                        # Solo terminar cuando tengamos ambos EOFs, no solo Q1
                        if "q1" in eof_received and "q3" in eof_received:
                            logger.info("Ambos reportes procesados completamente")
                            self.report_middleware.stop_consuming()
                            return
                        
                        # Continuar escuchando si solo tenemos uno de los dos
                        if "q1" in eof_received:
                            logger.info("Q1 procesado, continuando escucha para Q3...")
                        elif "q3" in eof_received:
                            logger.info("Q3 procesado, continuando escucha para Q1...")
                        
                        # No hacer return aquí para seguir escuchando
                    
                    # Procesar datos según routing key - solo acumular datos sin transformar
                    elif dto.batch_type == BatchType.RAW_CSV:
                        if routing_key == "q1.data":
                            q1_transactions.append(dto.data)
                        elif routing_key == "q3.data":
                            q3_transactions.append(dto.data)
                        
                        logger.info(f"Batch procesado: Q1 chunks={len(q1_transactions)}, Q3 chunks={len(q3_transactions)}")
                    
                except Exception as e:
                    logger.error(f"Error procesando batch del reporte: {e}")
            
            # Consumir batches
            self.report_middleware.start_consuming(report_callback)
            
            # Verificar si hay reportes que no se enviaron (por si acaso)
            if "q1" not in reports_sent and q1_transactions:
                raw_q1_data = '\n'.join(q1_transactions)
                self._send_report_via_protocol(protocol, raw_q1_data, "Q1")
                logger.info(f"Reporte Q1 enviado al final: {len(q1_transactions)} chunks")
            
            if "q3" not in reports_sent and q3_transactions:
                raw_q3_data = '\n'.join(q3_transactions)
                self._send_report_via_protocol(protocol, raw_q3_data, "Q3")
                logger.info(f"Reporte Q3 enviado al final: {len(q3_transactions)} chunks")
            
            self.report_middleware.close()
            logger.info("Todos los reportes han sido enviados")
            
        except Exception as e:
            logger.error(f"Error esperando reportes: {e}")
            self._send_error_to_client(protocol, f"Error processing reports: {e}")

    def _send_report_via_protocol(self, protocol, content, report_name="RPRT"):
        """Envía un reporte usando el protocolo existente."""
        try:
            # Limpiar buffer antes de enviar reporte
            protocol._flush_socket_buffer()
            
            success = protocol.send_response_batches("RPRT", "R", content)
            if success:
                logger.info(f"Reporte {report_name} enviado exitosamente: {len(content)} bytes")
            else:
                logger.error(f"Error enviando reporte {report_name}")
        except Exception as e:
            logger.error(f"Error enviando reporte {report_name}: {e}")

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
        if self._store_middleware:
            self._store_middleware.close()
            logger.info("Conexión con RabbitMQ cerrada.")
        logger.info("Servidor detenido correctamente.")
        sys.exit(0)