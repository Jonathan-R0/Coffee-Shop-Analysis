import time
import socket
import os
import json
import signal
import sys
from common.protocol import Protocol, ProtocolMessage 
from common.new_protocolo import ProtocolNew
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dataclasses import asdict
from logger import get_logger
from dtos.dto import TransactionBatchDTO, BatchType, StoreBatchDTO,UserBatchDTO, ReportBatchDTO

logger = get_logger(__name__)

class Gateway:

    def __init__(self, port, listener_backlog, rabbitmq_host, output_queue, store_exchange,reports_exchange=None):
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listener_backlog)
        self._is_running = False
        self._middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=output_queue)
        self._reports_exchange = reports_exchange
        self._join_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=store_exchange,
            route_keys=['stores.data', 'stores.eof','users.data','users.eof']
        )
        
        self.report_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=self._reports_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof', 'q4.data', 'q4.eof']  # Las 3 queries activas
        )

    def handle_connection(self, client_sock):
        """Maneja una conexión entrante."""
        logger.info(f"Conexión aceptada desde {client_sock.getpeername()}")
        protocol = ProtocolNew(client_sock)

        try:
            for message in protocol.receive_messages():
                logger.info(f"Received message: {message.action} {message.file_type}, size: {message.size}, last_batch: {message.last_batch}")

                if message.action == "EXIT":
                    logger.info(f"EXIT received")
                    self._wait_and_send_report(protocol)
                    break
                
                elif message.action == "FINISH": 
                    logger.info(f"FINISH received for file_type: {message.file_type}")
                    self._handle_finish(message.file_type)
                
                elif message.action == "BATCH":
                    if message.file_type == "D":
                        self.process_type_d_message(message)
                    elif message.file_type == "S":
                        self.process_type_s_message(message)
                    elif message.file_type == "U":
                        self.process_type_u_message(message)
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
                self._join_middleware.send(eof_dto.to_bytes_fast(), routing_key='stores.eof')
                logger.info("EOF:1 enviado para tipo S (stores)")

            elif file_type == "U":
                # EOF para users
                eof_dto = UserBatchDTO("EOF:1", batch_type=BatchType.EOF)
                self._join_middleware.send(eof_dto.to_bytes_fast(), routing_key='users.eof')
                logger.info("EOF:1 enviado para tipo U (users)")

        except Exception as e:
            logger.error(f"Error manejando FINISH: {e}")
            
    def process_type_d_message(self, message: ProtocolMessage):
        """
        Procesa un mensaje de tipo 'D', utilizando el DTO para manejar los datos en formato binario.
        """
        try:
            # logger.info(f"Mensaje tipo D recibido, {message.data}")
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


            self._join_middleware.send(serialized_data, routing_key='stores.data')


            line_count = len([line for line in dto.data.split('\n') if line.strip()])
            logger.info(f"Stores enviados a store_data queue: Líneas: {line_count}")
            
        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'U': {e}")
            
    def process_type_u_message(self, message: ProtocolMessage):
        """
        Procesa un mensaje de tipo 'U' (stores), utilizando UserBatchDTO.
        """
        try:
            bytes_data = message.data.encode('utf-8')
            dto = UserBatchDTO.from_bytes_fast(bytes_data)
            serialized_data = dto.to_bytes_fast()


            self._join_middleware.send(serialized_data, routing_key='users.data')


            line_count = len([line for line in dto.data.split('\n') if line.strip()])
            logger.info(f"Users enviados a users.data queue: Líneas: {line_count}")

        except Exception as e:
            logger.error(f"Error procesando mensaje de tipo 'U': {e}")

    def _wait_and_send_report(self, protocol):
        """Espera por TODOS los reportes (Q1, Q3, Q4) del report generator y los envía al cliente."""
        try:
            logger.info("Esperando reportes del pipeline...")
            
            # Recopilar datos de reportes
            report_data = self._collect_reports_from_pipeline()
            
            # Enviar reportes al cliente
            self._send_reports_to_client(protocol, report_data)
            
            self.report_middleware.close()
            
        except Exception as e:
            logger.error(f"Error esperando reportes: {e}")
            self._send_error_to_client(protocol, f"Error processing reports: {e}")

    def _collect_reports_from_pipeline(self):
        """Recopila todos los reportes del pipeline."""
        report_data = {
            'q1': [],
            'q3': [],
            'q4': []
        }
        eof_count = 0
        
        def report_callback(ch, method, properties, body):
            nonlocal eof_count
            
            try:
                dto = ReportBatchDTO.from_bytes_fast(body)
                routing_key = method.routing_key
                query_name = routing_key.split('.')[0]
                
                logger.info(f"Recibido mensaje para {query_name}: {dto.batch_type}, routing: {routing_key}")
                
                if routing_key.endswith('.eof'):
                    eof_count += 1
                    logger.info(f"EOF recibido para {query_name}. Total EOF: {eof_count}")
                    
                    # DEBUG: ¿Recibimos datos de Q4 antes del EOF?
                    if query_name == "q4":
                        logger.info(f"Q4 EOF recibido - datos acumulados: {len(report_data['q4'])}")
                    
                    
                    if eof_count >= 3:
                        logger.info("Todos los reportes recibidos completamente")
                        ch.stop_consuming()
                    return
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_report_batch(dto.data, query_name, report_data)
                    logger.info(f"Batch procesado: Q1={len(report_data['q1'])}, Q3={len(report_data['q3'])}, Q4={len(report_data['q4'])}")
                    
            except Exception as e:
                logger.error(f"Error procesando batch del reporte: {e}")
        
        # Consumir batches hasta recibir los 3 EOF
        self.report_middleware.start_consuming(report_callback)
        return report_data

    def _process_report_batch(self, data, query_name, report_data):
        """Procesa un batch de datos para una query específica."""
        lines = data.strip().split('\n')
        
        for line in lines:
            if not line.strip():
                continue
                
            values = line.split(',')
            
            if query_name == "q1" and len(values) >= 2:
                report_data['q1'].append({
                    "transaction_id": values[0],
                    "final_amount": values[1]
                })
            elif query_name == "q3" and len(values) >= 3:
                report_data['q3'].append({
                    "year_half": values[0],
                    "store_name": values[1],
                    "tpv": values[2]
                })
            elif query_name == "q4" and len(values) >= 2:
                report_data['q4'].append({
                    "store_name": values[0],
                    "birthdate": values[1],
                })

    def _send_reports_to_client(self, protocol, report_data):
        """Envía todos los reportes al cliente."""
        # Mapeo de queries a convertidores y nombres
        reports_config = [
            ('q1', self._convert_q1_to_csv, "Q1", "transacciones"),
            ('q3', self._convert_q3_to_csv, "Q3", "registros"),
            ('q4', self._convert_q4_to_csv, "Q4", "registros")
        ]
        
        for query_key, converter_func, report_name, unit_name in reports_config:
            transactions = report_data[query_key]
            
            if transactions:
                csv_content = converter_func(transactions)
                self._send_report_via_protocol(protocol, csv_content, report_name)
                logger.info(f"Reporte {report_name} enviado: {len(transactions)} {unit_name}")
            else:
                logger.warning(f"No se encontraron datos para {report_name}")
            
    def _convert_q1_to_csv(self, transactions):
        """
        Convierte lista de transacciones a formato CSV SIN HEADERS.
        """
        try:
            csv_lines = []
            for transaction in transactions:
                csv_lines.append(f"{transaction['transaction_id']},{transaction['final_amount']}")
            
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo transacciones a CSV: {e}")
            return "ERROR,0"

    def _convert_q3_to_csv(self, records):
        """Convierte registros Q3 a formato CSV SIN HEADERS."""
        try:
            csv_lines = []
            for record in records:
                csv_lines.append(f"{record['year_half']},{record['store_name']},{record['tpv']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q3 a CSV: {e}")
            return "ERROR,ERROR,0"

    def _convert_q4_to_csv(self, records):
        """Convierte registros Q4 a formato CSV SIN HEADERS."""
        try:
            csv_lines = []
            for record in records:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            return '\n'.join(csv_lines)
        except Exception as e:
            logger.error(f"Error convirtiendo Q4 a CSV: {e}")
            return "ERROR,0,0"

    def _send_report_via_protocol(self, protocol, csv_content, report_name="RPRT"):
        """Envía un reporte usando el protocolo existente."""
        try:
            success = protocol.send_response_batches(f"RPRT_{report_name}", "R", csv_content)
            if success:
                logger.info(f"Reporte {report_name} enviado exitosamente: {len(csv_content)} bytes")
            else:
                logger.error(f"Error enviando reporte {report_name}")
        except Exception as e:
            logger.error(f"Error enviando reporte {report_name}: {e}")

    # def _send_report_via_protocol(self, protocol, csv_content):
    #     """
    #     Envía el reporte usando el protocolo existente en batches.
    #     """
    #     try:
    #         # Usar el método de batches del protocolo para enviar respuesta
    #         success = protocol.send_response_batches("RPRT", "R", csv_content)
    #         if success:
    #             logger.info(f"Reporte enviado exitosamente en batches: {len(csv_content)} bytes")
    #         else:
    #             logger.error("Error enviando reporte en batches")
            
    #     except Exception as e:
    #         logger.error(f"Error enviando reporte via protocolo: {e}")

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
        if self._join_middleware:
            self._join_middleware.close()
            logger.info("Conexión con RabbitMQ cerrada.")
        logger.info("Servidor detenido correctamente.")
        sys.exit(0)
