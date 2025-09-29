
import logging
import os

from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos import TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.report_exchange = os.getenv('REPORT_EXCHANGE', 'reports_exchange')
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'report.exchange')
        
        # Conexión para recibir datos
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.input_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof']
        )
        self.report_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.report_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof']
        )
        
        # Acumuladores en memoria (listas de strings CSV)
        self.q1_lines = []  # Cada elemento es una línea: "transaction_id,final_amount"
        self.q3_lines = []  # Cada elemento es una línea: "year_half_created_at,store_name,tpv"
        
        # Tracking de EOF
        self.eof_received = set()
        
        logger.info(f"ReportGenerator inicializado")
        logger.info(f"  Exchange: {self.report_exchange}")
        logger.info(f"  RabbitMQ Host: {self.rabbitmq_host}")

    def process_message(self, message: bytes, routing_key: str):
        """Procesa mensajes según el routing key"""
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)

            logger.info(f"Mensaje recibido con routing key: {routing_key}, tipo: {dto.batch_type}, tamaño: {len(dto.data)} bytes")
            # Determinar query desde routing key
            if routing_key.startswith('q1'):
                query_name = 'q1'
            elif routing_key.startswith('q3'):
                query_name = 'q3'
            else:
                logger.warning(f"Routing key desconocido: {routing_key}")
                return False
            
            # Manejar EOF
            if routing_key.endswith('.eof'):
                logger.info(f"EOF recibido para {query_name}")
                self.eof_received.add(query_name)
                
                # Generar y enviar reporte para esta query
                self._generate_and_send_report(query_name)
                
                # Si ya recibimos ambos EOF, terminar
                if len(self.eof_received) >= 2:
                    logger.info("Todos los reportes generados - finalizando")
                    return True
                    
                return False
            
            # Procesar datos (.data)
            if routing_key.endswith('.data'):
                self._accumulate_data(dto.data, query_name)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return False

    def _accumulate_data(self, csv_data: str, query_name: str):
        """Acumula líneas CSV en memoria"""
        try:
            lines = csv_data.strip().split('\n')
            
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Saltar headers
                if 'transaction_id' in line or 'year_half_created_at' in line or 'store_name' in line:
                    continue
                
                # Acumular en la lista correspondiente
                if query_name == 'q1':
                    self.q1_lines.append(line)
                elif query_name == 'q3':
                    self.q3_lines.append(line)
                        
        except Exception as e:
            logger.error(f"Error acumulando datos para {query_name}: {e}")
            raise

    def _generate_and_send_report(self, query_name: str):
        """Genera el reporte final y lo envía al gateway"""
        try:
            logger.info(f"Generando reporte para {query_name}")
            if query_name == 'q1':
                lines = self.q1_lines
                # Q1: Ordenar por transaction_id (primera columna)
                sorted_lines = sorted(lines, key=lambda x: x.split(',')[0])
                logger.info(f"Q1: {len(sorted_lines)} transacciones ordenadas por transaction_id")
            else:  # q3
                # Q3: No ordenar, enviar tal cual
                sorted_lines = self.q3_lines
                logger.info(f"Q3: {len(sorted_lines)} registros (sin ordenar)")
            
            if not sorted_lines:
                logger.warning(f"No hay datos para {query_name}")
                return
            
            # Enviar al gateway
            self._send_report_to_gateway(sorted_lines, query_name)
            
        except Exception as e:
            logger.error(f"Error generando reporte para {query_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def _send_report_to_gateway(self, lines, query_name: str):
        """Envía el reporte al gateway usando exchange con routing keys"""
        try:
            
            batch_size = 150
            total_sent = 0
            
            logger.info(f"{query_name}: Enviando {len(lines)} líneas al gateway")
            
            # Enviar líneas en batches con routing key específico
            while total_sent < len(lines):
                end_idx = min(total_sent + batch_size, len(lines))
                batch_lines = lines[total_sent:end_idx]
                
                csv_batch = '\n'.join(batch_lines)
                dto = TransactionBatchDTO(csv_batch, batch_type="RAW_CSV")
                
                # Enviar con routing key específico de la query
                self.report_middleware.send(dto.to_bytes_fast(), routing_key=f'{query_name}.data')
                
                total_sent = end_idx
                logger.info(f"{query_name}: Batch enviado {total_sent}/{len(lines)}")
            
            # Enviar EOF con routing key específico
            eof_dto = TransactionBatchDTO(f"EOF:{query_name.upper()}", batch_type="EOF")
            self.report_middleware.send(eof_dto.to_bytes_fast(), routing_key=f'{query_name}.eof')
            
            logger.info(f"{query_name}: Reporte enviado completo: {len(lines)} registros")
            # NO cerrar aquí - se cierra en cleanup cuando todo termine

        except Exception as e:
            logger.error(f"Error enviando reporte {query_name}: {e}")
            import traceback
            logger.error(traceback.format_exc())

    def on_message_callback(self, ch, method, properties, body):
        """Callback de RabbitMQ"""
        try:
            routing_key = method.routing_key
            should_stop = self.process_message(body, routing_key)
            
            if should_stop:
                logger.info("Deteniendo consuming")
                ch.stop_consuming()
                
        except Exception as e:
            logger.error(f"Error en callback: {e}")

    def start(self):
        """Inicia el ReportGenerator"""
        logger.info("Iniciando ReportGenerator...")
        
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        """Limpieza de recursos"""
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión de entrada cerrada")
            if hasattr(self, 'report_middleware') and self.report_middleware:
                self.report_middleware.close()
                logger.info("Conexión de reporte cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    report_generator = ReportGenerator()
    report_generator.start()
