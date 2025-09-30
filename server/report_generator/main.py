import logging
import os
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.report_exchange = os.getenv('REPORT_EXCHANGE', 'report.exchange')
        self.to_gateway_report_exchange = os.getenv('REPORT_EXCHANGE', 'reports_exchange')
        
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.report_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof', 'q4.data', 'q4.eof']
        )
        
        self.csv_files = {}  # Para múltiples archivos
        self.eof_received = set()  # Tracking de EOF
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Exchange: {self.report_exchange}")
        logger.info(f"  Queries soportadas: Q1, Q3, Q4")

    def process_message(self, message: bytes, routing_key: str):
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            query_name = routing_key.split('.')[0]  # 'q1' o 'q3'
            
            if dto.batch_type == BatchType.EOF:
                logger.info(f"EOF recibido para {query_name}")
                self._close_csv_file(query_name)
                self.eof_received.add(query_name)
                
                # Si recibimos EOF de todas las queries, terminar
                if len(self.eof_received) >= 3:  # q1, q3 y q4
                    logger.info("Todos los reportes completados")
                    return True
                
                return False
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._write_to_csv(dto.data, query_name)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        """Callback para RabbitMQ cuando llega un mensaje."""
        try:
            routing_key = method.routing_key  
            should_stop = self.process_message(body, routing_key)  
            
            if should_stop:
                logger.info("Todos los reportes generados - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}")

    def start(self):
        """Inicia el ReportGenerator."""
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
        try:
            # Cerrar cualquier archivo CSV abierto
            for query_name in list(self.csv_files.keys()):
                self._close_csv_file(query_name)
            
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

    def _initialize_csv_file(self, query_name: str, sample_data: str):
        """Inicializa el archivo CSV con headers."""
        try:
            reports_dir = './reports'
            os.makedirs(reports_dir, exist_ok=True)
            os.chmod(reports_dir, 0o755)

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{reports_dir}/{query_name}_{timestamp}.csv"
            
            self.csv_files[query_name] = open(filename, 'w', encoding='utf-8')
            
            # Escribir header desde la primera línea del sample data
            header_line = sample_data.strip().split('\n')[0]
            self.csv_files[query_name].write(header_line + '\n')
            self.csv_files[query_name].flush()
            
            logger.info(f"Archivo CSV inicializado: {filename}")

        except Exception as e:
            logger.error(f"Error inicializando el archivo CSV para {query_name}: {e}")
            raise

    def _write_to_csv(self, csv_data: str, query_name: str):
        """Escribe datos al archivo CSV correspondiente."""
        try:
            # Inicializar archivo si no existe
            if query_name not in self.csv_files:
                self._initialize_csv_file(query_name, csv_data)
            
            # Escribir datos (skip header)
            lines = csv_data.strip().split('\n')
            for line in lines:
                line = line.strip()
                if not line:
                    continue
                
                # Skip headers comunes
                if any(header in line.lower() for header in ['transaction_id', 'year_half', 'store_name,birthdate']):
                    continue
                
                self.csv_files[query_name].write(line + '\n')
            
            self.csv_files[query_name].flush()
            
            processed_lines = len([l for l in lines if l.strip() and not any(h in l.lower() for h in ['transaction_id', 'year_half', 'store_name,birthdate'])])
            if processed_lines > 0:
                logger.info(f"Escritas {processed_lines} líneas en archivo {query_name}")
            
        except Exception as e:
            logger.error(f"Error escribiendo en CSV para {query_name}: {e}")
            raise

    def _close_csv_file(self, query_name: str):
        """Cierra el archivo CSV para una query específica."""
        try:
            if query_name in self.csv_files:
                self.csv_files[query_name].close()
                logger.info(f"Archivo CSV cerrado para {query_name}")
                del self.csv_files[query_name]
        except Exception as e:
            logger.error(f"Error cerrando el archivo CSV para {query_name}: {e}")


if __name__ == "__main__":
    report_generator = ReportGenerator()
    report_generator.start()