import logging
import os
import json
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_queue = os.getenv('OUTPUT_QUEUE', None)
        
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        self.filter_years = os.getenv('FILTER_YEARS', '2024,2025').split(',')
        self.filter_hours = os.getenv('FILTER_HOURS', '06:00-23:00')
        self.min_amount = float(os.getenv('MIN_AMOUNT', '75'))
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.input_queue
        )
        
        self.output_middleware = None
        if self.output_queue:
            self.output_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host, 
                queue_name=self.output_queue
            )
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Queue salida: {self.output_queue}")

    def _should_pass_filter(self, transaction):
        try:
            if self.filter_mode == 'year':
                return self._filter_by_year(transaction)
            elif self.filter_mode == 'hour':
                return self._filter_by_hour(transaction)
            elif self.filter_mode == 'amount':
                return self._filter_by_amount(transaction)
            else:
                logger.warning(f"Modo de filtro desconocido: {self.filter_mode}")
                return True
                
        except Exception as e:
            logger.error(f"Error aplicando filtro {self.filter_mode}: {e}")
            return False

    def _filter_by_year(self, transaction):
        logger.info(f"Aplicando filtro por año: {self.filter_years}")
        
        transaction_time = datetime.strptime(transaction.get("created_at"), "%Y-%m-%d %H:%M:%S")
        year_passes = str(transaction_time.year) in self.filter_years
        
        if year_passes:
            logger.info(f"Year filter PASS: {transaction.get('transaction_id')} ({transaction_time.year})")
        else:
            logger.info(f"Year filter REJECT: {transaction.get('transaction_id')} ({transaction_time.year})")
            
        return year_passes

    def _filter_by_hour(self, transaction):
        logger.debug(f"Aplicando filtro por hora: {self.filter_hours}")
        
        transaction_time = datetime.strptime(transaction.get("created_at"), "%Y-%m-%d %H:%M:%S")
        
        start_hour, end_hour = self.filter_hours.split('-')
        start_time = datetime.strptime(start_hour, "%H:%M").time()
        end_time = datetime.strptime(end_hour, "%H:%M").time()
        
        hour_passes = start_time <= transaction_time.time() <= end_time
        
        if hour_passes:
            logger.debug(f"Hour filter PASS: {transaction.get('transaction_id')} ({transaction_time.time()})")
        else:
            logger.debug(f"Hour filter REJECT: {transaction.get('transaction_id')} ({transaction_time.time()})")
            
        return hour_passes

    def _filter_by_amount(self, transaction):
        logger.debug(f"Aplicando filtro por monto mínimo: ${self.min_amount}")
        
        transaction_amount = float(transaction.get("subtotal", 0))
        amount_passes = transaction_amount >= self.min_amount
        
        if amount_passes:
            logger.debug(f"Amount filter PASS: {transaction.get('transaction_id')} (${transaction_amount})")
        else:
            logger.debug(f"Amount filter REJECT: {transaction.get('transaction_id')} (${transaction_amount})")
            
        return amount_passes

    def process_message(self, message: str):
        try:
            transaction = json.loads(message)
            transaction_id = transaction.get("transaction_id", "unknown")
            
            if not self._should_pass_filter(transaction):
                logger.debug(f"Transacción rechazada por filtro {self.filter_mode}: {transaction_id}")
                return
            
            logger.info(f"Transacción aprobada por filtro {self.filter_mode}: {transaction_id}")
            
            if self.output_middleware:
                self.output_middleware.send(message)
                logger.debug(f"Transacción enviada a {self.output_queue}: {transaction_id}")
            else:
                logger.info(f"RESULTADO FINAL - Transacción completamente filtrada: {transaction_id}")
                logger.info(f"  Datos: {transaction}")
                
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON: {e}")
        except KeyError as e:
            logger.error(f"Campo faltante en transacción: {e}")
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje
        
        Args:
            ch: Canal de RabbitMQ
            method: Información del método
            properties: Propiedades del mensaje
            body: Cuerpo del mensaje
        """
        message = body.decode('utf-8')
        self.process_message(message)

    def start(self):
        try:
            logger.info(f"Iniciando FilterNode en modo '{self.filter_mode}'")
            logger.info(f"Consumiendo de: {self.input_queue}")
            if self.output_queue:
                logger.info(f"Publicando en: {self.output_queue}")
            else:
                logger.info("Nodo final del pipeline")
                
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("Filtro detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión de entrada cerrada")
                
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Conexión de salida cerrada")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

if __name__ == "__main__":
    filter_node = FilterNode()
    filter_node.start()