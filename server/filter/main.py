import logging
import os
import json
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareQueue
from filter_factory import FilterStrategyFactory
from dtos.dto import TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_queue = os.getenv('OUTPUT_QUEUE', None)
        
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        
        self.filter_strategy = self._create_filter_strategy()
        
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

    def _create_filter_strategy(self):
        """
        Crea la estrategia de filtro apropiada basándose en la configuración.
        
        Returns:
            FilterStrategy: Instancia de la estrategia correspondiente
        """
        try:
            config = {}
            
            if self.filter_mode == 'year':
                config['filter_years'] = os.getenv('FILTER_YEARS', '2024,2025')
            elif self.filter_mode == 'hour':
                config['filter_hours'] = os.getenv('FILTER_HOURS', '06:00-23:00')
            elif self.filter_mode == 'amount':
                config['min_amount'] = float(os.getenv('MIN_AMOUNT', '75'))
            
            return FilterStrategyFactory.create_strategy(self.filter_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia de filtro: {e}")
            return None

    def process_message(self, message: bytes):
        """
        Procesa un batch de transacciones o una señal de control.
        """
        try:
            dto = TransactionBatchDTO.from_bytes(message)

            if dto.batch_type == "CONTROL":
                logger.info("Señal de finalización recibida. Propagando al siguiente nodo.")
                if self.output_middleware:
                    self.output_middleware.send(message)
                return

            filtered_transactions = self.filter_strategy.filter_batch(dto.transactions)

            if not filtered_transactions:
                return

            filtered_dto = TransactionBatchDTO(filtered_transactions)

            serialized_data = filtered_dto.to_bytes()

            if self.output_middleware:
                self.output_middleware.send(serialized_data)

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}. Mensaje recibido: {message}")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje.
        """
        try:
            self.process_message(body)
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}. ")

    def start(self):
        try:
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