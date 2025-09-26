import logging
import os
import json
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareQueue
from filter_factory import FilterStrategyFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_queue = os.getenv('OUTPUT_QUEUE', None)
        
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        
        # Crear la estrategia de filtro usando la factory
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
        logger.info(f"  Estrategia: {self.filter_strategy.get_filter_description() if self.filter_strategy else 'None'}")
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
                config['filter_hours'] = os.getenv('FILTER_HOURS', '06:00-22:59')
            elif self.filter_mode == 'amount':
                config['min_amount'] = float(os.getenv('MIN_AMOUNT', '75'))
            
            return FilterStrategyFactory.create_strategy(self.filter_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia de filtro: {e}")
            return None

    def _should_pass_filter(self, transaction):
        """
        Evalúa si una transacción debe pasar el filtro usando la estrategia configurada.
        
        Args:
            transaction (dict): Datos de la transacción a evaluar
            
        Returns:
            bool: True si la transacción pasa el filtro, False en caso contrario
        """
        try:
            if self.filter_strategy is None:
                logger.warning("No hay estrategia de filtro configurada, dejando pasar la transacción")
                return True
                
            logger.info(f"Aplicando filtro: {self.filter_strategy.get_filter_description()}")
            return self.filter_strategy.should_pass(transaction)
                
        except Exception as e:
            logger.error(f"Error aplicando filtro {self.filter_mode}: {e}")
            return False

    def process_message(self, message: str):
        try:
            transaction = json.loads(message)
            
            # Verificar si es una señal de finalización
            if transaction.get('type') == 'FINISH':
                logger.info(f"Señal de finalización recibida en filtro {self.filter_mode}")
                
                if self.output_middleware:
                    # Propagar la señal al siguiente nodo
                    self.output_middleware.send(message)
                    logger.info(f"Señal de finalización propagada a {self.output_queue}")
                else:
                    logger.info("Señal de finalización recibida en nodo final")
                return
            
            transaction_id = transaction.get("transaction_id", "unknown")
            
            if not self._should_pass_filter(transaction):
                logger.info(f"Transacción rechazada por filtro {self.filter_mode}: {transaction_id}")
                return
            
            logger.info(f"Transacción aprobada por filtro {self.filter_mode}: {transaction_id}")
            
            if self.output_middleware:
                self.output_middleware.send(message)
                logger.info(f"Transacción enviada a {self.output_queue}: {transaction_id}")
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