import logging
import os
from rabbitmq.middleware import MessageMiddlewareQueue
from strategies import FilterStrategyFactory
from configurators import NodeConfiguratorFactory
from dtos.dto import TransactionBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterNode:
    """
    Nodo de filtrado genérico que delega comportamientos específicos
    a configuradores especializados por tipo de filtro.
    """
    
    def __init__(self):
        # Configuración básica
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_q1 = os.getenv('OUTPUT_Q1', None)
        self.output_q3 = os.getenv('OUTPUT_Q3', None)
        self.output_q4 = os.getenv('OUTPUT_Q4', None)
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        
        # Configuración de sincronización de EOF
        total_env_var = f'TOTAL_{self.filter_mode.upper()}_FILTERS'
        self.total_filters = int(os.getenv(total_env_var, '1'))
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Total filtros {self.filter_mode}: {self.total_filters}")
        
        # Crear estrategia de filtro
        self.filter_strategy = self._create_filter_strategy()
        
        # Crear configurador específico del nodo
        self.node_configurator = NodeConfiguratorFactory.create_configurator(
            self.filter_mode,
            self.rabbitmq_host
        )
        
        # Middleware de entrada
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue
        )
        
        # Middlewares de salida (delegados al configurador)
        (self.output_middleware,
         self.output_exchange_middleware,
         self.output_middleware_exchange,
         self.output_q4_middleware) = self.node_configurator.create_output_middlewares(
            self.output_q1,
            self.output_q3,
            self.output_q4
        )

    def _create_filter_strategy(self):
        """
        Crea la estrategia de filtro apropiada basándose en la configuración.
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
            raise

    def process_message(self, message: bytes):
        """
        Procesa mensaje CSV o EOF.
        """
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)

            if dto.batch_type == BatchType.EOF:
                return self._handle_eof_message(dto)
            
            # Filtrar datos
            filtered_csv = self.filter_strategy.filter_csv_batch(dto.data)
            
            if not filtered_csv.strip():
                return False
            
            # Procesar datos filtrados (específico por tipo de nodo)
            processed_data = self.node_configurator.process_filtered_data(filtered_csv)
            
            # Enviar datos (específico por tipo de nodo)
            self.node_configurator.send_data(
                processed_data,
                self.output_middleware,
                self.output_exchange_middleware,
                self.output_middleware_exchange,
                self.output_q4_middleware
            )
            
            return False

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def _handle_eof_message(self, dto: TransactionBatchDTO):
        """
        Maneja mensajes EOF con contador para sincronización.
        """
        try:
            # Extraer el contador del mensaje
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total_filters={self.total_filters}")
            
            if counter < self.total_filters:
                # Reenviar a INPUT queue con counter+1
                self._forward_eof_to_input(counter + 1)
                
            elif counter == self.total_filters:
                # Enviar EOF final a salidas (delegado al configurador)
                self.node_configurator.send_eof(
                    self.output_middleware,
                    self.output_exchange_middleware,
                    self.output_middleware_exchange,
                    self.output_q4_middleware
                )
            
            logger.info("Finalizando procesamiento por EOF")
            return True
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False

    def _forward_eof_to_input(self, new_counter: int):
        """
        Reenvía EOF con nuevo contador a la queue de entrada.
        """
        eof_dto = TransactionBatchDTO(f"EOF:{new_counter}", batch_type=BatchType.EOF)
        
        input_middleware_sender = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue
        )
        input_middleware_sender.send(eof_dto.to_bytes_fast())
        input_middleware_sender.close()
        
        logger.info(f"EOF:{new_counter} reenviado a INPUT queue {self.input_queue}")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje.
        """
        try:
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")

    def start(self):
        """
        Inicia el consumo de mensajes.
        """
        try:
            logger.info("Iniciando consumo de mensajes...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Filtro detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        """
        Limpia recursos al finalizar.
        """
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión de entrada cerrada")
            
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Conexión de salida cerrada")
            
            if self.output_exchange_middleware:
                self.output_exchange_middleware.close()
                logger.info("Exchange de salida cerrado")
            
            if self.output_middleware_exchange:
                self.output_middleware_exchange.close()
                logger.info("Exchange Q3 cerrado")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    filter_node = FilterNode()
    filter_node.start()