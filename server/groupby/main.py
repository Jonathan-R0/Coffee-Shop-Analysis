import logging
import os
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from groupby_strategy import GroupByStrategyFactory

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class GroupByNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.groupby_mode = os.getenv('GROUPBY_MODE', 'tpv')
        
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        self.groupby_strategy = self._create_groupby_strategy()
        
        self._setup_input_middleware()
        
        self.output_middlewares = self.groupby_strategy.setup_output_middleware(
            self.rabbitmq_host,
            self.output_exchange
        )
        
        if self.groupby_mode == 'top_customers':
            self.output_middlewares['input_queue'] = self.input_middleware
        
        logger.info(f"GroupByNode inicializado:")
        logger.info(f"  Modo: {self.groupby_mode}")
        logger.info(f"  Output Exchange: {self.output_exchange}")
    
    def _setup_input_middleware(self):
        if self.groupby_mode == 'tpv':
            self.input_exchange = os.getenv('INPUT_EXCHANGE', 'groupby.join.exchange')
            semester = os.getenv('SEMESTER')
            if semester not in ['1', '2']:
                raise ValueError("SEMESTER debe ser '1' o '2' para modo TPV")
            
            self.input_routing_keys = [f'semester.{semester}', 'eof.all']
            
            self.input_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.input_exchange,
                route_keys=self.input_routing_keys
            )
            
            logger.info(f"  Input Exchange: {self.input_exchange}")
            logger.info(f"  Input Routing Keys: {self.input_routing_keys}")
        
        elif self.groupby_mode == 'top_customers':
            self.input_queue = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
            self.input_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=self.input_queue
                )
            
            logger.info(f"  Input Queue: {self.input_queue} (round-robin)")
    
    def _create_groupby_strategy(self):
        try:
            config = {}
            
            if self.groupby_mode == 'tpv':
                semester = os.getenv('SEMESTER')
                if semester not in ['1', '2']:
                    raise ValueError("SEMESTER debe ser '1' o '2'")
                config['semester'] = semester
            
            elif self.groupby_mode == 'top_customers':
                input_queue = os.getenv('INPUT_QUEUE', 'year_filtered_q4')
                config['input_queue_name'] = input_queue
            
            return GroupByStrategyFactory.create_strategy(self.groupby_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia: {e}")
            raise
    
    def _process_csv_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            self.groupby_strategy.process_csv_line(line)
            processed_count += 1
        
        if processed_count > 0:
            logger.info(f"Procesadas {processed_count} líneas en batch")
    
    def process_message(self, message: bytes) -> bool:
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                return self.groupby_strategy.handle_eof_message(dto, self.output_middlewares)
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._process_csv_batch(dto.data)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        """Callback para RabbitMQ."""
        try:
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        """Inicia el nodo."""
        try:
            logger.info(f"Iniciando GroupByNode en modo {self.groupby_mode}...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("GroupByNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Limpia recursos."""
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            self.groupby_strategy.cleanup_middlewares(self.output_middlewares)
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    node = GroupByNode()
    node.start()