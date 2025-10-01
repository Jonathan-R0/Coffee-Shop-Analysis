import logging
import os
from rabbitmq.middleware import MessageMiddlewareQueue
from strategies import FilterStrategyFactory
from configurators import NodeConfiguratorFactory
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType, FileType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class FilterNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_q1 = os.getenv('OUTPUT_Q1', None)
        self.output_q2 = os.getenv('OUTPUT_Q2', None)
        self.output_q3 = os.getenv('OUTPUT_Q3', None)
        self.output_q4 = os.getenv('OUTPUT_Q4', None)
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        
        total_env_var = f'TOTAL_{self.filter_mode.upper()}_FILTERS'
        self.total_filters = int(os.getenv(total_env_var, '1'))
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Total filtros {self.filter_mode}: {self.total_filters}")
        
        self.filter_strategy = self._create_filter_strategy()
        
        self.node_configurator = NodeConfiguratorFactory.create_configurator(
            self.filter_mode,
            self.rabbitmq_host
        )
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue
        )
        
        self.middlewares = self.node_configurator.create_output_middlewares(
            self.output_q1,
            self.output_q3,
            self.output_q4,
            self.output_q2
        )
        
        if self.filter_mode == 'year':
            self.eof_received_transactions = False
            self.eof_received_transaction_items = False
            logger.info("Nodo year: Habilitado tracking de EOF para ambos tipos")
        else:
            self.eof_received_transactions = False
            self.eof_received_transaction_items = True  # Marcamos como "ya recibido" para que no bloquee
            logger.info(f"Nodo {self.filter_mode}: Solo tracking de EOF para transactions")

    def _create_filter_strategy(self):
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
        try:
            decoded_data = message.decode('utf-8').strip()
            
            if "EOF:" in decoded_data:
                eof_type = None
                if decoded_data.startswith("D:EOF:"):
                    eof_type = "transactions"
                elif decoded_data.startswith("I:EOF:"):
                    eof_type = "transaction_items"
                elif decoded_data.startswith("EOF:"):
                    eof_type = "transactions"
                
                if eof_type:
                    dto = TransactionBatchDTO.from_bytes_fast(message)
                    return self._handle_eof_message(dto, eof_type)
            
            batch_type = "transactions"  
            actual_data = ""
            
            if decoded_data.startswith("D:"):
                actual_data = decoded_data[2:]  
                dto = TransactionBatchDTO(actual_data, BatchType.RAW_CSV)
                batch_type = "transactions"
            elif decoded_data.startswith("I:"):
                actual_data = decoded_data[2:]  
                dto = TransactionItemBatchDTO(actual_data, BatchType.RAW_CSV)
                batch_type = "transaction_items"
            else:
                actual_data = decoded_data
                dto = TransactionBatchDTO(actual_data, BatchType.RAW_CSV)
                batch_type = "transactions"
            
            logger.info(f"Procesando batch_type: {batch_type}")
            
            if hasattr(self.filter_strategy, 'set_dto_helper'):
                self.filter_strategy.set_dto_helper(dto)
            
            filtered_csv = self.filter_strategy.filter_csv_batch(actual_data)
            
            if not filtered_csv.strip():
                return False
            
            processed_data = self.node_configurator.process_filtered_data(filtered_csv)
            
            self.node_configurator.send_data(processed_data, self.middlewares, batch_type)
            
            return False

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def _handle_eof_message(self, dto: TransactionBatchDTO, eof_type: str):
        try:
            eof_data = dto.data.strip()
            if ":" in eof_data:
                parts = eof_data.split(':')
                counter = int(parts[-1])  
            else:
                counter = 1
            
            logger.info(f"EOF recibido para {eof_type} con counter={counter}, total_filters={self.total_filters}")
            
            if eof_type == "transactions":
                self.eof_received_transactions = True
            elif eof_type == "transaction_items":
                self.eof_received_transaction_items = True
            
            logger.info(f"Estado EOF: transactions={self.eof_received_transactions}, transaction_items={self.eof_received_transaction_items}")
            
            if counter < self.total_filters:
                self._forward_eof_to_input(counter + 1, eof_type)
                
            elif counter == self.total_filters:
                if self.filter_mode == 'year':
                    if self.eof_received_transactions and self.eof_received_transaction_items:
                        logger.info("Nodo year: EOF recibido para ambos tipos - enviando EOF downstream")
                        self.node_configurator.send_eof(self.middlewares, "transactions")
                        self.node_configurator.send_eof(self.middlewares, "transaction_items")
                        logger.info("Finalizando procesamiento por EOF completo")
                        return True
                    else:
                        logger.info("Nodo year: EOF recibido pero aún falta el otro tipo - continuando")
                        return False
                else:
                    logger.info(f"Nodo {self.filter_mode}: EOF recibido - enviando EOF downstream")
                    self.node_configurator.send_eof(self.middlewares, "transactions")
                    logger.info("Finalizando procesamiento por EOF")
                    return True
            
            return False
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False

    def _forward_eof_to_input(self, new_counter: int, eof_type: str):
        if eof_type == "transactions":
            eof_message = f"D:EOF:{new_counter}"
        elif eof_type == "transaction_items":
            eof_message = f"I:EOF:{new_counter}"
        else:
            eof_message = f"EOF:{new_counter}"  # Fallback
            
        eof_dto = TransactionBatchDTO(eof_message, batch_type=BatchType.EOF)
        
        input_middleware_sender = MessageMiddlewareQueue(
            host=self.rabbitmq_host,
            queue_name=self.input_queue
        )
        input_middleware_sender.send(eof_dto.to_bytes_fast())
        input_middleware_sender.close()
        
        logger.info(f"{eof_message} reenviado a INPUT queue {self.input_queue}")

    def on_message_callback(self, ch, method, properties, body):
        try:
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")

    def start(self):

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
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión de entrada cerrada")
            
            for name, middleware in self.middlewares.items():
                if middleware:
                    middleware.close()
                    logger.info(f"Middleware {name} cerrado")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    filter_node = FilterNode()
    filter_node.start()