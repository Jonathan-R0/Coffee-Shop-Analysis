import logging
import os
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from topknode import TopKNode
from best_selling_aggregator import BestSellingAggregatorNode


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopKNodeRunner:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.is_final = os.getenv('TOPK_MODE', 'intermediate') == 'final'
        self.node_id = os.getenv('TOPK_NODE_ID', '1')
        
        self.total_nodes = int(os.getenv('TOTAL_TOPK_NODES', '1'))
        
        self.topk_node = TopKNode(
            is_final=self.is_final, 
            total_nodes=self.total_nodes
        )
        
        self._setup_input_middleware()
        self._setup_output_middleware()
        
        logger.info(f"TopKNodeRunner inicializado:")
        logger.info(f"  Modo: {'Final' if self.is_final else 'Intermedio'}")
        logger.info(f"  Node ID: {self.node_id}")
        if self.is_final:
            logger.info(f"  Esperando {self.total_nodes} nodos TopK intermedios")
    
    def _setup_input_middleware(self):
        if self.is_final:
            input_exchange = os.getenv('INPUT_EXCHANGE', 'topk.exchange')
            
            self.input_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=input_exchange,
                route_keys=['topk.local.data','topk.local.eof']
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Input Routing Keys: 'topk.local.data','topk.local.eof'")
        else:
            input_exchange = os.getenv('INPUT_EXCHANGE', 'aggregated.exchange')
            
            node_id = int(os.getenv('TOPK_NODE_ID', '1'))
            total_nodes = int(os.getenv('TOTAL_TOPK_NODES', '2'))
            
            total_stores = 10  
            stores_per_node = total_stores // total_nodes
            extra_stores = total_stores % total_nodes
            
            start_store = (node_id - 1) * stores_per_node + min(node_id - 1, extra_stores)
            end_store = start_store + stores_per_node + (1 if node_id <= extra_stores else 0)
            
            routing_keys = [f"store.{i}" for i in range(start_store + 1, end_store + 1)]
            routing_keys.append('aggregated.eof')  
            
            self.input_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=input_exchange,
                route_keys=routing_keys
            )
            logger.info(f"  Input Exchange: {input_exchange}")
            logger.info(f"  Node {node_id}/{total_nodes} procesa stores: {start_store + 1}-{end_store}")
            logger.info(f"  Routing Keys: {routing_keys}")
    
    def _setup_output_middleware(self):
        output_exchange = os.getenv('OUTPUT_EXCHANGE', 'topk.exchange')
        
        output_routing_keys = [
            self.topk_node.get_output_routing_key(),
            self.topk_node.get_eof_routing_key()
        ]
        
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=output_routing_keys
        )
        
        if not self.is_final:
            self.input_requeue_middleware = self.input_middleware
        
        logger.info(f"  Output Exchange: {output_exchange}")
        logger.info(f"  Output Routing Keys: {output_routing_keys}")
    
    def _process_csv_batch(self, csv_data: str):
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('store_id,user_id,purchases_qty'):
                continue
            
            self.topk_node.process_csv_line(line)
            processed_count += 1
        
        if processed_count > 0:
            logger.info(f"Procesadas {processed_count} líneas en batch")
    
    def _handle_eof_message(self, dto: TransactionBatchDTO) -> bool:
        try:
            if self.is_final:
                should_send_results = self.topk_node.increment_eof_count()
                
                if should_send_results:
                    logger.info("Generando y enviando resultados TopK")
                    
                    results_csv = self.topk_node.generate_results_csv()
                    
                    result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                    self.output_middleware.send(
                        result_dto.to_bytes_fast(),
                        routing_key=self.topk_node.get_output_routing_key()
                    )
                    
                    eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
                    self.output_middleware.send(
                        eof_dto.to_bytes_fast(),
                        routing_key=self.topk_node.get_eof_routing_key()
                    )
                    
                    logger.info("Resultados TopK enviados")
                    return True
                
                return False
            else:
                eof_data = dto.data.strip()
                counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
                
                logger.info(f"EOF recibido con counter={counter}, total={self.total_nodes}")
                
                results_csv = self.topk_node.generate_results_csv()
                
                logger.info(f"Enviando datos TopK intermedios:")
                logger.info(f"  Longitud: {len(results_csv)} caracteres")
                logger.info(f"  Líneas: {len(results_csv.split(chr(10)))}")
                
                result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
                self.output_middleware.send(
                    result_dto.to_bytes_fast(),
                    routing_key=self.topk_node.get_output_routing_key()
                )
                
                eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
                self.output_middleware.send(
                    eof_dto.to_bytes_fast(),
                    routing_key=self.topk_node.get_eof_routing_key()
                )
                logger.info("EOF enviado a TopK final")
                
                if counter < self.total_nodes:
                    new_counter = counter + 1
                    eof_dto_requeue = TransactionBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                    self.input_requeue_middleware.send(eof_dto_requeue.to_bytes_fast())
                    
                    logger.info(f"EOF:{new_counter} reenviado a input queue para coordinación")
                
                logger.info("Datos TopK enviados - cerrando nodo")
                return True  
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def process_message(self, message: bytes, routing_key: str = None) -> bool:
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                return self._handle_eof_message(dto)
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._process_csv_batch(dto.data)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        try:
            routing_key = getattr(method, 'routing_key', None)
            should_stop = self.process_message(body, routing_key)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        try:
            mode_str = "Final" if self.is_final else "Intermedio"
            logger.info(f"Iniciando TopKNode {mode_str} (ID: {self.node_id})...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("TopKNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Input middleware cerrado")
            
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Output middleware cerrado")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    aggregator_type = os.getenv('AGGREGATOR_TYPE', 'topk')  
    

    if aggregator_type in ['best_selling_intermediate', 'best_selling_final']:

        aggregator = BestSellingAggregatorNode()
    else:
        aggregator = TopKNodeRunner()
    
    aggregator.start()