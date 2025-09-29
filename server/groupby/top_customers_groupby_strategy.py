import logging
import os
from collections import defaultdict
from typing import Dict
from base_strategy import GroupByStrategy
from user_purchase_count import UserPurchaseCount
from dtos.dto import TransactionBatchDTO, BatchType

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    """
    Estrategia para agrupar compras por tienda y usuario.
    Múltiples instancias procesan datos en round-robin desde una queue.
    """
    
    def __init__(self, input_queue_name: str):
        super().__init__()
        self.input_queue_name = input_queue_name
        self.store_user_purchases: Dict[str, Dict[str, UserPurchaseCount]] = defaultdict(
            lambda: defaultdict(UserPurchaseCount)
        )
        
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '3'))
        
        logger.info(f"TopCustomersGroupByStrategy inicializada")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
        logger.info(f"  Input queue: {self.input_queue_name}")
    
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        """Configura solo queue de salida."""
        from rabbitmq.middleware import MessageMiddlewareQueue
        
        output_queue = MessageMiddlewareQueue(
            host=rabbitmq_host,
            queue_name='aggregated_data'
        )
        
        logger.info(f"  Output queue: aggregated_data")
        
        return {
            "output_queue": output_queue
        }
    
    def handle_eof_message(self, dto: TransactionBatchDTO, middlewares: dict) -> bool:
        """
        Maneja EOF con sincronización estilo FilterNode.
        Solo un nodo recibe EOF de la queue, lo propaga a los demás.
        TODOS los nodos se cierran después de procesar sus datos.
        """
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
            
            # Enviar datos agregados de ESTE nodo
            results_csv = self.generate_results_csv()
            
            logger.info(f"Enviando datos agregados:")
            logger.info(f"  Longitud: {len(results_csv)} caracteres")
            logger.info(f"  Líneas: {len(results_csv.split(chr(10)))}")
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            middlewares["output_queue"].send(result_dto.to_bytes_fast())
            
            # Solo reenviar EOF si no es el último
            if counter < self.total_groupby_nodes:
                new_counter = counter + 1
                eof_dto = TransactionBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                middlewares["input_queue"].send(eof_dto.to_bytes_fast())
                
                logger.info(f"EOF:{new_counter} reenviado a input queue {self.input_queue_name}")
            else:
                # El último nodo envía UN solo EOF a TopK intermedios (ellos se coordinan)
                eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
                middlewares["output_queue"].send(eof_dto.to_bytes_fast())
                logger.info("EOF enviado a TopK intermedios (último nodo)")
            
            logger.info("Datos enviados - cerrando nodo")
            return True  # SIEMPRE cerrar después de procesar EOF
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def process_csv_line(self, csv_line: str):
        """Procesa una línea CSV: cuenta compras por (store_id, user_id)."""
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            
            if not store_id or not user_id or user_id.strip() == '':
                return
            
            if user_id not in self.store_user_purchases[store_id]:
                self.store_user_purchases[store_id][user_id] = UserPurchaseCount(user_id)
            
            self.store_user_purchases[store_id][user_id].add_purchase()
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")
    
    def generate_results_csv(self) -> str:
        """Genera CSV con TODOS los pares (store_id, user_id, count) de ESTE nodo."""
        if not self.store_user_purchases:
            logger.warning("No hay datos locales para generar")
            return "store_id,user_id,purchases_qty"
        
        csv_lines = ["store_id,user_id,purchases_qty"]
        
        for store_id in sorted(self.store_user_purchases.keys()):
            user_purchases = self.store_user_purchases[store_id]
            
            for user_purchase in user_purchases.values():
                csv_lines.append(user_purchase.to_csv_line(store_id))
        
        total_records = len(csv_lines) - 1
        logger.info(f"Datos locales generados: {total_records} registros")
        return '\n'.join(csv_lines)
    
    def get_output_routing_key(self) -> str:
        return 'aggregated.data'
    
    def get_eof_routing_key(self) -> str:
        return 'aggregated.eof'