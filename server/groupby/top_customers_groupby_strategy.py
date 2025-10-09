import logging
import os
from collections import defaultdict
from typing import Dict
from base_strategy import GroupByStrategy
from user_purchase_count import UserPurchaseCount
from dtos.dto import TransactionBatchDTO, BatchType
from rabbitmq.middleware import MessageMiddlewareExchange

logger = logging.getLogger(__name__)


class TopCustomersGroupByStrategy(GroupByStrategy):
    def __init__(self, input_queue_name: str):
        super().__init__()
        self.input_queue_name = input_queue_name
        self.client_states: Dict[int, Dict[str, Dict[str, UserPurchaseCount]]] = {}
        self.total_groupby_nodes = int(os.getenv('TOTAL_GROUPBY_NODES', '3'))
        
        logger.info(f"TopCustomersGroupByStrategy inicializada")
        logger.info(f"  Total nodos: {self.total_groupby_nodes}")
        logger.info(f"  Input queue: {self.input_queue_name}")
    
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        output_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=['store.*', 'aggregated.eof'] 
        )
        
        logger.info(f"  Output exchange: {output_exchange}")
        logger.info(f"  Routing pattern: store.* (por store_id)")
        
        return {"output": output_middleware}
    
    def _get_state(self, client_id: int) -> Dict[str, Dict[str, UserPurchaseCount]]:
        if client_id not in self.client_states:
            self.client_states[client_id] = {}
        return self.client_states[client_id]

    def process_csv_line(self, csv_line: str, client_id: int):
        try:
            store_user_purchases = self._get_state(client_id)
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            user_id = self.dto_helper.get_column_value(csv_line, 'user_id')
            
            if not store_id or not user_id or user_id.strip() == '':
                return

            store_data = store_user_purchases.setdefault(store_id, {})
            if user_id not in store_data:
                store_data[user_id] = UserPurchaseCount(user_id)
            store_data[user_id].add_purchase()
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")
    
    def generate_results_csv(self, store_user_purchases: Dict[str, Dict[str, UserPurchaseCount]]) -> str:
        if not store_user_purchases:
            logger.warning("No hay datos locales para generar")
            return "store_id,user_id,purchases_qty"
        
        csv_lines = ["store_id,user_id,purchases_qty"]
        
        for store_id in sorted(store_user_purchases.keys()):
            user_purchases = store_user_purchases[store_id]
            for user_purchase in user_purchases.values():
                csv_lines.append(user_purchase.to_csv_line(store_id))
        
        total_records = len(csv_lines) - 1
        logger.info(f"Datos locales generados: {total_records} registros")
        return '\n'.join(csv_lines)
    
    def handle_eof_message(self, dto: TransactionBatchDTO, middlewares: dict, client_id: int) -> bool:
        try:
            eof_data = dto.data.strip()
            counter = int(eof_data.split(':')[1]) if ':' in eof_data else 1
            
            logger.info(f"EOF recibido con counter={counter}, total={self.total_groupby_nodes}")
            
            store_user_purchases = self.client_states.get(client_id, {})
            headers = {'client_id': client_id}
            self._send_data_by_store(middlewares["output"], store_user_purchases, headers)
            
            if counter < self.total_groupby_nodes:
                new_counter = counter + 1
                eof_dto = TransactionBatchDTO(f"EOF:{new_counter}", BatchType.EOF)
                middlewares["input_queue"].send(eof_dto.to_bytes_fast(), headers=headers)
                logger.info(f"EOF:{new_counter} reenviado a input queue")
            else:
                eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
                middlewares["output"].send(
                    eof_dto.to_bytes_fast(),
                    'aggregated.eof',
                    headers=headers,
                )
                logger.info("EOF enviado a TopK intermedios (último nodo)")
                
                logger.info("EOF enviado a todas las stores - cerrando")
                if client_id in self.client_states:
                    del self.client_states[client_id]
                    logger.debug(f"Estado TopCustomers limpiado para cliente {client_id}")
            
            return True
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def _send_data_by_store(self, output_middleware, store_user_purchases: Dict[str, Dict[str, UserPurchaseCount]], headers):
        if not store_user_purchases:
            logger.warning("No hay datos locales para enviar")
            return
        
        total_stores = len(store_user_purchases)
        logger.info(f"Enviando datos de {total_stores} stores")
        
        for store_id in sorted(store_user_purchases.keys()):
            store_csv_lines = ["store_id,user_id,purchases_qty"]
            user_purchases = store_user_purchases[store_id]
            
            for user_purchase in user_purchases.values():
                store_csv_lines.append(user_purchase.to_csv_line(store_id))
            
            store_csv = '\n'.join(store_csv_lines)
            routing_key = f"store.{store_id}"
            
            result_dto = TransactionBatchDTO(store_csv, BatchType.RAW_CSV)
            output_middleware.send(result_dto.to_bytes_fast(), routing_key, headers=headers)
            
            logger.info(f"Store {store_id}: {len(store_csv_lines)-1} users → '{routing_key}'")