import logging
import os
from typing import Dict
from rabbitmq.middleware import MessageMiddlewareExchange, MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from collections import defaultdict


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class TopKNode:
    
    def __init__(self, is_final: bool = False, total_nodes: int = 1):
        super().__init__()
        self.is_final = is_final
        self.total_nodes = total_nodes
        self.eof_count = 0
        
        self.store_user_purchases: Dict[str, Dict[str, int]] = defaultdict(
            lambda: defaultdict(int)
        )
        
        mode = "top_final" if is_final else "top_local"
        logger.info(f"Top inicializada en modo {mode}")
        if is_final:
            logger.info(f"  Esperando {total_nodes} top nodes")
    
    def process_csv_line(self, csv_line: str):
        try:
            parts = csv_line.split(',')
            if len(parts) < 3:
                return
            
            if parts[0] == 'store_id':
                return
            
            store_id = parts[0]
            user_id = parts[1]
            purchases_qty = int(parts[2])
            
            self.store_user_purchases[store_id][user_id] += purchases_qty
            
            # Print para debugging en modo final
            if self.is_final:
                print(f"[TOPK FINAL] Procesando: Store {store_id}, User {user_id}, Qty {purchases_qty}")
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea: {e}")
    
    def increment_eof_count(self) -> bool:
        if not self.is_final:
            return True
        
        self.eof_count += 1
        logger.info(f"EOF recibido: {self.eof_count}/{self.total_nodes}")
        
        # Print para modo final
        print(f"[TOPK FINAL] EOF recibido {self.eof_count}/{self.total_nodes}")
        if self.eof_count >= self.total_nodes:
            print(f"[TOPK FINAL] Todos los nodos TopK intermedios han terminado. Procesando resultado final...")
        
        return self.eof_count >= self.total_nodes
    
    def generate_results_csv(self) -> str:
        if not self.store_user_purchases:
            result = "store_id,user_id,purchases_qty"
            if self.is_final:
                print(f"[TOPK FINAL] Resultado vacío enviado:")
                print(result)
            return result
        
        csv_lines = ["store_id,user_id,purchases_qty"]
        
        for store_id in sorted(self.store_user_purchases.keys()):
            user_purchases = self.store_user_purchases[store_id]
            
            # Ordenar por purchases_qty (descendente)
            sorted_users = sorted(
                user_purchases.items(),
                key=lambda x: x[1],
                reverse=True
            )
            
            # Tomar top 3
            top_3 = sorted_users[:3]
            
            for user_id, purchases_qty in top_3:
                csv_lines.append(f"{store_id},{user_id},{purchases_qty}")
        
        result = '\n'.join(csv_lines)
        logger.info(f"Top 3 calculado para {len(self.store_user_purchases)} tiendas")
        
        # Print detallado para modo final
        if self.is_final:
            print(f"\n[TOPK FINAL] Resultado final enviado al JOIN:")
            print("="*50)
            print(result)
            print("="*50)
            print(f"Total líneas: {len(csv_lines)}")
            print(f"Tiendas procesadas: {len(self.store_user_purchases)}")
            for store_id in sorted(self.store_user_purchases.keys()):
                user_count = len(self.store_user_purchases[store_id])
                print(f"  Store {store_id}: {user_count} usuarios únicos")
        
        return result
    
    def get_output_routing_key(self) -> str:
        if self.is_final:
            return 'top_customers.data'  # Para el join
        return 'topk.local.data'
    
    def get_eof_routing_key(self) -> str:
        if self.is_final:
            return 'top_customers.eof'   # Para el join
        return 'topk.local.eof'