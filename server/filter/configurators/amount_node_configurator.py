import logging
from typing import Optional, Tuple
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class AmountNodeConfigurator(NodeConfigurator):
    """
    Configurador para nodos de filtro por cantidad.
    Extrae solo columnas necesarias para Q1 y envía a exchange.
    """
    
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Tuple:
        output_exchange_middleware = None
        if output_q1:
            output_exchange_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q1,
                route_keys=['q1.data', 'q1.eof']
            )
            logger.info(f"  Output Exchange: {output_q1}")
        
        return None, output_exchange_middleware, None, None
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return self._extract_q1_columns(filtered_csv)
    
    def send_data(self, data: str, output_middleware, output_exchange_middleware, 
                 output_middleware_exchange, output_q4_middleware=None):
        if output_exchange_middleware:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            output_exchange_middleware.send(
                filtered_dto.to_bytes_fast(),
                routing_key='q1.data'
            )
    
    def send_eof(self, output_middleware, output_exchange_middleware, 
                output_middleware_exchange, output_q4_middleware=None):
        if output_exchange_middleware:
            eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
            output_exchange_middleware.send(
                eof_dto.to_bytes_fast(),
                routing_key='q1.eof'
            )
            logger.info("EOF:1 enviado a routing key 'q1.eof'")
    
    def _extract_q1_columns(self, csv_data: str) -> str:
        """
        Extrae solo transaction_id y final_amount para Query 1.
        """
        result_lines = ["transaction_id,final_amount"]
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            parts = line.split(',')
            if len(parts) >= 8:
                transaction_id = parts[0]
                final_amount = parts[7]
                result_lines.append(f"{transaction_id},{final_amount}")
        
        logger.info(f"Extraídas {len(result_lines)-1} líneas con columnas Q1")
        return '\n'.join(result_lines)
