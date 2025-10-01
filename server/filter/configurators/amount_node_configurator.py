import logging
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class AmountNodeConfigurator(NodeConfigurator):
    
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        logger.info(f"Configurando middlewares de salida para AmountNodeConfigurator {output_q1}")
        if output_q1:
            middlewares['q1'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q1,
                route_keys=['q1.data']
            )
            logger.info(f"  Output Q1 Exchange: {output_q1}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return self._extract_q1_columns(filtered_csv)
    
    def send_data(self, data: str, middlewares: Dict[str, Any]):
        if 'q1' in middlewares:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            middlewares['q1'].send(
                filtered_dto.to_bytes_fast(),
                routing_key='q1.data'
            )
            logger.info(f"Datos enviados a Q1 exchange con routing key 'q1.data'")
    
    def send_eof(self, middlewares: Dict[str, Any]):
        if 'q1' in middlewares:
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            middlewares['q1'].send(
                eof_dto.to_bytes_fast(),
                routing_key='q1.data'
            )
            logger.info("EOF:1 enviado a Q1 exchange con routing key 'q1.data'")

    def _extract_q1_columns(self, csv_data: str) -> str:
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