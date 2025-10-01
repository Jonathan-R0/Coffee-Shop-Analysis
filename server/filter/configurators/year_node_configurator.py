import logging
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, TransactionItemBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class YearNodeConfigurator(NodeConfigurator):
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None, output_q2: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        
        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q2:
            middlewares['q2'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q2,
                route_keys=['2024', '2025']
            )
            logger.info(f"  Output Q2 Exchange: {output_q2}")
        
        if output_q4:
            middlewares['q4'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q4
            )
            logger.info(f"  Output Q4 Queue: {output_q4}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, middlewares: Dict[str, Any], batch_type: str = "transactions"):
        if batch_type == "transactions":
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(filtered_dto.to_bytes_fast())
            
            if 'q4' in middlewares:
                middlewares['q4'].send(filtered_dto.to_bytes_fast())
        
        elif batch_type == "transaction_items":
            logger.info(f"Procesando líneas de TransactionItems para Q2")
            if 'q2' in middlewares:
                self._send_transaction_items_by_year(data, middlewares['q2'])
    
    def _send_transaction_items_by_year(self, data: str, q2_middleware):
        lines = data.strip().split('\n')
        
        header = lines[0] if lines else ""
        
        logger.info(f"Procesando {len(lines)} líneas de TransactionItems para Q2")
        
        data_by_year = {'2024': [header], '2025': [header]}
        
        dto_helper = TransactionItemBatchDTO("", BatchType.RAW_CSV)
        
        for line in lines[1:]:
            if not line.strip():
                continue
                
            try:
                created_at = dto_helper.get_column_value(line, 'created_at')
                if created_at and len(created_at) >= 4:
                    year = created_at[:4]
                    if year in data_by_year:
                        data_by_year[year].append(line)
                else:
                    logger.warning(f"Created_at inválido: '{created_at}' en línea: {line[:50]}...")
            except Exception as e:
                logger.warning(f"Error procesando línea TransactionItem para Q2: {e}")
                continue
        
        for year, year_lines in data_by_year.items():
            if len(year_lines) > 1:  
                year_csv = '\n'.join(year_lines)
                year_dto = TransactionItemBatchDTO(year_csv, batch_type=BatchType.RAW_CSV)
                q2_middleware.send(year_dto.to_bytes_fast(), routing_key=year)
                logger.info(f"TransactionItemBatchDTO enviado a Q2 con routing key {year}: {len(year_lines)-1} líneas")
            else:
                logger.info(f"No hay datos para año {year} - solo header")
    
    
    def send_eof(self, middlewares: Dict[str, Any], batch_type: str = "transactions"):
        if batch_type == "transactions":
            eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
            
            if 'q1' in middlewares:
                middlewares['q1'].send(eof_dto.to_bytes_fast())
                logger.info("EOF:1 (TransactionBatch) enviado a Q1 queue")
            
            if 'q4' in middlewares:
                middlewares['q4'].send(eof_dto.to_bytes_fast())
                logger.info("EOF:1 (TransactionBatch) enviado a Q4 queue")
        
        elif batch_type == "transaction_items":
            if 'q2' in middlewares:
                eof_dto = TransactionItemBatchDTO("EOF:1", batch_type=BatchType.EOF)
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2024')
                middlewares['q2'].send(eof_dto.to_bytes_fast(), routing_key='2025')
                logger.info("EOF:1 (TransactionItemBatch) enviado a Q2 exchange con routing keys 2024 y 2025")