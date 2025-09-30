import logging
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class YearNodeConfigurator(NodeConfigurator):
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        
        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q4:
            middlewares['q4'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q4
            )
            logger.info(f"  Output Q4 Queue: {output_q4}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, middlewares: Dict[str, Any]):
        filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
        
        if 'q1' in middlewares:
            middlewares['q1'].send(filtered_dto.to_bytes_fast())
        
        if 'q4' in middlewares:
            middlewares['q4'].send(filtered_dto.to_bytes_fast())
    
    def send_eof(self, middlewares: Dict[str, Any]):
        eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
        
        if 'q1' in middlewares:
            middlewares['q1'].send(eof_dto.to_bytes_fast())
            logger.info("EOF:1 enviado a Q1 queue")
        
        if 'q4' in middlewares:
            middlewares['q4'].send(eof_dto.to_bytes_fast())
            logger.info("EOF:1 enviado a Q4 queue")