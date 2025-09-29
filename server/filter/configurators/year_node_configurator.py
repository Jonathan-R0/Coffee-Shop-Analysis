import logging
from typing import Optional, Tuple
from rabbitmq.middleware import MessageMiddlewareQueue,MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class YearNodeConfigurator(NodeConfigurator):
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Tuple:
        output_middleware = None
        if output_q1:
            output_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Queue: {output_q1}")
        
        output_q4_middleware = None
        if output_q4:
            output_q4_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q4
            )
            logger.info(f"  Output Q4 Queue: {output_q4}")
        
        return output_middleware, None, None, output_q4_middleware
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, output_middleware, output_exchange_middleware, 
                 output_middleware_exchange, output_q4_middleware=None):
        # Enviar a queue normal (para hour filters)
        if output_middleware:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            output_middleware.send(filtered_dto.to_bytes_fast())
        
        # Enviar a queue Q4 (para top customers GroupBy nodes)
        if output_q4_middleware:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            output_q4_middleware.send(filtered_dto.to_bytes_fast())
    
    def send_eof(self, output_middleware, output_exchange_middleware, 
                output_middleware_exchange, output_q4_middleware=None):
        eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
        
        if output_middleware:
            output_middleware.send(eof_dto.to_bytes_fast())
            logger.info("EOF:1 enviado a output queue")
        
        if output_q4_middleware:
            output_q4_middleware.send(eof_dto.to_bytes_fast())
            logger.info("EOF:1 enviado a Q4 queue")
