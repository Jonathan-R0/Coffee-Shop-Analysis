import logging
from typing import Optional, Dict, Any
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class HourNodeConfigurator(NodeConfigurator):
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Dict[str, Any]:
        middlewares = {}
        
        if output_q1:
            middlewares['q1'] = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Q1 Queue: {output_q1}")
        
        if output_q3:
            middlewares['q3'] = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q3,
                route_keys=['semester.1', 'semester.2', 'eof.all']
            )
            logger.info(f"  Output Q3 Exchange: {output_q3}")
        
        return middlewares
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, middlewares: Dict[str, Any]):
        if 'q1' in middlewares:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            middlewares['q1'].send(filtered_dto.to_bytes_fast())
        
        if 'q3' in middlewares:
            self._send_to_exchange_by_semester(data, middlewares['q3'])
    
    def send_eof(self, middlewares: Dict[str, Any]):
        eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
        
        if 'q1' in middlewares:
            middlewares['q1'].send(eof_dto.to_bytes_fast())
            logger.info("EOF:1 enviado a Q1 queue")
        
        if 'q3' in middlewares:
            middlewares['q3'].send(
                eof_dto.to_bytes_fast(),
                routing_key='eof.all'
            )
            logger.info("EOF:1 enviado a Q3 exchange con routing key 'eof.all'")
            
    def _send_to_exchange_by_semester(self, csv_data: str, exchange_middleware):
        semester_1_lines = []
        semester_2_lines = []
        
        for line in csv_data.split('\n'):
            if not line.strip():
                continue
            
            month = self._get_month_from_csv_line(line)
            if month:
                if month <= 6:
                    semester_1_lines.append(line)
                else:
                    semester_2_lines.append(line)
        
        if semester_1_lines:
            csv_s1 = '\n'.join(semester_1_lines)
            dto_s1 = TransactionBatchDTO(csv_s1, batch_type=BatchType.RAW_CSV)
            exchange_middleware.send(dto_s1.to_bytes_fast(), routing_key='semester.1')
        
        if semester_2_lines:
            csv_s2 = '\n'.join(semester_2_lines)
            dto_s2 = TransactionBatchDTO(csv_s2, batch_type=BatchType.RAW_CSV)
            exchange_middleware.send(dto_s2.to_bytes_fast(), routing_key='semester.2')
    
    @staticmethod
    def _get_month_from_csv_line(line):
        """Extract month from CSV line"""
        fields = line.split(',')
        if len(fields) >= 9:
            date_str = fields[8]
            return int(date_str[5:7])
        return None