import logging
from typing import Optional, Tuple
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType
from .base_configurator import NodeConfigurator

logger = logging.getLogger(__name__)


class HourNodeConfigurator(NodeConfigurator):
    def create_output_middlewares(self, output_q1: Optional[str], output_q3: Optional[str],
                                  output_q4: Optional[str] = None) -> Tuple:
        output_middleware = None
        if output_q1:
            output_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name=output_q1
            )
            logger.info(f"  Output Queue: {output_q1}")
        
        output_middleware_exchange = None
        if output_q3:
            output_middleware_exchange = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q3,
                route_keys=['semester.1', 'semester.2', 'eof.all']
            )
            logger.info(f"  Output Exchange: {output_q3}")
        
        # Nueva salida para top_customers
        output_q4_middleware = None
        if output_q4:
            output_q4_middleware = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=output_q4,
                route_keys=['filtered.data', 'filtered.eof']
            )
            logger.info(f"  Output Q4 Exchange: {output_q4}")
        
        return output_middleware, None, output_middleware_exchange, output_q4_middleware
    
    def process_filtered_data(self, filtered_csv: str) -> str:
        return filtered_csv
    
    def send_data(self, data: str, output_middleware, output_exchange_middleware, 
                 output_middleware_exchange, output_q4_middleware=None):
        # Enviar a queue normal (para filter amount)
        if output_middleware:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            output_middleware.send(filtered_dto.to_bytes_fast())
        
        # Enviar a exchange por semestre (para groupby TPV)
        if output_middleware_exchange:
            self._send_to_exchange_by_semester(data, output_middleware_exchange)
        
        # Enviar a exchange para top_customers
        if output_q4_middleware:
            filtered_dto = TransactionBatchDTO(data, batch_type=BatchType.RAW_CSV)
            output_q4_middleware.send(filtered_dto.to_bytes_fast(), routing_key='filtered.data')
            logger.info("Datos filtrados enviados a top_customers exchange")
    
    def send_eof(self, output_middleware, output_exchange_middleware, 
                output_middleware_exchange, output_q4_middleware=None):
        eof_dto = TransactionBatchDTO("EOF:1", batch_type=BatchType.EOF)
        
        if output_middleware:
            output_middleware.send(eof_dto.to_bytes_fast())
        
        if output_middleware_exchange:
            output_middleware_exchange.send(
                eof_dto.to_bytes_fast(),
                routing_key='eof.all'
            )
            logger.info("EOF:1 enviado a routing key 'eof.all'")
        
        # Enviar EOF a top_customers exchange
        if output_q4_middleware:
            output_q4_middleware.send(
                eof_dto.to_bytes_fast(),
                routing_key='filtered.eof'
            )
            logger.info("EOF:1 enviado a top_customers exchange")
            
    def _send_to_exchange_by_semester(self, csv_data: str, exchange_middleware):
        """Separa datos por semestre y envía con routing key apropiada."""
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