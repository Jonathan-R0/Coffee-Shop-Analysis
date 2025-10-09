import logging
from collections import defaultdict
from typing import Dict, Tuple
from base_strategy import GroupByStrategy
from tpv_aggregation import TPVAggregation
from dtos.dto import TransactionBatchDTO, BatchType
from rabbitmq.middleware import MessageMiddlewareExchange

logger = logging.getLogger(__name__)


class TPVGroupByStrategy(GroupByStrategy):
    def __init__(self, semester: str):
        super().__init__()
        self.semester = semester
        self.client_states = {}
        logger.info(f"TPVGroupByStrategy inicializada para semestre {semester}")
    
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        
        output_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=['tpv.data', 'tpv.eof']
        )
        
        logger.info(f"  Output exchange: {output_exchange}")
        logger.info(f"  Routing keys: tpv.data, tpv.eof")
        
        return {"output": output_middleware}
    
    def _get_state(self, client_id: int) -> Dict[Tuple[str, str], TPVAggregation]:
        if client_id not in self.client_states:
            self.client_states[client_id] = defaultdict(TPVAggregation)
        return self.client_states[client_id]

    def process_csv_line(self, csv_line: str, client_id: int):
        try:
            aggregations = self._get_state(client_id)
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            final_amount_str = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not all([store_id, created_at, final_amount_str]):
                return
            
            year = created_at[:4]
            year_half = f"{year}-H{self.semester}"
            final_amount = float(final_amount_str)
            
            key = (year_half, store_id)
            aggregations[key].add_transaction(final_amount)
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error procesando línea para TPV: {e}")
    
    def generate_results_csv(self, aggregations: Dict[Tuple[str, str], TPVAggregation]) -> str:
        if not aggregations:
            logger.warning("No hay datos TPV para generar resultados")
            return "year_half_created_at,store_id,total_payment_value,transaction_count"
        
        csv_lines = ["year_half_created_at,store_id,total_payment_value,transaction_count"]
        
        for (year_half, store_id) in sorted(aggregations.keys()):
            aggregation = aggregations[(year_half, store_id)]
            csv_lines.append(aggregation.to_csv_line(year_half, store_id))
        
        logger.info(f"Resultados TPV generados para {len(aggregations)} grupos")
        return '\n'.join(csv_lines)
    
    def handle_eof_message(self, dto: TransactionBatchDTO, middlewares: dict, client_id: int) -> bool:
        logger.info("EOF recibido. Generando resultados TPV finales")
        aggregations = self.client_states.get(client_id, {})
        results_csv = self.generate_results_csv(aggregations)
        headers = {'client_id': client_id}
        
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        middlewares["output"].send(
            result_dto.to_bytes_fast(),
            routing_key='tpv.data',
            headers=headers,
        )
        
        eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
        middlewares["output"].send(
            eof_dto.to_bytes_fast(),
            routing_key='tpv.eof',
            headers=headers,
        )
        
        if client_id in self.client_states:
            del self.client_states[client_id]
            logger.debug(f"Estado TPV limpiado para cliente {client_id}")
        
        logger.info("Resultados TPV enviados al siguiente nodo")
        return True