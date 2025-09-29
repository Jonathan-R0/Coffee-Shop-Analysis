import logging
import os
from collections import defaultdict
from typing import Dict, Tuple
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos import TransactionBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

    
class TPVAggregation:
    """Clase para manejar el Total Payment Value (TPV) por tienda y semestre."""
    
    def __init__(self):
        self.total_payment_value = 0.0
        self.transaction_count = 0
    
    def add_transaction(self, final_amount: float):
        """Agrega una transacción al TPV."""
        self.total_payment_value += final_amount
        self.transaction_count += 1
    
    def to_csv_line(self, year_half: str, store_id: str) -> str:
        """Convierte la agregación a una línea CSV."""
        return f"{year_half},{store_id},{self.total_payment_value:.2f},{self.transaction_count}"


class GroupByNode:
    """
    Nodo para calcular TPV por semestre y tienda.
    Los datos ya vienen filtrados por año (2024-2025) y hora (06:00-23:00).
    """
    
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_exchange = os.getenv('INPUT_Q3', 'groupby.join.exchange')
        self.join_exchange_q3 = os.getenv('JOIN_EXCHANGE', 'join.exchange')
        self.semester = self._validate_semester()
        
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.input_exchange,
            route_keys=[f'semester.{self.semester}', 'eof.all'] 
        )

        self.join_exchange_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.join_exchange_q3,
            route_keys=['tpv.data', 'tpv.eof']
        )

        self.tpv_aggregations: Dict[Tuple[str, str], TPVAggregation] = defaultdict(TPVAggregation)
        
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        
        logger.info(f"GroupByNode inicializado:")
        logger.info(f"  Exchange: {self.input_exchange}")
        logger.info(f"  Semestre: {self.semester}")
        logger.info(f"  Calculando TPV por semestre y tienda")
    
    def _validate_semester(self) -> str:
        """Valida y retorna el semestre configurado."""
        semester = os.getenv('SEMESTER')
        if semester not in ['1', '2']:
            raise ValueError("SEMESTER debe ser '1' o '2'")
        return semester
    
    def _extract_transaction_data(self, csv_line: str) -> Tuple[str, str, float]:
        """
        Extrae datos necesarios de una línea CSV usando el DTO helper.
        
        Returns:
            Tuple[year_half, store_id, final_amount]
        """
        try:
            store_id = self.dto_helper.get_column_value(csv_line, 'store_id')
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            final_amount_str = self.dto_helper.get_column_value(csv_line, 'final_amount')
            
            if not all([store_id, created_at, final_amount_str]):
                return None, None, None
            
            year = created_at[:4]  
            year_half = f"{year}-H{self.semester}"
            final_amount = float(final_amount_str)
            
            return year_half, store_id, final_amount
            
        except (ValueError, IndexError) as e:
            logger.warning(f"Error extrayendo datos de línea: {e}")
            return None, None, None
    
    def _process_csv_batch(self, csv_data: str):
        """
        Procesa un batch de datos CSV y actualiza las agregaciones de TPV.
        Los datos ya vienen filtrados, solo necesitamos agrupar y sumar.
        """
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            year_half, store_id, final_amount = self._extract_transaction_data(line)
            
            if year_half and store_id and final_amount is not None:
                key = (year_half, store_id)
                self.tpv_aggregations[key].add_transaction(final_amount)
                processed_count += 1
        
        if processed_count > 0:
            logger.info(f"Procesadas {processed_count} transacciones. "
                       f"Total grupos (year_half, store_id): {len(self.tpv_aggregations)}")
    
    def _generate_results_csv(self) -> str:
        """
        Genera el CSV con los resultados de TPV por semestre y tienda.
        
        Returns:
            str: CSV con headers y datos de TPV agregados
        """
        if not self.tpv_aggregations:
            logger.warning("No hay datos para generar resultados")
            return "year_half_created_at,store_id,total_payment_value,transaction_count"
        
        csv_lines = ["year_half_created_at,store_id,total_payment_value,transaction_count"]
        
        # Ordenar por year_half y luego por store_id para consistencia
        sorted_keys = sorted(self.tpv_aggregations.keys())
        
        for (year_half, store_id) in sorted_keys:
            aggregation = self.tpv_aggregations[(year_half, store_id)]
            csv_lines.append(aggregation.to_csv_line(year_half, store_id))
        
        logger.info(f"Resultados TPV generados para {len(self.tpv_aggregations)} grupos")
        return '\n'.join(csv_lines)
    
    def _handle_eof_message(self) -> bool:
        try:
            logger.info("EOF recibido. Generando resultados finales de TPV")
            
            results_csv = self._generate_results_csv()
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.join_exchange_middleware.send(
                result_dto.to_bytes_fast(), 
                routing_key='tpv.data'
            )
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.join_exchange_middleware.send(
                eof_dto.to_bytes_fast(), 
                routing_key='tpv.eof'
            )
            
            logger.info("Resultados TPV enviados al join node")
            return True
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
    
    def process_message(self, message: bytes) -> bool:
        """
        Procesa un mensaje de la cola.
        
        Args:
            message: Mensaje binario recibido
            
        Returns:
            bool: True si debe terminar el consuming, False para continuar
        """
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)
            
            if dto.batch_type == BatchType.EOF:
                return self._handle_eof_message()
            
            if dto.batch_type == BatchType.RAW_CSV:
                self._process_csv_batch(dto.data)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        """Callback para RabbitMQ cuando llega un mensaje."""
        try:
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        """Inicia el nodo de agrupación TPV."""
        try:
            logger.info("Iniciando GroupByNode para cálculo de TPV...")
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("GroupByNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Limpia recursos al finalizar."""
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")


if __name__ == "__main__":
    node = GroupByNode()
    node.start()
    
