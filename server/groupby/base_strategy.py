import logging
from abc import ABC, abstractmethod
from dtos.dto import TransactionBatchDTO, BatchType

logger = logging.getLogger(__name__)


class GroupByStrategy(ABC):
    def __init__(self):
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
    
    @abstractmethod
    def process_csv_line(self, csv_line: str):
        """Cada estrategia decide qué hacer con cada línea"""
        pass
    
    @abstractmethod
    def generate_results_csv(self) -> str:
        """Cada estrategia genera su propio output"""
        pass
    
    @abstractmethod
    def get_output_routing_key(self) -> str:
        """Retorna la routing key para enviar datos."""
        pass
    
    @abstractmethod
    def get_eof_routing_key(self) -> str:
        """Retorna la routing key para enviar EOF."""
        pass
    
    def setup_output_middleware(self, rabbitmq_host: str, output_exchange: str):
        """Por defecto: crear un exchange simple"""
        from rabbitmq.middleware import MessageMiddlewareExchange
        
        output_routing_keys = [
            self.get_output_routing_key(),
            self.get_eof_routing_key()
        ]
        
        output_middleware = MessageMiddlewareExchange(
            host=rabbitmq_host,
            exchange_name=output_exchange,
            route_keys=output_routing_keys
        )
        
        return {"output_middleware": output_middleware}
    
    def handle_eof_message(self, middlewares: dict) -> bool:
        """Por defecto: enviar resultados inmediatamente"""
        logger.info("EOF recibido. Generando resultados finales")
        
        results_csv = self.generate_results_csv()
        
        result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
        middlewares["output_middleware"].send(
            result_dto.to_bytes_fast(),
            routing_key=self.get_output_routing_key()
        )
        
        eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
        middlewares["output_middleware"].send(
            eof_dto.to_bytes_fast(),
            routing_key=self.get_eof_routing_key()
        )
        
        logger.info("Resultados enviados al siguiente nodo")
        return True
    
    def cleanup_middlewares(self, middlewares: dict):
        """Limpia los middlewares específicos de la estrategia."""
        for name, middleware in middlewares.items():
            if middleware and hasattr(middleware, 'close'):
                try:
                    middleware.close()
                    logger.info(f"{name} middleware cerrado")
                except Exception as e:
                    logger.warning(f"Error cerrando {name}: {e}")
        