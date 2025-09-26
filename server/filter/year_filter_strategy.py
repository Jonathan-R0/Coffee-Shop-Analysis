import logging
from datetime import datetime
from base_strategy import FilterStrategy

logger = logging.getLogger(__name__)

class YearFilterStrategy(FilterStrategy):
    """
    Estrategia de filtro que evalúa transacciones basándose en el año.
    """
    
    def __init__(self, filter_years: list):
        """
        Inicializa el filtro de año.
        
        Args:
            filter_years (list): Lista de años permitidos como strings
        """
        self.filter_years = filter_years
        self.count = 0
        
    def should_pass(self, transaction: dict) -> bool:
        """
        Evalúa si una transacción pasa el filtro de año.
        
        Args:
            transaction (dict): Datos de la transacción
            
        Returns:
            bool: True si el año de la transacción está en la lista permitida
        """
        try:
            transaction_time = datetime.strptime(transaction.get("created_at"), "%Y-%m-%d %H:%M:%S")
            year_passes = str(transaction_time.year) in self.filter_years
            
            transaction_id = transaction.get('transaction_id', 'unknown')
            if year_passes:
                self.count += 1
                logger.info(f"Year filter PASS: {transaction_id} ({transaction_time.year})")
                logger.info(f"Ammount of transactions passed the year filter so far: {self.count}")
            else:
                logger.info(f"Year filter REJECT: {transaction_id} ({transaction_time.year})")
                
            return year_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing date in year filter: {e}")
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por año: {', '.join(self.filter_years)}"
