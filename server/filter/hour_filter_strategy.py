import logging
from datetime import datetime
from base_strategy import FilterStrategy

logger = logging.getLogger(__name__)

class HourFilterStrategy(FilterStrategy):
    """
    Estrategia de filtro que evalúa transacciones basándose en el rango horario.
    """
    
    def __init__(self, filter_hours: str):
        """
        Inicializa el filtro de hora.
        
        Args:
            filter_hours (str): Rango horario en formato "HH:MM-HH:MM"
        """
        self.filter_hours = filter_hours
        start_hour, end_hour = filter_hours.split('-')
        self.start_time = datetime.strptime(start_hour, "%H:%M").time()
        self.end_time = datetime.strptime(end_hour, "%H:%M").time()
        self.count = 0
        
    def should_pass(self, transaction: dict) -> bool:
        """
        Evalúa si una transacción pasa el filtro de hora.
        
        Args:
            transaction (dict): Datos de la transacción
            
        Returns:
            bool: True si la hora de la transacción está en el rango permitido
        """
        try:
            transaction_time = datetime.strptime(transaction.get("created_at"), "%Y-%m-%d %H:%M:%S")
            hour_passes = self.start_time <= transaction_time.time() <= self.end_time
            
            transaction_id = transaction.get('transaction_id', 'unknown')
            if hour_passes:
                self.count += 1
                logger.info(f"Ammount of transactions passed the hour filter so far: {self.count}")

            return hour_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing date/time in hour filter: {e}")
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por hora: {self.filter_hours}"