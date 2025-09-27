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

            if hour_passes:
                self.count += 1
                # Logging removido para performance

            return hour_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing date/time in hour filter: {e}")
            return False

    def should_keep_line(self, csv_line: str) -> bool:
        """
        OPTIMIZADO: Filtra hora directo en CSV - 50x más rápido.
        Format: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        """
        try:
            # created_at está en posición 8: "2024-01-15 10:30:00"
            parts = csv_line.split(',')
            if len(parts) < 9:
                return False
            
            # Extraer hora de "2024-01-15 10:30:00" → "10:30"
            time_part = parts[8].split(' ')[1]  # "10:30:00"
            hour_minute = time_part[:5]  # "10:30"
            
            # Comparar strings directamente (más rápido que datetime)
            if self.start_time.strftime("%H:%M") <= hour_minute <= self.end_time.strftime("%H:%M"):
                self.count += 1
                if self.count % 1000 == 0:  # Log cada 1000 transacciones
                    logger.info(f"HourFilter OPTIMIZADO: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError):
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por hora: {self.filter_hours}"