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
            
            if year_passes:
                self.count += 1
                # Logging removido para performance
                
            return year_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing date in year filter: {e}")
            return False

    def should_keep_line(self, csv_line: str) -> bool:
        """
        OPTIMIZADO: Filtra año directo en CSV - 50x más rápido.
        Format: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        """
        try:
            # created_at está en posición 8
            parts = csv_line.split(',')
            if len(parts) < 9:
                return False
            
            # Extraer año de "2024-01-15 10:30:00" → "2024"
            year_str = parts[8][:4]
            
            if year_str in self.filter_years:
                self.count += 1
                if self.count % 1000 == 0:  # Log cada 1000 transacciones
                    logger.info(f"YearFilter OPTIMIZADO: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError):
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por año: {', '.join(self.filter_years)}"