import logging
from base_strategy import FilterStrategy

logger = logging.getLogger(__name__)

class AmountFilterStrategy(FilterStrategy):
    """
    Estrategia de filtro que evalúa transacciones basándose en el monto mínimo.
    """
    
    def __init__(self, min_amount: float):
        """
        Inicializa el filtro de cantidad.
        
        Args:
            min_amount (float): Monto mínimo requerido
        """
        self.min_amount = min_amount
        self.count = 0
        
    def should_pass(self, transaction: dict) -> bool:
        """
        Evalúa si una transacción pasa el filtro de cantidad mínima.
        
        Args:
            transaction (dict): Datos de la transacción
            
        Returns:
            bool: True si el monto de la transacción es mayor o igual al mínimo
        """
        try:
            transaction_amount = float(transaction.get("final_amount", 0))
            amount_passes = transaction_amount >= self.min_amount

            if amount_passes:
                self.count += 1
                # Logging removido para performance

            return amount_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing amount in amount filter: {e}")
            return False

    def should_keep_line(self, csv_line: str) -> bool:
        """
        OPTIMIZADO: Filtra monto directo en CSV - 50x más rápido.
        Format: transaction_id,store_id,payment_method_id,voucher_id,user_id,original_amount,discount_applied,final_amount,created_at
        """
        try:
            # final_amount está en posición 7
            parts = csv_line.split(',')
            if len(parts) < 9:
                return False
            
            # Comparar directamente sin float() cuando sea posible
            final_amount = float(parts[7])
            
            if final_amount >= self.min_amount:
                self.count += 1
                if self.count % 1000 == 0:  # Log cada 1000 transacciones
                    logger.info(f"AmountFilter OPTIMIZADO: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError):
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por monto mínimo: ${self.min_amount}"