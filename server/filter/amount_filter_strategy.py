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
            transaction_amount = float(transaction.get("subtotal", 0))
            amount_passes = transaction_amount >= self.min_amount
            
            transaction_id = transaction.get('transaction_id', 'unknown')
            if amount_passes:
                self.count += 1
                logger.info(f"Amount filter PASS: {transaction_id} (${transaction_amount})")
                logger.info(f"Ammount of transactions passed the amount filter so far: {self.count}")
            else:
                logger.info(f"Amount filter REJECT: {transaction_id} (${transaction_amount})")

            return amount_passes
            
        except (ValueError, TypeError) as e:
            logger.error(f"Error parsing amount in amount filter: {e}")
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por monto mínimo: ${self.min_amount}"
