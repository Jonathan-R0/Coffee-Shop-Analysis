from abc import ABC, abstractmethod

class FilterStrategy(ABC):
    """
    Interfaz base para todas las estrategias de filtro.
    Define el contrato que deben cumplir todas las implementaciones específicas.
    """
    
    @abstractmethod
    def should_pass(self, transaction: dict) -> bool:
        """
        Determina si una transacción debe pasar el filtro.
        
        Args:
            transaction (dict): Datos de la transacción a evaluar
            
        Returns:
            bool: True si la transacción pasa el filtro, False en caso contrario
        """
        pass
    
    @abstractmethod
    def get_filter_description(self) -> str:
        """
        Retorna una descripción legible del filtro para logging.
        
        Returns:
            str: Descripción del filtro
        """
        pass
