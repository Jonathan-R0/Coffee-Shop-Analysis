from abc import ABC, abstractmethod
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class FilterStrategy(ABC):
    """
    Clase base para las estrategias de filtro.
    """

    @abstractmethod
    def should_pass(self, transaction: Dict) -> bool:
        """
        Determina si una transacción individual pasa el filtro.
        Args:
            transaction (Dict): Una transacción representada como un diccionario.
        Returns:
            bool: True si pasa el filtro, False en caso contrario.
        """
        pass

    def filter_batch(self, batch: List[Dict]) -> List[Dict]:
        """
        Filtra un batch completo de transacciones.
        Args:
            batch (List[Dict]): Un batch de transacciones representado como una lista de diccionarios.
        Returns:
            List[Dict]: Una lista de transacciones que pasaron el filtro.
        """
        filtered_batch = []
        for transaction in batch:
            if self.should_pass(transaction):
                filtered_batch.append(transaction)
        
        return filtered_batch
