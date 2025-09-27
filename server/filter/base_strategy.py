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

    def should_keep_line(self, csv_line: str) -> bool:
        """
        OPTIMIZADO: Filtra línea CSV directo sin parsear a dict.
        Implementar en subclases para máximo rendimiento.
        Args:
            csv_line (str): Línea CSV raw
        Returns:
            bool: True si la línea pasa el filtro
        """
        # Fallback: parsear y usar should_pass (más lento)
        parts = csv_line.split(',')
        if len(parts) < 9:
            return False
        
        transaction = {
            "transaction_id": parts[0],
            "store_id": parts[1], 
            "payment_method_id": parts[2],
            "voucher_id": parts[3],
            "user_id": parts[4],
            "original_amount": parts[5],
            "discount_applied": parts[6],
            "final_amount": parts[7],
            "created_at": parts[8],
        }
        return self.should_pass(transaction)

    def filter_csv_batch(self, csv_data: str) -> str:
        """
        OPTIMIZADO: Filtra CSV completo sin deserialización.
        Args:
            csv_data (str): Datos CSV raw
        Returns:
            str: CSV filtrado
        """
        lines = csv_data.split('\n')
        filtered_lines = []
        
        for line in lines:
            if line.strip() and self.should_keep_line(line):
                filtered_lines.append(line)
        
        return '\n'.join(filtered_lines)
