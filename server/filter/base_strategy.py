from abc import ABC, abstractmethod
from typing import List, Dict
import logging

logger = logging.getLogger(__name__)

class FilterStrategy(ABC):
    """
    Clase base para las estrategias de filtro.
    """

    @abstractmethod
    def should_keep_line(self, csv_line: str) -> bool:
        """
        OPTIMIZADO: Filtra línea CSV directo sin parsear a dict.
        Debe ser implementado por cada subclase para máximo rendimiento.
        Args:
            csv_line (str): Línea CSV raw
        Returns:
            bool: True si la línea pasa el filtro
        """
        pass

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