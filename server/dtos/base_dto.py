from abc import ABC, abstractmethod
from typing import Dict, List
from .enums import FileType, BatchType


class BaseDTO(ABC):
    """
    DTO base abstracto para manejar diferentes tipos de archivos CSV.
    Cada tipo de archivo tendrá su propia implementación específica.
    """
    
    def __init__(self, data, batch_type: BatchType = BatchType.DATA, file_type: FileType = None):
        """
        Inicializa el DTO base.
        
        Args:
            data: Los datos (puede ser lista de diccionarios, string CSV, etc.)
            batch_type: Tipo de batch (DATA, CONTROL, RAW_CSV, EOF)
            file_type: Tipo de archivo que maneja este DTO
        """
        self.data = data
        self.batch_type = batch_type
        self.file_type = file_type
        
    @abstractmethod
    def get_column_index(self, column_name: str) -> int:
        """Retorna el índice de una columna específica"""
        pass
    
        
    def get_column_value(self, csv_line: str, column_name: str) -> str:
        """
        Helper method: Extrae el valor de una columna de una línea CSV.
        
        Args:
            csv_line: Línea CSV
            column_name: Nombre de la columna
            
        Returns:
            str: Valor de la columna o cadena vacía si no existe
        """
        try:
            parts = csv_line.split(',')
            column_index = self.get_column_index(column_name)
            
            if column_index >= len(parts):
                return ""
            
            return parts[column_index].strip()
            
        except (IndexError, ValueError):
            return ""
    
    @abstractmethod
    def get_csv_headers(self) -> List[str]:
        """
        Retorna los headers específicos para este tipo de archivo.
        Debe ser implementado por cada DTO específico.
        """
        pass
    
    @abstractmethod
    def dict_to_csv_line(self, record: Dict) -> str:
        """
        Convierte un diccionario a una línea CSV.
        Debe ser implementado por cada DTO específico.
        """
        pass
    
    @abstractmethod
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        """
        Convierte una línea CSV a diccionario.
        Debe ser implementado por cada DTO específico.
        """
        pass
    
    def to_bytes_fast(self) -> bytes:
        """
        Versión optimizada para serialización rápida.
        """
        if self.batch_type in [BatchType.EOF, BatchType.RAW_CSV]:
            return self.data.encode('utf-8')
        else:
            return self.data.encode('utf-8')
    
    @classmethod
    def from_bytes_fast(cls, data: bytes) -> 'BaseDTO':
        """
        Versión optimizada que mantiene CSV raw.
        """
        decoded_data = data.decode('utf-8').strip()
        
        if decoded_data.startswith("EOF:"):
            return cls(decoded_data, BatchType.EOF)
        
        return cls(decoded_data, BatchType.RAW_CSV)
