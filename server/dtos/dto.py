from abc import ABC, abstractmethod
import struct
import json
from typing import Dict, List, Any, Optional
from enum import Enum


class FileType(Enum):
    """Enumeración para los tipos de archivos soportados"""
    TRANSACTIONS = "transactions"
    USERS = "users"
    STORES = "stores"
    MENU_ITEMS = "menu_items"
    PAYMENT_METHODS = "payment_methods"
    VOUCHERS = "vouchers"
    TRANSACTION_ITEMS = "transaction_items"


class BatchType(Enum):
    """Enumeración para los tipos de batch"""
    DATA = "DATA"
    CONTROL = "CONTROL"
    RAW_CSV = "RAW_CSV"
    EOF = "EOF"


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
            return
    
    
    @classmethod
    def from_bytes_fast(cls, data: bytes) -> 'BaseDTO':
        """
        Versión optimizada que mantiene CSV raw.
        """
        decoded_data = data.decode('utf-8').strip()
        
        if decoded_data.startswith("EOF:"):
            return cls(decoded_data, BatchType.EOF)
        
        return cls(decoded_data, BatchType.RAW_CSV)
    
    def to_bytes_fast(self) -> bytes:
        """
        Versión optimizada para serialización rápida.
        """

        return self.data.encode('utf-8')


class TransactionBatchDTO(BaseDTO):
    """
    DTO específico para transacciones (mantiene compatibilidad con tu código actual).
    """
    
    def __init__(self, transactions, batch_type="DATA"):
        if isinstance(batch_type, str):
            batch_type = BatchType(batch_type)
        super().__init__(transactions, batch_type, FileType.TRANSACTIONS)


    def get_csv_headers(self) -> List[str]:
        return [
            "transaction_id", "store_id", "payment_method_id", "voucher_id",
            "user_id", "original_amount", "discount_applied", "final_amount", "created_at"
        ]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['transaction_id']},{record['store_id']},{record['payment_method_id']},"
                f"{record['voucher_id']},{record['user_id']},{record['original_amount']},"
                f"{record['discount_applied']},{record['final_amount']},{record['created_at']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "transaction_id": values[0],
            "store_id": values[1],
            "payment_method_id": values[2],
            "voucher_id": values[3],
            "user_id": values[4],
            "original_amount": values[5],
            "discount_applied": values[6],
            "final_amount": values[7],
            "created_at": values[8],
        }
    def get_column_index(self, column_name: str) -> int:
        """Mapeo de columnas para transacciones"""
        column_map = {
            'transaction_id': 0,
            'store_id': 1,
            'payment_method_id': 2,
            'voucher_id': 3,
            'user_id': 4,
            'original_amount': 5,
            'discount_applied': 6,
            'final_amount': 7,
            'created_at': 8
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en transacciones")
        
        return column_map[column_name]
    


class UserBatchDTO(BaseDTO):
    """
    DTO específico para usuarios.
    """
    
    def __init__(self, users, batch_type=BatchType.DATA):
        super().__init__(users, batch_type, FileType.USERS)
    
    def get_csv_headers(self) -> List[str]:
        return ["user_id", "gender", "birthdate", "registered_at"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['user_id']},{record['gender']},{record['birthdate']},"
                f"{record['registered_at']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "user_id": values[0],
            "gender": values[1],
            "birthdate": values[2],
            "registered_at": values[3]
        }
    
    def get_column_index(self, column_name: str) -> int:
        """Mapeo de columnas para stores"""
        column_map = {
            'store_id': 0,
            'store_name': 1,
            'street': 2,
            'postal_code': 3,
            'city': 4,
            'state': 5,
            'latitude': 6,
            'longitude': 7
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en stores")
        
        return column_map[column_name]


class StoreBatchDTO(BaseDTO):
    """
    DTO específico para tiendas.
    """
    
    def __init__(self, stores, batch_type=BatchType.DATA):
        super().__init__(stores, batch_type, FileType.STORES)
    
    def get_csv_headers(self) -> List[str]:
        return ["store_id", "store_name", "street", "postal_code", "city","state","latitude","longitude"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['store_id']},{record['store_name']},{record['street']},"
                f"{record['postal_code']},{record['city']},{record['state']},"
                f"{record['latitude']},{record['longitude']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "store_id": values[0],
            "store_name": values[1],
            "street": values[2],
            "postal_code": values[3],
            "city": values[4],
            "state": values[5],
            "latitude": values[6],
            "longitude": values[7]
        }
        
    def get_column_index(self, column_name: str) -> int:
        column_map = {
            'store_id': 0,
            'store_name': 1,
            'street': 2,
            'postal_code': 3,
            'city': 4,
            'state': 5,
            'latitude': 6,
            'longitude': 7
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en stores")
        
        return column_map[column_name]


class MenuItemBatchDTO(BaseDTO):
    """
    DTO específico para items del menú.
    """
    
    def __init__(self, menu_items, batch_type=BatchType.DATA):
        super().__init__(menu_items, batch_type, FileType.MENU_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["item_id", "item_name", "category", "is_seasonal", "available_from","available_to"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['item_id']},{record['item_name']},{record['category']},"
                f"{record['is_seasonal']},{record['available_from']},{record['available_to']}")
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "item_id": values[0],
            "item_name": values[1],
            "category": values[2],
            "is_seasonal": values[3],
            "available_from": values[4],
            "available_to": values[5]
        }


class TransactionItemBatchDTO(BaseDTO):
    """
    DTO específico para items de transacciones.
    """
    
    def __init__(self, transaction_items, batch_type=BatchType.DATA):
        super().__init__(transaction_items, batch_type, FileType.TRANSACTION_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["transaction_id", "item_id", "quantity", "unit_price", "subtotal","created_at"]
    
    def dict_to_csv_line(self, record: Dict) -> str:
        return (f"{record['transaction_id']},{record['item_id']},{record['quantity']},"
                f"{record['unit_price']},{record['subtotal']},{record['created_at']}")
    
    def csv_line_to_dict(self, csv_line: str) -> Dict:
        values = csv_line.split(',')
        return {
            "transaction_id": values[0],
            "item_id": values[1],
            "quantity": values[2],
            "unit_price": values[3],
            "subtotal": values[4],
            "created_at": values[5]
        }


class DTOFactory:
    """
    Factory para crear DTOs según el tipo de archivo.
    """
    
    _dto_mapping = {
        FileType.TRANSACTIONS: TransactionBatchDTO,
        FileType.USERS: UserBatchDTO,
        FileType.STORES: StoreBatchDTO,
        FileType.MENU_ITEMS: MenuItemBatchDTO,
        FileType.TRANSACTION_ITEMS: TransactionItemBatchDTO,
    }
    
    @classmethod
    def create_dto(cls, file_type: FileType, data, batch_type=BatchType.DATA) -> BaseDTO:
        """
        Crea un DTO específico según el tipo de archivo.
        
        Args:
            file_type: Tipo de archivo
            data: Datos a encapsular
            batch_type: Tipo de batch
            
        Returns:
            BaseDTO: Instancia del DTO específico
        """
        dto_class = cls._dto_mapping.get(file_type)
        if not dto_class:
            raise ValueError(f"Tipo de archivo no soportado: {file_type}")
        
        return dto_class(data, batch_type)