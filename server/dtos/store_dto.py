from typing import Dict, List
from .base_dto import BaseDTO
from .enums import FileType, BatchType


class StoreBatchDTO(BaseDTO):
    """
    DTO específico para tiendas.
    """
    
    def __init__(self, stores, batch_type=BatchType.DATA):
        super().__init__(stores, batch_type, FileType.STORES)
    
    def get_csv_headers(self) -> List[str]:
        return ["store_id", "store_name", "street", "postal_code", "city", "state", "latitude", "longitude"]
    
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
        """Mapeo de columnas para tiendas"""
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
