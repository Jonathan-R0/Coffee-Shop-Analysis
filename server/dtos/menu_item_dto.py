from typing import Dict, List
from .base_dto import BaseDTO
from .enums import FileType, BatchType


class MenuItemBatchDTO(BaseDTO):
    """
    DTO específico para items del menú.
    """
    
    def __init__(self, menu_items, batch_type=BatchType.DATA):
        super().__init__(menu_items, batch_type, FileType.MENU_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["item_id", "item_name", "category", "is_seasonal", "available_from", "available_to"]
    
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
        
    def get_column_index(self, column_name: str) -> int:
        """Mapeo de columnas para items del menú"""
        column_map = {
            'item_id': 0,
            'item_name': 1,
            'category': 2,
            'is_seasonal': 3,
            'available_from': 4,
            'available_to': 5
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en menu_items")
        
        return column_map[column_name]
