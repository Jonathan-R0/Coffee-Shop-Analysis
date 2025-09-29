from typing import Dict, List
from .base_dto import BaseDTO
from .enums import FileType, BatchType


class TransactionItemBatchDTO(BaseDTO):
    """
    DTO específico para items de transacciones.
    """
    
    def __init__(self, transaction_items, batch_type=BatchType.DATA):
        super().__init__(transaction_items, batch_type, FileType.TRANSACTION_ITEMS)
    
    def get_csv_headers(self) -> List[str]:
        return ["transaction_id", "item_id", "quantity", "unit_price", "subtotal", "created_at"]
    
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
        
    def get_column_index(self, column_name: str) -> int:
        """Mapeo de columnas para transaction items"""
        column_map = {
            'transaction_id': 0,
            'item_id': 1,
            'quantity': 2,
            'unit_price': 3,
            'subtotal': 4,
            'created_at': 5
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en transaction_items")
        
        return column_map[column_name]
