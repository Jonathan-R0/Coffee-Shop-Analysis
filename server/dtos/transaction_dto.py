from typing import Dict, List
from .base_dto import BaseDTO
from .enums import FileType, BatchType


class TransactionBatchDTO(BaseDTO):
    """
    DTO específico para transacciones (mantiene compatibilidad con código actual).
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
