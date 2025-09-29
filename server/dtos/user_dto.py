from typing import Dict, List
from .base_dto import BaseDTO
from .enums import FileType, BatchType


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
        """Mapeo de columnas para usuarios"""
        column_map = {
            'user_id': 0,
            'gender': 1,
            'birthdate': 2,
            'registered_at': 3
        }
        
        if column_name not in column_map:
            raise ValueError(f"Columna '{column_name}' no existe en usuarios")
        
        return column_map[column_name]
