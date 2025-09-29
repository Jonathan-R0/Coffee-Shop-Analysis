import logging
from datetime import datetime
from .base_strategy import FilterStrategy
from dtos.dto import TransactionBatchDTO, BatchType

logger = logging.getLogger(__name__)

class YearFilterStrategy(FilterStrategy):
    def __init__(self, filter_years: list):
        self.filter_years = filter_years
        self.count = 0
        self.dto_helper = TransactionBatchDTO("", BatchType.RAW_CSV)
        

    def should_keep_line(self, csv_line: str) -> bool:
        try:
            created_at = self.dto_helper.get_column_value(csv_line, 'created_at')
            
            if not created_at or len(created_at) < 4:
                return False
            
            year_str = created_at[:4]
            
            if year_str in self.filter_years:
                self.count += 1
                if self.count % 1000 == 0:
                    logger.info(f"YearFilter: {self.count} transacciones pasaron el filtro")
                return True
            return False
            
        except (IndexError, ValueError):
            return False
    
    def get_filter_description(self) -> str:
        return f"Filtro por año: {', '.join(self.filter_years)}"