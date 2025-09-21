import csv
import logging
from typing import List, Dict, Optional
from dataclasses import dataclass

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

@dataclass
class TransactionItem:
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str

@dataclass
class BatchResult:
    items: List[TransactionItem]
    is_eof: bool

class TransactionCSVProcessor:
    
    def __init__(self, csv_filepath: str, batch_size: int = 100):
        self.csv_filepath = csv_filepath
        self.batch_size = batch_size
        self.is_eof = False
        self.file = None
        self.reader = None
        
        self._open_file()
    
    def _open_file(self):
        try:
            self.file = open(self.csv_filepath, 'r', encoding='utf-8')
            self.reader = csv.DictReader(self.file)
            logger.info(f"Archivo CSV abierto: {self.csv_filepath}")
            logger.info(f"Headers detectados: {self.reader.fieldnames}")
        except Exception as e:
            logger.error(f"Error abriendo archivo: {e}")
            raise
    
    def has_more_batches(self) -> bool:
        return not self.is_eof
    
    def read_next_batch(self) -> BatchResult:

        if self.is_eof:
            return BatchResult(items=[], is_eof=True)
        
        items = []
        
        try:
            for _ in range(self.batch_size):
                try:
                    row = next(self.reader)
                    item = self._parse_row(row)
                    if item:
                        items.append(item)
                except StopIteration:
                    self.is_eof = True
                    break
                    
        except Exception as e:
            logger.error(f"Error leyendo batch: {e}")
            return BatchResult(items=[], is_eof=True)
        
        logger.info(f"Batch leído: {len(items)} items")
        return BatchResult(items=items, is_eof=self.is_eof)
    
    def _parse_row(self, row: Dict[str, str]) -> Optional[TransactionItem]:
        try:
            item = TransactionItem(
                transaction_id=row['transaction_id'],
                item_id=int(row['item_id']),
                quantity=int(row['quantity']),
                unit_price=float(row['unit_price']),
                subtotal=float(row['subtotal']),
                created_at=row['created_at']
            )
            return item
            
        except (ValueError, KeyError) as e:
            logger.warning(f"Error parseando fila: {e}")
            return None
    
    def close(self):
        if self.file:
            self.file.close()
            logger.info("Archivo CSV cerrado")
    