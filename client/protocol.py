import struct
import socket
import logging
from typing import List
from processor import BatchResult, TransactionItem

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535
EOF_FLAG_TRUE = 0x01
EOF_FLAG_FALSE = 0x00

class Protocol:    
    def __init__(self, conn: socket.socket):
        self.conn = conn
    

    def send_batch_message(self, batch_result: BatchResult) -> bool:
        try:
            batch_lines = []
            for item in batch_result.items:
                line = f"{item.transaction_id};{item.item_id};{item.quantity};"
                line += f"{item.unit_price};{item.subtotal};{item.created_at}"
                batch_lines.append(line)
            
            message_content = "\n".join(batch_lines)
            
            if len(message_content) > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje demasiado largo: {len(message_content)} bytes")
                return False
            
            message_size = len(message_content)
            
            eof_flag = EOF_FLAG_TRUE if batch_result.is_eof else EOF_FLAG_FALSE
            
            message = bytearray()
            message.append(eof_flag)                                    # 1 byte: EOF flag
            message.extend(struct.pack('>H', message_size))             # 2 bytes: tamaño (big endian)
            message.extend(message_content.encode('utf-8'))             # N bytes: contenido
            
            success = self._send_all(bytes(message))
            
            if success:
                logger.info(f"Batch enviado: {len(batch_result.items)} items, "
                          f"EOF: {batch_result.is_eof}, Size: {message_size} bytes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False
    
    def _send_all(self, data: bytes) -> bool:
        try:
            total_sent = 0
            while total_sent < len(data):
                sent = self.conn.send(data[total_sent:])
                if sent == 0:
                    logger.error("Conexión cerrada durante envío")
                    return False
                total_sent += sent
            return True
            
        except socket.error as e:
            logger.error(f"Error de socket enviando datos: {e}")
            return False
        except Exception as e:
            logger.error(f"Error inesperado enviando datos: {e}")
            return False
    
    
    def close(self):
        try:
            self.conn.close()
            logger.info("Conexión del protocolo cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")