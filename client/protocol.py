import struct
import socket
import logging
from typing import List
from processor import BatchResult, TransactionItem

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535

class Protocol:    
    def __init__(self, conn: socket.socket):
        self.conn = conn
    
    def send_batch_message(self, batch_result: BatchResult) -> bool:
        try:
            batch_lines = []
            for item in batch_result.items:
                line = f"{item.transaction_id},{item.item_id},{item.quantity},"
                line += f"{item.unit_price},{item.subtotal},{item.created_at}"
                batch_lines.append(line)
            
            message_content = "\n".join(batch_lines)
            
            if len(message_content) > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje demasiado largo: {len(message_content)} bytes")
                return False
            
            message_size = len(message_content.encode('utf-8'))
            
            # ACTION(4) + FILE-TYPE(1) + SIZE(4) + LAST-BATCH(1) + DATA(N)
            message = bytearray()
            
            # ACTION (4 bytes): "SEND" as fixed 4-byte field
            action_bytes = b'SEND'  # 4 bytes exactly
            message.extend(action_bytes)
            
            # FILE-TYPE (1 byte): 'D' for transaction Items
            message.extend(b'D')
            
            # SIZE (4 bytes, big endian)
            message.extend(struct.pack('>I', message_size))
            
            # LAST-BATCH (1 byte): 1 if EOF, 0 otherwise
            last_batch_flag = 1 if batch_result.is_eof else 0
            message.extend(struct.pack('B', last_batch_flag))
            
            # DATA (N bytes)
            message.extend(message_content.encode('utf-8'))
            
            success = self._send_all(bytes(message))
            
            if success:
                logger.info(f"Batch enviado: {len(batch_result.items)} items, "
                          f"EOF: {batch_result.is_eof}, Size: {message_size} bytes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False
    
    def send_exit_message(self) -> bool:
        """Send EXIT message to signal end of transmission"""
        try:
            message = bytearray()
            
            # ACTION (4 bytes): "EXIT"
            action_bytes = b'EXIT'  # 4 bytes exactly
            message.extend(action_bytes)
            
            # FILE-TYPE (1 byte): empty
            message.extend(b'\x00')
            
            # SIZE (4 bytes): 0
            message.extend(struct.pack('>I', 0))
            
            # LAST-BATCH (1 byte): 1 (true)
            message.extend(struct.pack('B', 1))
            
            # No DATA for EXIT message
            
            success = self._send_all(bytes(message))
            
            if success:
                logger.info("EXIT message sent")
            
            return success
            
        except Exception as e:
            logger.error(f"Error sending EXIT message: {e}")
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