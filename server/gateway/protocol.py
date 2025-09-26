from dataclasses import dataclass
import struct
import socket
import logging
from typing import List

@dataclass
class TransactionItem:
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str


logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535

class ServerProtocol:
    def __init__(self, conn: socket.socket):
        self.conn = conn

    def receive_client_data(self) -> List[TransactionItem]:
        code_bytes = self._recv_all(4)
        if not code_bytes:
            logger.error("No se pudieron leer los primeros 4 bytes del código de acción")
            return
        
        action_code = struct.unpack('>I', code_bytes)[0]
        # TODO implement whats missing of the protocol based on action_code
        return []

    def _recv_all(self, num_bytes: int) -> bytes:
        data = bytearray()
        while len(data) < num_bytes:
            packet = self.conn.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def close(self):
        try:
            self.conn.close()
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")