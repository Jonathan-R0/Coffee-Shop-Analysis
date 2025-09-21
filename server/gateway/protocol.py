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

    def receive_batch_message(self) -> List[TransactionItem]:
        try:
            # Recibir el flag EOF (1 byte)
            eof_flag = self._recv_all(1)
            if not eof_flag:
                logger.error("No se pudo leer el flag EOF")
                return []
            eof_flag = struct.unpack('B', eof_flag)[0]

            # Leer el tamaño del mensaje (2 bytes, big endian)
            message_size_data = self._recv_all(2)
            if not message_size_data:
                logger.error("No se pudo leer el tamaño del mensaje")
                return []
            message_size = struct.unpack('>H', message_size_data)[0]

            if message_size > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje demasiado grande: {message_size} bytes")
                return []

            # Leer el contenido del mensaje
            message_content = self._recv_all(message_size)
            if not message_content:
                logger.error("No se pudo leer el contenido del mensaje")
                return []

            decoded_message = message_content.decode('utf-8')
            logger.info(f"Mensaje recibido: {decoded_message}")

            transaction_items = self._parse_batch(decoded_message)
            logger.info(f"Batch parseado: {len(transaction_items)} items")

            if eof_flag == 0x01:
                logger.info("Se recibió el último batch (EOF)")

            return transaction_items

        except Exception as e:
            logger.error(f"Error recibiendo batch: {e}")
            return []

    def _recv_all(self, num_bytes: int) -> bytes:
        data = bytearray()
        while len(data) < num_bytes:
            packet = self.conn.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def _parse_batch(self, batch_data: str) -> List[TransactionItem]:
        items = []
        try:
            lines = batch_data.split("\n")
            for line in lines:
                if not line.strip():
                    continue
                fields = line.split(";")
                if len(fields) != 6:
                    logger.warning(f"Formato inválido en línea: {line}")
                    continue
                try:
                    item = TransactionItem(
                        transaction_id=fields[0],
                        item_id=int(fields[1]),
                        quantity=int(fields[2]),
                        unit_price=float(fields[3]),
                        subtotal=float(fields[4]),
                        created_at=fields[5]
                    )
                    items.append(item)
                except (ValueError, IndexError) as e:
                    logger.warning(f"Error parseando línea: {line}, Error: {e}")
        except Exception as e:
            logger.error(f"Error parseando batch: {e}")
        return items

    def close(self):
        try:
            self.conn.close()
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")