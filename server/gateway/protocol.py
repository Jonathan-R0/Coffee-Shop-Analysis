from dataclasses import dataclass
import struct
import socket
from typing import Generator, Optional, Union

from entities import (
    BaseEntity, TransactionItem, 
    get_entity_class, get_entity_name
)
from logger import get_logger

@dataclass
class ProtocolMessage:
    action: str
    file_type: str
    size: int
    last_batch: bool
    data: str

@dataclass
class AckResponse:
    batch_id: int
    status: int  # 0=Success, 1=Retry, 2=Error

logger = get_logger(__name__)

class ServerProtocol:
    def __init__(self, conn: socket.socket):
        self.conn = conn
        self.batch_counter = 0

    def receive_messages(self) -> Generator[ProtocolMessage, None, None]:
        try:
            while True:
                message = self._receive_single_message()
                if message is None:
                    break
                
                yield message
                
                self._send_ack(self.batch_counter, 0) # 0 = Success
                self.batch_counter += 1
                
                if message.action == "EXIT" or message.last_batch:
                    break
                    
        except Exception as e:
            logger.error(f"Error receiving messages: {e}")
            try:
                self._send_ack(self.batch_counter, 2)  # 2 = Error
            except:
                pass

    def _receive_single_message(self) -> Optional[ProtocolMessage]:
        try:
            # Read ACTION (4 bytes as string)
            action_bytes = self._recv_all(4)
            if not action_bytes:
                logger.error("No se pudieron leer los 4 bytes del ACTION")
                return None
            
            logger.info(f"Raw ACTION bytes: {action_bytes} (hex: {action_bytes.hex()})")
            action = action_bytes.decode('utf-8').rstrip('\x00').rstrip('|')

            if action == "EXIT":
                return ProtocolMessage(action="EXIT", file_type="", size=0, last_batch=True, data="")

            # Read FILE-TYPE (1 byte)
            file_type_bytes = self._recv_all(1)
            if not file_type_bytes:
                logger.error("No se pudo leer el FILE-TYPE")
                return None
            file_type = file_type_bytes.decode('utf-8')

            # Read SIZE (4 bytes, big endian)
            size_bytes = self._recv_all(4)
            if not size_bytes:
                logger.error("No se pudo leer el SIZE")
                return None
            size = struct.unpack('>I', size_bytes)[0]

            # Read LAST-BATCH (1 byte)
            last_batch_bytes = self._recv_all(1)
            if not last_batch_bytes:
                logger.error("No se pudo leer el LAST-BATCH flag")
                return None
            last_batch = struct.unpack('B', last_batch_bytes)[0] == 1

            # Read DATA (size bytes)
            if size > 0:
                data_bytes = self._recv_all(size)
                if not data_bytes:
                    logger.error("No se pudieron leer los datos")
                    return None
                data = data_bytes.decode('utf-8')
            else:
                data = ""

            logger.info(f"Received message: ACTION={action}, FILE-TYPE={file_type}, SIZE={size}, LAST-BATCH={last_batch}")
            
            return ProtocolMessage(
                action=action,
                file_type=file_type,
                size=size,
                last_batch=last_batch,
                data=data
            )

        except Exception as e:
            logger.error(f"Error parsing message: {e}")
            return None

    def parse_entities(self, message: ProtocolMessage) -> Generator[BaseEntity, None, None]:
        if not message.data.strip():
            return

        try:
            entity_class = get_entity_class(message.file_type)
            
            lines = message.data.strip().split('\n')
            for line in lines:
                if not line.strip():
                    continue
                
                entity = entity_class.parse_line(line)
                if entity:
                    yield entity
                    
        except KeyError:
            logger.warning(f"Unknown file type: {message.file_type}")
        except Exception as e:
            logger.error(f"Error parsing entities: {e}")


    def parse_transaction_items(self, message: ProtocolMessage) -> Generator[TransactionItem, None, None]:
        for entity in self.parse_entities(message):
            if isinstance(entity, TransactionItem):
                yield entity

    def parse_data_by_type(self, message: ProtocolMessage) -> Generator[str, None, None]:
        if not message.data.strip():
            return

        lines = message.data.strip().split('\n')
        for line in lines:
            if line.strip():
                yield line.strip()

    @staticmethod
    def get_entity_type_name(file_type: str) -> str:
        try:
            return get_entity_name(file_type)
        except KeyError:
            return f'Unknown_{file_type}'


    def _send_ack(self, batch_id: int, status: int):
        try:
            # Pack ACK resp: ACK|<BATCH-ID>|<STATUS>|
            ack_data = struct.pack('>4sIB', b'ACK|', batch_id, status)
            self.conn.send(ack_data)
            logger.info(f"ACK sent: batch_id={batch_id}, status={status}")
        except Exception as e:
            logger.error(f"Error sending ACK: {e}")

    def _handle_exit_action(self):
        logger.info("Acción EXIT recibida. Cerrando conexión.")
        self.close()


    def _recv_all(self, num_bytes: int) -> bytes:
        logger.info(f"Attempting to read {num_bytes} bytes from socket")
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