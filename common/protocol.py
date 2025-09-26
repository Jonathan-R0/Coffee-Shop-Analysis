import struct
import socket
import logging
from dataclasses import dataclass
from typing import Generator, Optional, List
from common.processor import BatchResult, TransactionItem
from common.entities import BaseEntity, get_entity_class, get_entity_name

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535

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

class Protocol:
    def __init__(self, conn: socket.socket):
        self.conn = conn
        self.batch_counter = 0

    # -------------------- CLIENT METHODS --------------------

    def send_batch_message(self, batch_result: BatchResult, file_type: str) -> bool:
        try:
            batch_lines = []
            for item in batch_result.items:
                # Usar comas como separador (según TransactionItem.parse_line())
                line = f"{item.transaction_id},{item.item_id},{item.quantity},"
                line += f"{item.unit_price},{item.subtotal},{item.created_at}"
                batch_lines.append(line)
            
            message_content = "\n".join(batch_lines)
            
            if len(message_content) > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje demasiado largo: {len(message_content)} bytes")
                return False
            
            message_size = len(message_content)
            is_last_batch = batch_result.is_eof
            
            # Crear el mensaje: [ACTION(4)] + [FILE-TYPE(1)] + [SIZE(4)] + [LAST-BATCH(1)] + [DATA(N)]
            message = bytearray()
            
            # ACTION (4 bytes, padded with nulls)
            action = "SEND"
            message.extend(action.ljust(4, '\x00').encode('utf-8'))
            
            # FILE-TYPE (1 byte)
            message.extend(file_type.encode('utf-8'))
            
            # SIZE (4 bytes, big endian)
            message.extend(struct.pack('>I', message_size))
            
            # LAST-BATCH (1 byte)
            message.extend(struct.pack('B', 1 if is_last_batch else 0))
            
            # DATA (N bytes)
            message.extend(message_content.encode('utf-8'))
            
            success = self._send_all(bytes(message))
            
            if success:
                logger.info(f"Batch enviado: {len(batch_result.items)} items, "
                            f"FILE-TYPE: {file_type}, LAST-BATCH: {is_last_batch}, Size: {message_size} bytes")
            
            return success
            
        except Exception as e:
            logger.error(f"Error enviando batch: {e}")
            return False

    def send_exit_message(self) -> bool:
        """Envía un mensaje EXIT para cerrar la conexión."""
        try:
            # EXIT message: [ACTION(4)] + [FILE-TYPE(1)] + [SIZE(4)] + [LAST-BATCH(1)]
            message = bytearray()
            
            # ACTION (4 bytes)
            action = "EXIT"
            message.extend(action.ljust(4, '\x00').encode('utf-8'))
            
            # FILE-TYPE (1 byte) - 'D' para consistencia (aunque no importa para EXIT)
            message.extend(b'D')
            
            # SIZE (4 bytes) - 0 para EXIT
            message.extend(struct.pack('>I', 0))
            
            # LAST-BATCH (1 byte) - true para EXIT
            message.extend(struct.pack('B', 1))
            
            success = self._send_all(bytes(message))
            
            if success:
                logger.info("Mensaje EXIT enviado")
            
            return success
            
        except Exception as e:
            logger.error(f"Error enviando EXIT: {e}")
            return False

    # -------------------- SERVER METHODS --------------------

    def receive_messages(self) -> Generator[ProtocolMessage, None, None]:
        """Recibe mensajes del cliente."""
        try:
            while True:
                message = self._receive_single_message()
                if message is None:
                    break
                
                yield message
                
                self._send_ack(self.batch_counter, 0)  # 0 = Success
                self.batch_counter += 1
                
                if message.action == "EXIT" or message.last_batch:
                    break
                    
        except Exception as e:
            logger.error(f"Error recibiendo mensajes: {e}")
            try:
                self._send_ack(self.batch_counter, 2)  # 2 = Error
            except:
                pass

    def _receive_single_message(self) -> Optional[ProtocolMessage]:
        """Recibe un único mensaje del cliente."""
        try:
            # Leer ACTION (4 bytes)
            action_bytes = self._recv_all(4)
            if not action_bytes:
                return None
            action = action_bytes.decode('utf-8').rstrip('\x00')

            if action == "EXIT":
                return ProtocolMessage(action="EXIT", file_type="", size=0, last_batch=True, data="")

            # Leer FILE-TYPE (1 byte)
            file_type_bytes = self._recv_all(1)
            if not file_type_bytes:
                return None
            file_type = file_type_bytes.decode('utf-8')

            # Leer SIZE (4 bytes, big endian)
            size_bytes = self._recv_all(4)
            if not size_bytes:
                return None
            size = struct.unpack('>I', size_bytes)[0]

            # Leer LAST-BATCH (1 byte)
            last_batch_bytes = self._recv_all(1)
            if not last_batch_bytes:
                return None
            last_batch = struct.unpack('B', last_batch_bytes)[0] == 1

            # Leer DATA (size bytes)
            data = ""
            if size > 0:
                data_bytes = self._recv_all(size)
                if not data_bytes:
                    return None
                data = data_bytes.decode('utf-8')

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
        """Parsea entidades desde un mensaje recibido."""
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
        """Parsea elementos de transacción desde un mensaje recibido."""
        for entity in self.parse_entities(message):
            if isinstance(entity, TransactionItem):
                yield entity

    def parse_data_by_type(self, message: ProtocolMessage) -> Generator[str, None, None]:
        """Parsea datos genéricos desde un mensaje recibido."""
        if not message.data.strip():
            return

        lines = message.data.strip().split('\n')
        for line in lines:
            if line.strip():
                yield line.strip()

    @staticmethod
    def get_entity_type_name(file_type: str) -> str:
        """Obtiene el nombre del tipo de entidad basado en el file_type."""
        try:
            return get_entity_name(file_type)
        except KeyError:
            return f'Unknown_{file_type}'

    # -------------------- COMMON METHODS --------------------

    def _send_ack(self, batch_id: int, status: int):
        """Envía un ACK al cliente."""
        try:
            ack_data = struct.pack('>4sIB', b'ACK|', batch_id, status)
            self.conn.send(ack_data)
            logger.info(f"ACK enviado: batch_id={batch_id}, status={status}")
        except Exception as e:
            logger.error(f"Error enviando ACK: {e}")

    def _send_all(self, data: bytes) -> bool:
        """Envía todos los datos al socket."""
        try:
            total_sent = 0
            while total_sent < len(data):
                sent = self.conn.send(data[total_sent:])
                if sent == 0:
                    logger.error("Conexión cerrada durante envío")
                    return False
                total_sent += sent
            return True
        except Exception as e:
            logger.error(f"Error enviando datos: {e}")
            return False

    def _recv_all(self, num_bytes: int) -> bytes:
        """Recibe exactamente `num_bytes` del socket."""
        data = bytearray()
        while len(data) < num_bytes:
            packet = self.conn.recv(num_bytes - len(data))
            if not packet:
                return None
            data.extend(packet)
        return bytes(data)

    def close(self):
        """Cierra la conexión del socket."""
        try:
            self.conn.close()
            logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")