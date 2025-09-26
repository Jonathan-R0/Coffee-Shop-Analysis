from dataclasses import dataclass
import struct
import socket
import logging
from typing import Generator, Optional, Union

@dataclass
class MenuItem:
    item_id: int
    item_name: str
    category: str
    price: float
    is_seasonal: bool
    available_from: str
    available_to: str

@dataclass
class PaymentMethod:
    payment_method_id: int
    payment_method_name: str
    processing_fee: float

@dataclass
class Store:
    store_id: int
    store_name: str
    street: str
    postal_code: str
    city: str
    state: str
    latitude: float

@dataclass
class TransactionItem:
    transaction_id: str
    item_id: int
    quantity: int
    unit_price: float
    subtotal: float
    created_at: str

@dataclass
class Voucher:
    voucher_id: int
    voucher_code: str
    discount_type: str
    discount_value: float
    valid_from: str
    valid_to: str

@dataclass
class User:
    user_id: int
    gender: str
    birthdate: str
    registered_at: str

@dataclass
class Transaction:
    transaction_id: str
    store_id: int
    payment_method_id: int
    voucher_id: int
    user_id: int
    original_amount: float
    discount_applied: float
    final_amount: float
    created_at: str

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

logger = logging.getLogger(__name__)

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

    def parse_entities(self, message: ProtocolMessage) -> Generator[Union[MenuItem, PaymentMethod, Store, TransactionItem, Voucher, User, Transaction], None, None]:
        if not message.data.strip():
            return

        try:
            lines = message.data.strip().split('\n')
            for line in lines:
                if not line.strip():
                    continue
                
                entity = None
                if message.file_type == 'A':
                    entity = self._parse_menu_item_line(line)
                elif message.file_type == 'B':
                    entity = self._parse_payment_method_line(line)
                elif message.file_type == 'C':
                    entity = self._parse_store_line(line)
                elif message.file_type == 'D':
                    entity = self._parse_transaction_item_line(line)
                elif message.file_type == 'E':
                    entity = self._parse_voucher_line(line)
                elif message.file_type == 'F':
                    entity = self._parse_user_line(line)
                elif message.file_type == 'G':
                    entity = self._parse_transaction_line(line)
                else:
                    logger.warning(f"Unknown file type: {message.file_type}")
                    continue
                
                if entity:
                    yield entity
                    
        except Exception as e:
            logger.error(f"Error parsing entities: {e}")

    def parse_transaction_items(self, message: ProtocolMessage) -> Generator[TransactionItem, None, None]:
        for entity in self.parse_entities(message):
            if isinstance(entity, TransactionItem):
                yield entity

    def _parse_menu_item_line(self, line: str) -> Optional[MenuItem]:
        try:
            parts = line.split(',')
            if len(parts) >= 7:
                return MenuItem(
                    item_id=int(parts[0].strip()),
                    item_name=parts[1].strip(),
                    category=parts[2].strip(),
                    price=float(parts[3].strip()),
                    is_seasonal=parts[4].strip().lower() in ('true', '1', 'yes'),
                    available_from=parts[5].strip(),
                    available_to=parts[6].strip()
                )
            else:
                logger.warning(f"Invalid menu item data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing menu item line '{line}': {e}")
            return None

    def _parse_payment_method_line(self, line: str) -> Optional[PaymentMethod]:
        try:
            parts = line.split(',')
            if len(parts) >= 3:
                return PaymentMethod(
                    payment_method_id=int(parts[0].strip()),
                    payment_method_name=parts[1].strip(),
                    processing_fee=float(parts[2].strip())
                )
            else:
                logger.warning(f"Invalid payment method data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing payment method line '{line}': {e}")
            return None

    def _parse_store_line(self, line: str) -> Optional[Store]:
        try:
            parts = line.split(',')
            if len(parts) >= 7:
                return Store(
                    store_id=int(parts[0].strip()),
                    store_name=parts[1].strip(),
                    street=parts[2].strip(),
                    postal_code=parts[3].strip(),
                    city=parts[4].strip(),
                    state=parts[5].strip(),
                    latitude=float(parts[6].strip())
                )
            else:
                logger.warning(f"Invalid store data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing store line '{line}': {e}")
            return None

    def _parse_transaction_item_line(self, line: str) -> Optional[TransactionItem]:
        try:
            parts = line.split(',')
            if len(parts) >= 6:
                return TransactionItem(
                    transaction_id=parts[0].strip(),
                    item_id=int(parts[1].strip()),
                    quantity=int(parts[2].strip()),
                    unit_price=float(parts[3].strip()),
                    subtotal=float(parts[4].strip()),
                    created_at=parts[5].strip()
                )
            else:
                logger.warning(f"Invalid transaction item data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing transaction item line '{line}': {e}")
            return None

    def _parse_voucher_line(self, line: str) -> Optional[Voucher]:
        try:
            parts = line.split(',')
            if len(parts) >= 6:
                return Voucher(
                    voucher_id=int(parts[0].strip()),
                    voucher_code=parts[1].strip(),
                    discount_type=parts[2].strip(),
                    discount_value=float(parts[3].strip()),
                    valid_from=parts[4].strip(),
                    valid_to=parts[5].strip()
                )
            else:
                logger.warning(f"Invalid voucher data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing voucher line '{line}': {e}")
            return None

    def _parse_user_line(self, line: str) -> Optional[User]:
        try:
            parts = line.split(',')
            if len(parts) >= 4:
                return User(
                    user_id=int(parts[0].strip()),
                    gender=parts[1].strip(),
                    birthdate=parts[2].strip(),
                    registered_at=parts[3].strip()
                )
            else:
                logger.warning(f"Invalid user data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing user line '{line}': {e}")
            return None

    def _parse_transaction_line(self, line: str) -> Optional[Transaction]:
        try:
            parts = line.split(',')
            if len(parts) >= 9:
                return Transaction(
                    transaction_id=parts[0].strip(),
                    store_id=int(parts[1].strip()),
                    payment_method_id=int(parts[2].strip()),
                    voucher_id=int(parts[3].strip()),
                    user_id=int(parts[4].strip()),
                    original_amount=float(parts[5].strip()),
                    discount_applied=float(parts[6].strip()),
                    final_amount=float(parts[7].strip()),
                    created_at=parts[8].strip()
                )
            else:
                logger.warning(f"Invalid transaction data: {line}")
                return None
        except (ValueError, IndexError) as e:
            logger.error(f"Error parsing transaction line '{line}': {e}")
            return None

    def parse_data_by_type(self, message: ProtocolMessage) -> Generator[str, None, None]:
        if not message.data.strip():
            return

        lines = message.data.strip().split('\n')
        for line in lines:
            if line.strip():
                yield line.strip()

    @staticmethod
    def get_entity_type_name(file_type: str) -> str:
        type_mapping = {
            'A': 'MenuItem',
            'B': 'PaymentMethod', 
            'C': 'Store',
            'D': 'TransactionItem',
            'E': 'Voucher',
            'F': 'User',
            'G': 'Transaction'
        }
        return type_mapping.get(file_type, f'Unknown_{file_type}')

    def _send_ack(self, batch_id: int, status: int):
        """Send ACK response to client"""
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