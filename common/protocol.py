import struct
import socket
import logging
from dataclasses import dataclass
from typing import Generator, Optional, List
from common.processor import BatchResult

logger = logging.getLogger(__name__)

MAX_MESSAGE_SIZE = 65535
ACTION_EXIT = "EXIT"
ACTION_SEND = "SEND"

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
        """Envía un batch de líneas al servidor."""
        try:
            # Unir las líneas con '\n' para formar el contenido del batch
            message_content = "\n".join(batch_result.items)

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
            
            # Enviar el mensaje
            success = self._send_all(bytes(message))
            
            if not success:
                return False
            
            # Esperar el ACK del servidor
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor")
                return False
            
            if ack_response.status != 0:  # 0 = Success
                logger.error(f"Server respondió con error: status={ack_response.status}")
                return False
            
            logger.info(f"Batch enviado y confirmado: {len(batch_result.items)} items, "
                        f"FILE-TYPE: {file_type}, LAST-BATCH: {is_last_batch}, Size: {message_size} bytes")
            
            return True
            
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
            
            if not success:
                return False
            
            # Esperar el ACK del servidor para EXIT
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor para EXIT")
                return False
            
            if ack_response.status != 0:  # 0 = Success
                logger.error(f"Server respondió con error para EXIT: status={ack_response.status}")
                return False
            
            logger.info("Mensaje EXIT enviado")
            return True
            
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
                
                # Enviar ACK ANTES del yield para garantizar que se envíe incluso si hay break
                self._send_ack(self.batch_counter, 0)  # 0 = Success
                self.batch_counter += 1
                
                yield message
                    
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

    # -------------------- COMMON METHODS --------------------

    def _flush_socket_buffer(self):
        """Limpia cualquier dato pendiente en el buffer del socket."""
        try:
            self.conn.settimeout(0.1)  # Timeout muy corto
            while True:
                try:
                    data = self.conn.recv(1024)
                    if not data:
                        break
                    logger.info(f"DEBUG: Descartando datos residuales del buffer: {len(data)} bytes")
                except socket.timeout:
                    break
        except Exception as e:
            logger.info(f"DEBUG: Error limpiando buffer (normal): {e}")
        finally:
            self.conn.settimeout(None)  # Restaurar timeout original

    def _receive_ack(self) -> Optional[AckResponse]:
        """Recibe un ACK del servidor."""
        try:
            # Leer ACK header (4 bytes: 'ACK|')
            ack_header = self._recv_all(4)
            if not ack_header or ack_header != b'ACK|':
                logger.error(f"ACK header inválido: {ack_header}")
                return None
            
            # Leer batch_id (4 bytes)
            batch_id_bytes = self._recv_all(4)
            if not batch_id_bytes:
                return None
            batch_id = struct.unpack('>I', batch_id_bytes)[0]
            
            # Leer status (1 byte)
            status_bytes = self._recv_all(1)
            if not status_bytes:
                return None
            status = struct.unpack('B', status_bytes)[0]
            
            return AckResponse(batch_id=batch_id, status=status)
            
        except Exception as e:
            logger.error(f"Error recibiendo ACK: {e}")
            return None

    def send_response_message(self, action: str, file_type: str, data: str, is_last_batch: bool = True) -> bool:
        """Envía un mensaje de respuesta del servidor al cliente (como un reporte)."""
        try:
            message_size = len(data) if data else 0
            
            if message_size > MAX_MESSAGE_SIZE:
                logger.error(f"Mensaje de respuesta demasiado largo: {message_size} bytes")
                return False
            
            # Crear el mensaje: [ACTION(4)] + [FILE-TYPE(1)] + [SIZE(4)] + [LAST-BATCH(1)] + [DATA(N)]
            message = bytearray()
            
            # ACTION (4 bytes, padded with nulls)
            action_bytes = action.ljust(4, '\x00').encode('utf-8')
            message.extend(action_bytes)
            
            # FILE-TYPE (1 byte)
            file_type_bytes = file_type.encode('utf-8')
            message.extend(file_type_bytes)
            
            # SIZE (4 bytes, big endian)
            size_bytes = struct.pack('>I', message_size)
            message.extend(size_bytes)
            
            # LAST-BATCH (1 byte)
            last_batch_bytes = struct.pack('B', 1 if is_last_batch else 0)
            message.extend(last_batch_bytes)
            
            # DATA (N bytes)
            if data:
                data_bytes = data.encode('utf-8')
                message.extend(data_bytes)
            
            # Enviar el mensaje
            success = self._send_all(bytes(message))
            
            if not success:
                logger.error(f"Error enviando mensaje de respuesta: action={action}, file_type={file_type}")
                return False
            
            # Esperar ACK del cliente (solo para batches de datos, no para EOF)
            if action != "EOF":
                ack_response = self._receive_ack()
                if ack_response is None:
                    logger.error("No se recibió ACK del cliente")
                    return False
                
                if ack_response.status != 0:  # 0 = Success
                    logger.error(f"Cliente respondió con error: status={ack_response.status}")
                    return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error enviando mensaje de respuesta: {e}")
            return False

    def send_response_batches(self, action: str, file_type: str, data: str, max_lines_per_batch: int = 150) -> bool:
        """Envía un mensaje de respuesta grande dividido en batches por líneas, terminando con EOF."""
        try:
            if not data:
                # Si no hay datos, enviar solo EOF
                return self.send_response_message("EOF", "R", "EOF:1", is_last_batch=True)
            
            # Dividir el CSV en líneas
            lines = data.strip().split('\n')
            total_lines = len(lines)
            
            if total_lines == 0:
                return self.send_response_message("EOF", "R", "EOF:1", is_last_batch=True)
            
            lines_sent = 0
            batch_number = 1
            
            # Enviar todos los batches de datos (nunca last_batch=True)
            while lines_sent < total_lines:
                # Calcular las líneas del batch actual
                end_line = min(lines_sent + max_lines_per_batch, total_lines)
                batch_lines = lines[lines_sent:end_line]
                batch_content = '\n'.join(batch_lines)
                
                # Verificar que el batch no sea demasiado grande
                if len(batch_content) > MAX_MESSAGE_SIZE:
                    logger.error(f"Batch {batch_number} demasiado grande: {len(batch_content)} bytes")
                    return False
                
                # Enviar batch (siempre is_last_batch=False para datos)
                if not self.send_response_message(action, file_type, batch_content, is_last_batch=False):
                    logger.error(f"Error enviando batch {batch_number}")
                    return False
                
                lines_sent = end_line
                batch_number += 1
            
            # Enviar mensaje EOF al final
            if not self.send_response_message("EOF", "R", "EOF:1", is_last_batch=True):
                logger.error("Error enviando mensaje EOF")
                return False
            
            return True
            
        except Exception as e:
            logger.error(f"Error enviando respuesta en batches: {e}")
            return False

    def _send_ack(self, batch_id: int, status: int):
        """Envía un ACK al cliente."""
        try:
            ack_data = struct.pack('>4sIB', b'ACK|', batch_id, status)
            self.conn.send(ack_data)
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
            


    def send_finish_message(self, file_type: str) -> bool:
        """
        Envía un mensaje FINISH indicando que terminó de procesar todos los archivos de un tipo.
        """
        try:
            message = bytearray()
            
            # ACTION (4 bytes)
            action = "FINI"  # Nuevo action type
            message.extend(action.ljust(4, '\x00').encode('utf-8'))
            
            # FILE-TYPE (1 byte)
            message.extend(file_type.encode('utf-8'))
            
            # SIZE (4 bytes) - 0 
            message.extend(struct.pack('>I', 0))
            
            # LAST-BATCH (1 byte) - true
            message.extend(struct.pack('B', 1))
            
            success = self._send_all(bytes(message))
            
            if not success:
                return False
            
            # Esperar el ACK del servidor para FINISH
            ack_response = self._receive_ack()
            if ack_response is None:
                logger.error("No se recibió ACK del servidor para FINISH")
                return False
            
            if ack_response.status != 0:  # 0 = Success
                logger.error(f"Server respondió con error para FINISH: status={ack_response.status}")
                return False
            
            logger.info(f"Mensaje FINISH enviado para file_type: {file_type}")
            return True
            
        except Exception as e:
            logger.error(f"Error enviando FINISH: {e}")
            return False