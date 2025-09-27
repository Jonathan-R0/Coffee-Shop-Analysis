import os
import socket
import logging
from common.processor import TransactionCSVProcessor
from common.protocol import Protocol

logger = logging.getLogger(__name__)

class Client:
    def __init__(self, server_port, max_batch_size):
        self.server_port = server_port
        self.max_batch_size = int(max_batch_size)
        self.keep_running = False
        self.client_socket = None
        self.protocol = None  
        self.processor = None
    
    def run(self):
        self.keep_running = True
        try:
            logger.info(f"Conectando al servidor en el puerto {self.server_port}")
            self.client_socket = socket.create_connection(('gateway', self.server_port))
            
            self.protocol = Protocol(self.client_socket)

            self.process_and_send_files_from_volumes()

        except socket.error as err:
            logger.error(f"Error de socket: {err}")
        except Exception as err:
            logger.error(f"Error inesperado: {err}")
        finally:
            self._cleanup()
    
    def process_and_send_files_from_volumes(self):
        """Procesa y envía archivos desde las carpetas montadas como volúmenes."""
        mounted_folders = {
            "D": "/data/transactions_test",
            #"D": "/data/transactions",
            #"D": "/data/transaction_items",
            #"users": "/data/users",
            #"stores": "/data/stores",
            #"menu_items": "/data/menu_items",
            #"payment_methods": "/data/payment_methods",
            #"vouchers": "/data/vouchers"
        }

        try:
            for file_type, folder_path in mounted_folders.items():
                if not os.path.exists(folder_path):
                    logging.warning(f"La carpeta {folder_path} no existe. Saltando...")
                    continue

                files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
                if not files:
                    logging.info(f"No se encontraron archivos en {folder_path}.")
                    continue

                logging.info(f"Procesando archivos en {folder_path}: {files}")

                for file in files:
                    file_path = os.path.join(folder_path, file)
                    self.send_data(file_path, file_type)
            
            if self.protocol:
                self.protocol.send_exit_message()

        except Exception as e:
            logger.error(f"Error procesando archivos desde volúmenes: {e}")
            raise
    
    def send_data(self, csv_filepath, file_type):
        """Procesa y envía un archivo CSV línea por línea."""
        try:
            logger.info(f"Iniciando procesamiento de {csv_filepath} con file_type {file_type}")
            self.processor = TransactionCSVProcessor(csv_filepath, self.max_batch_size)
            
            while self.processor.has_more_batches() and self.keep_running:
                batch_result = self.processor.read_next_batch()
                
                if not batch_result.items:
                    logger.info("Batch vacío, terminando...")
                    break
                
                success = self.protocol.send_batch_message(batch_result, file_type)
                
                if not success:
                    logger.error("Error enviando batch o no se recibió ACK. Deteniendo el envío.")
                    break
            
            logger.info(f"Procesamiento completado para {csv_filepath}")
            
        except Exception as e:
            logger.error(f"Error en send_data: {e}")
            raise
    
    def _cleanup(self):
        """Limpieza de recursos al finalizar."""
        self.keep_running = False
        
        if self.processor:
            self.processor.close()
            logger.info("Procesador CSV cerrado")
        
        if self.protocol:
            self.protocol.close()
        elif self.client_socket:
            self.client_socket.close()
            
        logger.info("Cliente cerrado completamente")
