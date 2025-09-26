import socket
import logging
import os
from common.processor import TransactionCSVProcessor, TransactionItem, BatchResult
from common.protocol import Protocol

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
            logging.info(f"Conectando al servidor en el puerto {self.server_port}")
            self.client_socket = socket.create_connection(('gateway', self.server_port))
            
            self.protocol = Protocol(self.client_socket)

            self.process_and_send_files_from_volumes()

        except socket.error as err:
            logging.error(f"Error de socket: {err}")
        except Exception as err:
            logging.error(f"Error inesperado: {err}")
        finally:
            self._cleanup()
    
    def process_and_send_files_from_volumes(self):
        """Procesa y envía archivos desde las carpetas montadas como volúmenes."""
        mounted_folders = {
            "D": "/data/transaction_items",
            #"users": "/data/users",
            #"stores": "/data/stores",
            #"menu_items": "/data/menu_items",
            #"payment_methods": "/data/payment_methods",
            #"vouchers": "/data/vouchers"
        }

        for file_type, folder_path in mounted_folders.items():
            if not os.path.exists(folder_path):
                logging.warning(f"La carpeta {folder_path} no existe. Saltando...")
                continue

            files = [f for f in os.listdir(folder_path) if f.endswith(".csv")]
            if not files:
                logging.info(f"No se encontraron archivos en {folder_path}.")
                continue

            logging.info(f"Procesando archivos en {folder_path}: {files}")

            # Procesar y enviar cada archivo
            for file in files:
                file_path = os.path.join(folder_path, file)
                self.send_data(file_path, file_type)
    
    def send_data(self, csv_filepath, file_type):
        """Procesa y envía un archivo CSV con su file_type."""
        try:
            logging.info(f"Iniciando procesamiento de {csv_filepath} con file_type {file_type}")
            self.processor = TransactionCSVProcessor(csv_filepath, self.max_batch_size)
            
            batch_counter = 0
            total_items_sent = 0
            
            while self.processor.has_more_batches() and self.keep_running:
                batch_result = self.processor.read_next_batch()
                
                if not batch_result.items:
                    logging.info("Batch vacío, terminando...")
                    break
                
                batch_counter += 1
                
                # Enviar el batch con el file_type al protocolo
                if self.protocol.send_batch_message(batch_result, file_type):
                    total_items_sent += len(batch_result.items)
                    logging.info(f"Batch {batch_counter} enviado: {len(batch_result.items)} items")
                else:
                    logging.error(f"Error enviando batch {batch_counter}, deteniendo...")
                    break
                
                if batch_result.is_eof:
                    logging.info("Último batch procesado")
                    break
            
            # Enviar mensaje de salida para indicar el fin de la transmisión
            if self.protocol:
                self.protocol.send_exit_message()
            
            logging.info(f"Procesamiento completado para {csv_filepath}:")
            logging.info(f"  - Batches enviados: {batch_counter}")
            logging.info(f"  - Items totales: {total_items_sent}")
            
        except Exception as e:
            logging.error(f"Error en send_data: {e}")
            raise
    
    def _cleanup(self):
        """Limpieza de recursos al finalizar."""
        self.keep_running = False
        
        if self.processor:
            self.processor.close()
            logging.info("Procesador CSV cerrado")
        
        if self.protocol:
            self.protocol.close()
        elif self.client_socket:
            self.client_socket.close()
            
        logging.info("Cliente cerrado completamente")
