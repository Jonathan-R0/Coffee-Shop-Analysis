import socket
import logging
from processor import TransactionCSVProcessor, TransactionItem, BatchResult
from protocol import Protocol

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
            logging.info(f"Conectando al server en puerto {self.server_port}")
            self.client_socket = socket.create_connection(('gateway', self.server_port))
            
            self.protocol = Protocol(self.client_socket)

            self.send_data("transaction_items_202412.csv")

        except socket.error as err:
            logging.error(f"Socket error: {err}")
        except Exception as err:
            logging.error(f"Unexpected error: {err}")
        finally:
            self._cleanup()
            
    def send_data(self, csv_filepath):
        try:
            logging.info(f"Iniciando procesamiento de {csv_filepath}")
            self.processor = TransactionCSVProcessor(csv_filepath, self.max_batch_size)
            
            batch_counter = 0
            total_items_sent = 0
            
            while self.processor.has_more_batches() and self.keep_running:
                batch_result = self.processor.read_next_batch()
                
                if not batch_result.items:
                    logging.info("Batch vacío, terminando...")
                    break
                
                batch_counter += 1
                
                if self.protocol.send_batch_message(batch_result):
                    total_items_sent += len(batch_result.items)
                    logging.info(f"Batch {batch_counter} enviado: {len(batch_result.items)} items")
                else:
                    logging.error(f"Error enviando batch {batch_counter}, deteniendo...")
                    break
                
                if batch_result.is_eof:
                    logging.info("Último batch procesado")
                    break
            
            # Send EXIT message to signal end of transmission
            if self.protocol:
                self.protocol.send_exit_message()
            
            logging.info(f"Procesamiento completado:")
            logging.info(f"  - Batches enviados: {batch_counter}")
            logging.info(f"  - Items totales: {total_items_sent}")
            
        except Exception as e:
            logging.error(f"Error en send_data: {e}")
            raise
    
    def _cleanup(self):
        self.keep_running = False
        
        if self.processor:
            self.processor.close()
            logging.info("Procesador CSV cerrado")
        
        if self.protocol:
            self.protocol.close()
        elif self.client_socket:
            self.client_socket.close()
            
        logging.info("Cliente cerrado completamente")
