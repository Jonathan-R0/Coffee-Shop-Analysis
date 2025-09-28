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
            "D": "/data/transactions",
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
                # Después de enviar EXIT, esperar el reporte del servidor
                self._receive_report()

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
    
    def _receive_report(self):
        """Recibe el reporte del servidor en batches y lo guarda."""
        try:
            logger.info("Esperando reporte del servidor...")
            
            # Crear directorio reports si no existe
            reports_dir = "/reports"
            logger.info(f"Creando directorio: {reports_dir}")
            os.makedirs(reports_dir, exist_ok=True)
            
            # Verificar que se creó correctamente
            if os.path.exists(reports_dir):
                logger.info(f"Directorio creado/existe: {reports_dir}")
                logger.info(f"Permisos del directorio: {oct(os.stat(reports_dir).st_mode)[-3:]}")
            else:
                logger.error(f"No se pudo crear el directorio: {reports_dir}")
                return
            
            report_content = []
            batch_count = 0
            
            while True:
                # Recibir un mensaje del servidor
                response = self.protocol._receive_single_message()
                
                if response is None:
                    logger.warning("Conexión cerrada por el servidor")
                    # Si tenemos datos, guardarlos aunque no hayamos recibido EOF
                    if report_content:
                        logger.info(f"Guardando reporte incompleto con {batch_count} batches recibidos")
                        self._save_report(report_content, reports_dir)
                    break
                
                if response.action == "RPRT":
                    batch_count += 1
                    logger.info(f"Recibido batch {batch_count} del reporte: {len(response.data)} bytes")
                    
                    # Agregar el contenido del batch
                    if response.data:
                        report_content.append(response.data)
                        
                elif response.action == "EOF":
                    logger.info(f"EOF recibido. Total batches: {batch_count}")
                    if report_content:
                        self._save_report(report_content, reports_dir)
                    else:
                        logger.warning("EOF recibido pero no hay contenido del reporte")
                    break
                        
                elif response.action == "ERRO":
                    logger.error(f"Error del servidor: {response.data}")
                    break
                else:
                    logger.warning(f"Mensaje inesperado del servidor: {response.action}")
                    
        except Exception as e:
            logger.error(f"Error recibiendo reporte: {e}")
    
    def _save_report(self, report_batches, reports_dir):
        """Guarda el reporte completo combinando todos los batches."""
        try:
            logger.info(f"Iniciando guardado del reporte...")
            logger.info(f"Directorio destino: {reports_dir}")
            logger.info(f"Número de batches a combinar: {len(report_batches)}")
            
            # Verificar que el directorio existe y es escribible
            if not os.path.exists(reports_dir):
                logger.error(f"El directorio {reports_dir} no existe")
                return
            
            if not os.access(reports_dir, os.W_OK):
                logger.error(f"No hay permisos de escritura en {reports_dir}")
                return
            
            # Combinar todos los batches
            full_report = '\n'.join(report_batches)
            logger.info(f"Reporte combinado - tamaño: {len(full_report)} caracteres")
            
            # Guardar en archivo
            report_path = os.path.join(reports_dir, "query1.csv")
            logger.info(f"Intentando escribir archivo: {report_path}")
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(full_report)
                f.flush()  # Asegurar que se escriba inmediatamente
            
            # Verificar que el archivo se creó
            if os.path.exists(report_path):
                file_size = os.path.getsize(report_path)
                logger.info(f"Archivo creado exitosamente: {report_path}")
                logger.info(f"Tamaño del archivo: {file_size} bytes")
            else:
                logger.error(f"El archivo no se creó: {report_path}")
            
            # Contar líneas para logging
            lines = len(full_report.split('\n')) if full_report else 0
            
            logger.info(f"Reporte guardado en {report_path}")
            logger.info(f"Total líneas en el reporte: {lines}")
            logger.info(f"Tamaño del contenido: {len(full_report)} bytes")
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
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
