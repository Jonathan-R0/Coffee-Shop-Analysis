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
        self.reports_ammount = 2
    
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
            #"D": "/data/transactions_test",
            #"D": "/data/transaction_items",
            #"users": "/data/users",
            "S": "/data/stores",
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
                self.protocol.send_finish_message(file_type)
            
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
            # Crear directorio reports si no existe
            report_counter = 0
            reports_dir = "/reports"
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
                logger.info("Esperando mensaje del servidor...")
                response = self.protocol._receive_single_message()
                # logger.info(f"Respuesta recibida: {response}")
                
                if response is None:
                    logger.warning("Conexión cerrada por el servidor o response None")
                    # Si tenemos datos, guardarlos aunque no hayamos recibido EOF
                    if report_content:
                        logger.info(f"Guardando reporte incompleto con {batch_count} batches recibidos")
                        self._save_report(report_content, reports_dir)
                    break
                
                # logger.info(f"Respuesta recibida: {response}")
                
                if response.action == "RPRT":
                    batch_count += 1
                    
                    # Agregar el contenido del batch
                    if response.data:
                        report_content.append(response.data)
                    
                    # Enviar ACK al servidor
                    self.protocol._send_ack(batch_count, 0)  # 0 = Success
                        
                elif response.action == "EOF":
                    logger.info(f"EOF recibido. Total batches: {batch_count}")
                    if report_content:
                        logger.info(f"Guardando reporte con {report_content} ")
                        self._save_report(report_content, reports_dir,f"report_{report_counter+1}.csv")
                        report_counter += 1
                        report_content = []  # Reset para el próximo reporte
                        if report_counter >= self.reports_ammount:
                            logger.info(f"Se recibieron y guardaron {report_counter} reportes. Terminando recepción.")
                            break
                        else:
                            logger.info(f"Esperando más reportes... ({report_counter}/{self.reports_ammount})")
                            continue
                    else:
                        logger.warning("EOF recibido pero no hay contenido del reporte")
                    # No enviar ACK para EOF
                    
                    break
                        
                elif response.action == "ERRO":
                    logger.error(f"Error del servidor: {response.data}")
                    break
                else:
                    logger.warning(f"Mensaje inesperado del servidor: action={response.action}, data={response.data}")
                    
        except Exception as e:
            logger.error(f"Error recibiendo reporte: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
    
    def _save_report(self, report_batches, reports_dir,query_name):
        """Guarda el reporte completo combinando todos los batches y transformándolo a CSV."""
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
            
            # Combinar todos los batches en datos raw
            raw_data = '\n'.join(report_batches)
            logger.info(f"Datos raw combinados - tamaño: {len(raw_data)} caracteres")
            
            # Transformar a CSV
            csv_content = self._convert_raw_data_to_csv(raw_data)
            logger.info(f"Datos transformados a CSV - tamaño: {len(csv_content)} caracteres")
            
            # Guardar en archivo
            report_path = os.path.join(reports_dir, query_name)
            logger.info(f"Intentando escribir archivo: {report_path}")
            
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(csv_content)
                f.flush()  # Asegurar que se escriba inmediatamente
            
            # Verificar que el archivo se creó
            if os.path.exists(report_path):
                file_size = os.path.getsize(report_path)
                logger.info(f"Archivo creado exitosamente: {report_path}")
                logger.info(f"Tamaño del archivo: {file_size} bytes")
            else:
                logger.error(f"El archivo no se creó: {report_path}")
            
            # Contar líneas para logging
            lines = len(csv_content.split('\n')) if csv_content else 0
            
            logger.info(f"Reporte guardado en {report_path}")
            logger.info(f"Total líneas en el reporte: {lines}")
            logger.info(f"Tamaño del contenido: {len(csv_content)} bytes")
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")

    def _convert_raw_data_to_csv(self, raw_data):
        """Convierte los datos raw a formato CSV con headers."""
        try:
            logger.info("Iniciando conversión de datos raw a CSV")
            
            if not raw_data or not raw_data.strip():
                logger.warning("Datos raw vacíos")
                return "transaction_id,final_amount\n"
            
            lines = raw_data.strip().split('\n')
            logger.info(f"Procesando {len(lines)} líneas de datos raw")
            
            # Agregar header CSV
            csv_lines = []
            
            valid_lines = 0
            for line in lines:
                line = line.strip()
                if line:  # Solo procesar líneas no vacías
                    csv_lines.append(line)
                    valid_lines += 1
            
            logger.info(f"Agregadas {valid_lines} líneas válidas al CSV")
            
            result = '\n'.join(csv_lines)
            logger.info(f"CSV generado con {len(csv_lines)} líneas totales (incluyendo header)")
            
            return result
            
        except Exception as e:
            logger.error(f"Error convirtiendo datos raw a CSV: {e}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return "transaction_id,final_amount\nERROR,0"
    
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
