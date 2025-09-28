import logging
import os
import json
import csv
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareQueue
from dtos.dto import TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'final_data')
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.input_queue
        )
        
        self.items_processed = []
        self.csv_file = None
        self.csv_writer = None
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Queue entrada: {self.input_queue}")

    def process_message(self, message: bytes):
        """
        Procesa un batch de transacciones o una señal de control.
        """
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)

            if dto.batch_type == "CONTROL":
                logger.info("Señal de finalización recibida. Cerrando archivo y finalizando.")
                self._close_csv_file() 
                self._cleanup()  
                return
            elif dto.batch_type == "EOF":
                logger.info(f"Mensaje EOF recibido: {dto.transactions}. Cerrando archivo y finalizando.")
                self._close_csv_file() 
                self._cleanup()  
                return

            self._write_to_csv(dto.transactions)
        
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}. Mensaje recibido: {message}")

    def save_report_to_file(self):
        """
        Guarda el reporte en un archivo CSV.
        """
        try:
            if not self.items_processed:
                logger.warning("No hay datos procesados para guardar en el reporte.")
                return

            headers = self.items_processed[0].keys()

            reports_dir = './reports'  
            os.makedirs(reports_dir, exist_ok=True)
            os.chmod(reports_dir, 0o755)  

            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{reports_dir}/coffee_shop_report_{timestamp}.csv"

            with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=headers)
                writer.writeheader()  
                writer.writerows(self.items_processed)  

            os.chmod(filename, 0o644)

            logger.info(f"Reporte CSV guardado en: {filename}")

        except Exception as e:
            logger.error(f"Error guardando reporte CSV: {e}")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje.
        """
        try:
            self.process_message(body)  
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}")

    def start(self):
        """
        Inicia el ReportGenerator.
        """
        logger.info("Iniciando ReportGenerator...")
        logger.info(f"Consumiendo de: {self.input_queue}")
        
        try:
            self._initialize_csv_file()  
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self._close_csv_file()  
            self._cleanup()

    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

    def _initialize_csv_file(self):
        """
        Inicializa el archivo CSV y escribe los encabezados.
        """
        try:
            reports_dir = './reports'
            os.makedirs(reports_dir, exist_ok=True)
            os.chmod(reports_dir, 0o755)

            self.csv_filename = f"{reports_dir}/query1.csv"

            self.csv_file = open(self.csv_filename, 'w', newline='', encoding='utf-8')
            self.csv_writer = None
            logger.info(f"Archivo CSV inicializado: {self.csv_filename}")

        except Exception as e:
            logger.error(f"Error inicializando el archivo CSV: {e}")
            raise

    def _write_to_csv(self, transactions):
        """
        Escribe las transacciones en el archivo CSV, incluyendo solo las columnas necesarias.
        """
        try:
            # Inicializar CSV writer si es necesario
            if not self.csv_writer:
                headers = ["transaction_id", "final_amount"]
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=headers)
                self.csv_writer.writeheader()

            # Manejar tanto CSV raw como lista de diccionarios
            if isinstance(transactions, str):
                # Es CSV raw, procesarlo línea por línea
                lines = transactions.strip().split('\n')
                for line in lines:
                    if line.strip():  # Evitar líneas vacías
                        values = line.split(',')
                        if len(values) >= 8:  # Verificar que tiene suficientes columnas
                            transaction_id = values[0]
                            final_amount = values[7]  # final_amount está en la columna 7
                            self.csv_writer.writerow({
                                "transaction_id": transaction_id,
                                "final_amount": final_amount
                            })
            else:
                # Es lista de diccionarios
                filtered_transactions = [
                    {"transaction_id": transaction["transaction_id"], "final_amount": transaction["final_amount"]}
                    for transaction in transactions
                ]
                self.csv_writer.writerows(filtered_transactions)

        except Exception as e:
            logger.error(f"Error escribiendo en el archivo CSV: {e}")
            raise

    def _close_csv_file(self):
        """
        Cierra el archivo CSV si está abierto.
        """
        try:
            if hasattr(self, 'csv_file') and self.csv_file:
                self.csv_file.close()
                os.chmod(self.csv_filename, 0o644)
                logger.info(f"Archivo CSV cerrado: {self.csv_filename}")
        except Exception as e:
            logger.error(f"Error cerrando el archivo CSV: {e}")

if __name__ == "__main__":
    report_generator = ReportGenerator()
    report_generator.start()
