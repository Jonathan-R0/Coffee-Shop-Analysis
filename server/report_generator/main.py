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
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Queue entrada: {self.input_queue}")

    def process_message(self, message: bytes):
        """
        Procesa un batch de transacciones en formato binario y las almacena para generar el reporte.
        """
        try:
            dto = TransactionBatchDTO.from_bytes(message)
            logger.info(f"Batch recibido con {len(dto.transactions)} transacciones")

            self.items_processed.extend(dto.transactions)
            logger.info(f"Procesadas {len(dto.transactions)} transacciones. Total acumulado: {len(self.items_processed)}")
    
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}.  Mensaje recibido: {message}")

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
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            if self.items_processed:
                logger.info("Generando reporte final con datos pendientes...")
                self.save_report_to_file()
            self._cleanup()

    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

if __name__ == "__main__":
    report_generator = ReportGenerator()
    report_generator.start()
