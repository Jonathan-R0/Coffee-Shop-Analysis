import logging
import os
from datetime import datetime

from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'final_data')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'reports_exchange')
        self.query_type = os.getenv('QUERY_TYPE', 'query1')
        self.report_exchange = os.getenv('REPORT_EXCHANGE', 'report.exchange')

        
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.report_exchange,
            route_keys=['q1.data', 'q1.eof', 'q3.data', 'q3.eof']
        )
        
        self.csv_files = {}  # Para múltiples archivos
        self.eof_received = set()  # Tracking de EOF
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Exchange salida: {self.output_exchange}")
        logger.info(f"  Tipo de query: {self.query_type}")
        logger.info(f"  Exchange: {self.report_exchange}")

    def process_message(self, message: bytes, routing_key: str):
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)

            if dto.batch_type == "CONTROL":
                logger.info("Señal de finalización recibida. Cerrando archivo y finalizando.")
                self._close_csv_file() 
                self._cleanup()  
                return
            elif dto.batch_type == "EOF":
                logger.info(f"Mensaje EOF recibido: {dto.transactions}. Generando reporte final.")
                self._generate_final_report()
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
            logger.error(f"Error procesando mensaje: {e}")
            return False

    def on_message_callback(self, ch, method, properties, body):
        """Callback para RabbitMQ cuando llega un mensaje."""
        try:
            routing_key = method.routing_key  # ✅ Obtener routing key
            should_stop = self.process_message(body, routing_key)  # ✅ Pasar routing key
            
            if should_stop:
                logger.info("Todos los reportes generados - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en el callback de mensaje: {e}")

    def start(self):
        """Inicia el ReportGenerator."""
        logger.info("Iniciando ReportGenerator...")
        
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            # Cerrar cualquier archivo CSV abierto
            for query_name in list(self.csv_files.keys()):
                self._close_csv_file(query_name)
            
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")



    def _write_to_csv(self, csv_data: str, query_name: str):
        """Escribe datos al archivo CSV correspondiente."""
        try:
            # Recopilar todas las transacciones para ordenar
            transaction_records = []

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
                            transaction_records.append({
                                "transaction_id": transaction_id,
                                "final_amount": final_amount
                            })
            else:
                # Es lista de diccionarios
                transaction_records = [
                    {"transaction_id": transaction["transaction_id"], "final_amount": transaction["final_amount"]}
                    for transaction in transactions
                ]

            # Agregar a la lista acumulativa para ordenar al final
            self.items_processed.extend(transaction_records)

        except Exception as e:
            logger.error(f"Error procesando transacciones: {e}")
            raise

    def _generate_final_report(self):
        """
        Genera el reporte final ordenado y lo envía al exchange.
        """
        try:
            if not self.items_processed:
                logger.warning("No hay datos procesados para el reporte.")
                return

            # Ordenar por transaction_id como hace pandas
            sorted_transactions = sorted(self.items_processed, key=lambda x: x["transaction_id"])
            
            # Solo enviar al exchange para que el gateway lo reciba
            # El cliente será quien guarde el reporte final
            self._send_report_to_exchange(sorted_transactions)
            
            logger.info(f"Reporte final generado con {len(sorted_transactions)} transacciones")

        except Exception as e:
            logger.error(f"Error generando reporte final: {e}")

    def _write_sorted_csv(self, sorted_transactions):
        """
        Escribe las transacciones ordenadas al archivo CSV.
        """
        try:
            # Inicializar CSV writer si no existe
            if not self.csv_writer:
                if not hasattr(self, 'csv_file') or not self.csv_file:
                    self._initialize_csv_file()
                headers = ["transaction_id", "final_amount"]
                self.csv_writer = csv.DictWriter(self.csv_file, fieldnames=headers)
                self.csv_writer.writeheader()

            # Escribir todas las transacciones ordenadas
            self.csv_writer.writerows(sorted_transactions)
            self.csv_file.flush()  # Asegurar que se escriba al disco
            
            logger.info(f"CSV ordenado escrito: {self.csv_filename}")

        except Exception as e:
            logger.error(f"Error escribiendo CSV ordenado: {e}")

    def _send_report_to_exchange(self, sorted_transactions):
        """
        Envía el reporte al exchange usando DTOs en batches.
        """
        try:
            # Crear middleware para el exchange
            reports_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host,
                queue_name="report_queue"  # Cola específica para reportes
            )
            
            # Enviar transacciones en batches usando DTO
            batch_size = 150  # Mismo tamaño que usamos en el cliente
            total_sent = 0
            
            while total_sent < len(sorted_transactions):
                # Crear batch
                end_idx = min(total_sent + batch_size, len(sorted_transactions))
                batch_transactions = sorted_transactions[total_sent:end_idx]
                
                # Convertir a CSV raw para enviar (solo transaction_id y final_amount)
                csv_lines = []
                for transaction in batch_transactions:
                    csv_lines.append(f"{transaction['transaction_id']},{transaction['final_amount']}")
                
                csv_batch = '\n'.join(csv_lines)
                
                # Crear DTO con CSV raw
                dto = TransactionBatchDTO(csv_batch, batch_type="RAW_CSV")
                
                # Enviar usando to_bytes_fast()
                reports_middleware.send(dto.to_bytes_fast())
                
                total_sent = end_idx
                logger.info(f"Batch enviado: {len(batch_transactions)} transacciones ({total_sent}/{len(sorted_transactions)})")
            
            # Enviar EOF para indicar fin del reporte
            eof_dto = TransactionBatchDTO("EOF:REPORT", batch_type="EOF")
            reports_middleware.send(eof_dto.to_bytes_fast())
            
            logger.info(f"Reporte {self.query_type} enviado en batches: {len(sorted_transactions)} registros totales")
            
            # Cerrar conexión
            reports_middleware.close()

        except Exception as e:
            logger.error(f"Error enviando reporte al exchange: {e}")

    def _close_csv_file(self):
        """
        Cierra el archivo CSV si está abierto.
        """
        try:
            if query_name in self.csv_files:
                self.csv_files[query_name].close()
                logger.info(f"Archivo CSV cerrado para {query_name}")
                del self.csv_files[query_name]
        except Exception as e:
            logger.error(f"Error cerrando el archivo CSV para {query_name}: {e}")


if __name__ == "__main__":
    report_generator = ReportGenerator()
    report_generator.start()