import logging
import os
import json
import time
import threading
import signal
import sys
from datetime import datetime
from collections import defaultdict
from rabbitmq.middleware import MessageMiddlewareQueue

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class ReportGenerator:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.input_queue = os.getenv('INPUT_QUEUE', 'final_data')
        self.report_every_n_items = 14000  # Reporte cada 14000 items
        self.session_timeout = 100  # 100 segundos sin actividad
        self.last_message_time = time.time()
        self.consuming = True
        self.timeout_thread = None
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.input_queue
        )
        
        # Estadísticas para el reporte
        self.stats = {
            'total_items': 0,
            'total_amount': 0.0,
            'items_by_hour': defaultdict(int),
            'items_by_year': defaultdict(int),
            'amount_by_year': defaultdict(float),
            'transactions_by_id': defaultdict(int),
            'items_processed': []
        }
        
        logger.info(f"ReportGenerator inicializado:")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Generará reporte cada {self.report_every_n_items} items")
        logger.info(f"  Timeout de sesión: {self.session_timeout} segundos")
        
        # Configurar signal handlers para shutdown limpio
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum, frame):
        """
        Maneja señales de terminación para shutdown limpio.
        """
        logger.info(f"Señal {signum} recibida, iniciando shutdown limpio...")
        self.consuming = False
        
        # Detener el middleware para salir del loop de consuming
        try:
            if self.input_middleware:
                self.input_middleware.stop_consuming()
        except Exception as e:
            logger.error(f"Error deteniendo consuming: {e}")

    def process_message(self, message: str):
        """
        Procesa un mensaje (item) y actualiza las estadísticas.
        Genera reporte automáticamente cada N items.
        """
        try:
            item = json.loads(message)
            
            # Ignorar señales especiales, solo procesar items reales
            if item.get('type') in ['FINISH', 'PARTIAL_FINISH']:
                logger.debug(f"Ignorando señal especial: {item.get('type')}")
                return
            
            # Actualizar estadísticas
            self.stats['total_items'] += 1
            self.stats['total_amount'] += float(item.get('subtotal', 0))
            
            # Extraer información temporal
            created_at = item.get('created_at', '')
            if created_at:
                try:
                    dt = datetime.strptime(created_at, "%Y-%m-%d %H:%M:%S")
                    hour_key = f"{dt.hour:02d}:00"
                    year_key = str(dt.year)
                    
                    self.stats['items_by_hour'][hour_key] += 1
                    self.stats['items_by_year'][year_key] += 1
                    self.stats['amount_by_year'][year_key] += float(item.get('subtotal', 0))
                except ValueError as e:
                    logger.warning(f"Error parsing date {created_at}: {e}")
            
            # Contar por transaction_id
            transaction_id = item.get('transaction_id', 'unknown')
            self.stats['transactions_by_id'][transaction_id] += 1
            
            # Guardar item para análisis detallado (limitar a 100 para no consumir mucha memoria)
            if len(self.stats['items_processed']) < 100:
                self.stats['items_processed'].append(item)
            
            logger.info(f"Item procesado: {transaction_id} - Item {item.get('item_id', 'N/A')} - ${item.get('subtotal', 0)}")
            logger.info(f"Total items procesados en esta sesión: {self.stats['total_items']}")
            
            # Generar reporte automáticamente cada N items
            if self.stats['total_items'] % self.report_every_n_items == 0:
                logger.info(f"Generando reporte automático después de {self.stats['total_items']} items...")
                self.generate_report()
                self._reset_stats()
            
        except json.JSONDecodeError as e:
            logger.error(f"Error decodificando JSON: {e}")
        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")

    def _reset_stats(self):
        """
        Reinicia las estadísticas para una nueva ejecución.
        """
        self.stats = {
            'total_items': 0,
            'total_amount': 0.0,
            'items_by_hour': defaultdict(int),
            'items_by_year': defaultdict(int),
            'amount_by_year': defaultdict(float),
            'transactions_by_id': defaultdict(int),
            'items_processed': []
        }
        logger.info("Estadísticas reiniciadas para nueva sesión")

    def generate_report(self):
        """
        Genera un reporte final con todas las estadísticas.
        """
        logger.info("=== GENERANDO REPORTE FINAL ===")
        
        # Reporte general
        logger.info(f"ESTADÍSTICAS GENERALES:")
        logger.info(f"   • Total de items procesados: {self.stats['total_items']}")
        logger.info(f"   • Monto total: ${self.stats['total_amount']:.2f}")
        logger.info(f"   • Transacciones únicas: {len(self.stats['transactions_by_id'])}")
        
        if self.stats['total_items'] > 0:
            avg_amount = self.stats['total_amount'] / self.stats['total_items']
            logger.info(f"   • Monto promedio por item: ${avg_amount:.2f}")
        
        # Reporte por año
        if self.stats['items_by_year']:
            logger.info(f"DISTRIBUCIÓN POR AÑO:")
            for year in sorted(self.stats['items_by_year'].keys()):
                count = self.stats['items_by_year'][year]
                amount = self.stats['amount_by_year'][year]
                logger.info(f"   • {year}: {count} items, ${amount:.2f}")
        
        # Reporte por hora (top 10)
        if self.stats['items_by_hour']:
            logger.info(f"TOP 10 HORAS MÁS ACTIVAS:")
            sorted_hours = sorted(self.stats['items_by_hour'].items(), 
                                key=lambda x: x[1], reverse=True)[:10]
            for hour, count in sorted_hours:
                logger.info(f"   • {hour}: {count} items")
        
        # Reporte de transacciones con más items
        if self.stats['transactions_by_id']:
            logger.info(f"TOP 5 TRANSACCIONES CON MÁS ITEMS:")
            sorted_transactions = sorted(self.stats['transactions_by_id'].items(), 
                                       key=lambda x: x[1], reverse=True)[:5]
            for tx_id, count in sorted_transactions:
                logger.info(f"   • {tx_id[:20]}...: {count} items")
        
        # Guardar reporte en archivo
        self.save_report_to_file()

    def save_report_to_file(self):
        """
        Guarda el reporte en un archivo JSON.
        """
        try:
            report_data = {
                'timestamp': datetime.now().isoformat(),
                'summary': {
                    'total_items': self.stats['total_items'],
                    'total_amount': self.stats['total_amount'],
                    'unique_transactions': len(self.stats['transactions_by_id']),
                    'average_amount_per_item': self.stats['total_amount'] / max(1, self.stats['total_items'])
                },
                'by_year': dict(self.stats['items_by_year']),
                'amount_by_year': dict(self.stats['amount_by_year']),
                'top_hours': dict(sorted(self.stats['items_by_hour'].items(), 
                                       key=lambda x: x[1], reverse=True)[:10]),
                'top_transactions': dict(sorted(self.stats['transactions_by_id'].items(), 
                                               key=lambda x: x[1], reverse=True)[:10])
            }
            
            # Crear directorio de reportes si no existe
            reports_dir = '/app/reports'
            os.makedirs(reports_dir, exist_ok=True)
            
            # Generar nombre de archivo con timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{reports_dir}/coffee_shop_report_{timestamp}.json"
            
            with open(filename, 'w') as f:
                json.dump(report_data, f, indent=2)
            
            logger.info(f"Reporte guardado en: {filename}")
            
        except Exception as e:
            logger.error(f"Error guardando reporte: {e}")

    def timeout_checker(self):
        """
        Hilo que verifica el timeout y genera reporte cuando no hay actividad.
        Se detiene limpiamente cuando self.consuming = False.
        """
        logger.info("Timeout checker iniciado")
        
        while self.consuming:
            # Sleep en chunks pequeños para ser responsivo al shutdown
            for _ in range(10):  # 10 x 0.1s = 1s total, pero verificando consuming cada 0.1s
                if not self.consuming:
                    break
                time.sleep(0.1)
            
            if not self.consuming:
                break
                
            current_time = time.time()
            
            time_since_last_message = current_time - self.last_message_time
            
            if time_since_last_message > self.session_timeout:
                if self.stats['total_items'] > 0:
                    logger.info(f"🚨 TIMEOUT ALCANZADO: {self.session_timeout}s sin mensajes, generando reporte...")
                    logger.info(f"Items acumulados: {self.stats['total_items']}")
                    self.generate_report()
                    self._reset_stats()
                    self.last_message_time = current_time  # Reset timeout
                else:
                    # Si no hay datos, solo logear cada 30 segundos para no saturar
                    if int(current_time) % 30 == 0:
                        logger.info(f"⏰ Esperando datos... ({int(time_since_last_message)}s sin actividad)")
            else:
                # Debug: mostrar tiempo restante cada 5 segundos
                if int(current_time) % 5 == 0 and self.stats['total_items'] > 0:
                    remaining = self.session_timeout - time_since_last_message
                    logger.debug(f"🔄 {remaining:.1f}s restantes para timeout, items: {self.stats['total_items']}")
        
        logger.info("Timeout checker terminado")

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje.
        """
        message = body.decode('utf-8')
        self.last_message_time = time.time()
        self.process_message(message)

    def start(self):
        """
        Inicia el report generator con timeout automático.
        """
        logger.info("Iniciando ReportGenerator con timeout automático")
        logger.info(f"Consumiendo de: {self.input_queue}")
        logger.info(f"Generará reporte cada {self.report_every_n_items} items o después de {self.session_timeout}s sin actividad")
        
        try:
            # Iniciar hilo de timeout checker
            self.consuming = True
            self.timeout_thread = threading.Thread(target=self.timeout_checker, daemon=True)
            self.timeout_thread.start()
            logger.info("Hilo de timeout iniciado")
            
            logger.info("Esperando mensajes...")
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("ReportGenerator detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            # Detener hilo de timeout de forma limpia
            logger.info("Deteniendo timeout thread...")
            self.consuming = False
            
            if self.timeout_thread and self.timeout_thread.is_alive():
                logger.info("Esperando que termine el timeout thread...")
                self.timeout_thread.join(timeout=5)
                
                if self.timeout_thread.is_alive():
                    logger.warning("Timeout thread no terminó en 5 segundos, pero continuando...")
                else:
                    logger.info("Timeout thread terminado correctamente")
            
            # Generar reporte final si hay datos pendientes
            if self.stats['total_items'] > 0:
                logger.info("Generando reporte final con datos pendientes...")
                self.generate_report()
            
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
