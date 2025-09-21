import logging
from rabbitmq.middleware import MessageMiddlewareQueue
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterNode:
    def __init__(self, host: str, queue_name: str):
        rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        self.middleware = MessageMiddlewareQueue(host=rabbitmq_host, queue_name=queue_name)

    def process_message(self, message: str):
        logger.info(f"Procesando mensaje: {message}")
        
    def on_message_callback(self,ch, method, properties, body):
        message = body.decode('utf-8')
        self.process_message(message)

    def start(self):
        try:
            logger.info("Iniciando consumo de mensajes...")
            self.middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Consumo detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
        finally:
            self.middleware.close()
            logger.info("Conexión con RabbitMQ cerrada")

if __name__ == "__main__":
    consumer = FilterNode(host="localhost", queue_name="data_queue")
    consumer.start()