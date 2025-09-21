from abc import ABC, abstractmethod
import pika
import logging

logger = logging.getLogger(__name__)

class MessageMiddlewareMessageError(Exception):
    pass

class MessageMiddlewareDisconnectedError(Exception):
    pass

class MessageMiddlewareCloseError(Exception):
    pass

class MessageMiddlewareDeleteError(Exception):
    pass

class MessageMiddleware(ABC):

	#Comienza a escuchar a la cola/exchange e invoca a on_message_callback tras
	#cada mensaje de datos o de control.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def start_consuming(self, on_message_callback):
		pass
	
	#Si se estaba consumiendo desde la cola/exchange, se detiene la escucha. Si
	#no se estaba consumiendo de la cola/exchange, no tiene efecto, ni levanta
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	@abstractmethod
	def stop_consuming(self):
		pass
	
	#Envía un mensaje a la cola o al tópico con el que se inicializó el exchange.
	#Si se pierde la conexión con el middleware eleva MessageMiddlewareDisconnectedError.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareMessageError.
	@abstractmethod
	def send(self, message):
		pass

	#Se desconecta de la cola o exchange al que estaba conectado.
	#Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareCloseError.
	@abstractmethod
	def close(self):
		pass

	# Se fuerza la eliminación remota de la cola o exchange.
	# Si ocurre un error interno que no puede resolverse eleva MessageMiddlewareDeleteError.
	@abstractmethod
	def delete(self):
		pass


class MessageMiddlewareExchange(MessageMiddleware):
	def __init__(self, host, exchange_name, route_keys):
		pass

class MessageMiddlewareQueue(MessageMiddleware):
    def __init__(self, host: str, queue_name: str):
        self.host = host
        self.queue_name = queue_name
        self.connection = None
        self.channel = None

        self._connect()

    def _connect(self):
        try:
            self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.host))
            self.channel = self.connection.channel()
            self.channel.queue_declare(queue=self.queue_name, durable=True)  # Cola persistente
            logger.info(f"Conectado a RabbitMQ. Cola declarada: {self.queue_name}")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Error conectando a RabbitMQ: {e}")
            raise MessageMiddlewareDisconnectedError("No se pudo conectar a RabbitMQ")

    def send(self, message: str):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            self.channel.basic_publish(
                exchange='',  
                routing_key=self.queue_name,
                body=message,
                properties=pika.BasicProperties(
                    delivery_mode=2  # Mensajes persistentes
                )
            )
            logger.info(f"Mensaje publicado en la cola {self.queue_name}: {message}")
        except Exception as e:
            logger.error(f"Error enviando mensaje: {e}")
            raise MessageMiddlewareMessageError(f"Error enviando mensaje: {e}")

    def start_consuming(self, on_message_callback):
        try:
            if not self.channel:
                raise MessageMiddlewareDisconnectedError("No hay conexión activa con RabbitMQ")
            
            logger.info(f"Esperando mensajes en la cola {self.queue_name}...")
            self.channel.basic_consume(
                queue=self.queue_name,
                on_message_callback=on_message_callback,
                auto_ack=True
            )
            self.channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            logger.error(f"Conexión con RabbitMQ perdida: {e}")
            raise MessageMiddlewareDisconnectedError("Conexión con RabbitMQ perdida")
        except Exception as e:
            logger.error(f"Error durante el consumo de mensajes: {e}")
            raise MessageMiddlewareMessageError(f"Error durante el consumo de mensajes: {e}")

    def stop_consuming(self):
        try:
            if self.channel:
                self.channel.stop_consuming()
                logger.info("Consumo de mensajes detenido")
        except Exception as e:
            logger.error(f"Error deteniendo el consumo: {e}")
            raise MessageMiddlewareMessageError(f"Error deteniendo el consumo: {e}")

    def delete(self):
        try:
            if self.channel:
                self.channel.queue_delete(queue=self.queue_name)
                logger.info(f"Cola {self.queue_name} eliminada")
        except Exception as e:
            logger.error(f"Error eliminando la cola: {e}")
            raise MessageMiddlewareDeleteError(f"Error eliminando la cola: {e}")

    def close(self):
        try:
            if self.connection:
                self.connection.close()
                logger.info("Conexión con RabbitMQ cerrada")
        except Exception as e:
            logger.error(f"Error cerrando conexión: {e}")
            raise MessageMiddlewareCloseError(f"Error cerrando conexión: {e}")
