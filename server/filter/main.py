import logging  
import os
import json
from datetime import datetime
from rabbitmq.middleware import MessageMiddlewareQueue,MessageMiddlewareExchange
from filter_factory import FilterStrategyFactory
from dtos.dto import TransactionBatchDTO

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FilterNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        self.input_queue = os.getenv('INPUT_QUEUE', 'raw_data')
        self.output_q1 = os.getenv('OUTPUT_Q1', None)
        self.output_q3 = os.getenv('OUTPUT_Q3', None)
        
        self.filter_mode = os.getenv('FILTER_MODE', 'year')
        
        # Variable para el total de filtros de este tipo
        total_env_var = f'TOTAL_{self.filter_mode.upper()}_FILTERS'
        self.total_filters = int(os.getenv(total_env_var, '1'))
        
        logger.info(f"Total filtros {self.filter_mode}: {self.total_filters}")
        
        self.filter_strategy = self._create_filter_strategy()
        
        self.input_middleware = MessageMiddlewareQueue(
            host=self.rabbitmq_host, 
            queue_name=self.input_queue
        )
        
        self.output_middleware = None
        if self.output_q1:
            self.output_middleware = MessageMiddlewareQueue(
                host=self.rabbitmq_host, 
                queue_name=self.output_q1
            )
            
        self.output_middleware_exchange = None
        if self.output_q3:
            self.output_middleware_exchange = MessageMiddlewareExchange(
                host=self.rabbitmq_host,
                exchange_name=self.output_q3,
                route_keys=['semester.1', 'semester.2']
            )
            logger.info(f"  Output Exchange: {self.output_q3}")
        
        logger.info(f"FilterNode inicializado:")
        logger.info(f"  Modo: {self.filter_mode}")
        logger.info(f"  Queue entrada: {self.input_queue}")
        logger.info(f"  Queue salida: {self.output_q1}")

    def _create_filter_strategy(self):
        """
        Crea la estrategia de filtro apropiada basándose en la configuración.
        
        Returns:
            FilterStrategy: Instancia de la estrategia correspondiente
        """
        try:
            config = {}
            
            if self.filter_mode == 'year':
                config['filter_years'] = os.getenv('FILTER_YEARS', '2024,2025')
            elif self.filter_mode == 'hour':
                config['filter_hours'] = os.getenv('FILTER_HOURS', '06:00-23:00')
            elif self.filter_mode == 'amount':
                config['min_amount'] = float(os.getenv('MIN_AMOUNT', '75'))
            
            return FilterStrategyFactory.create_strategy(self.filter_mode, **config)
            
        except Exception as e:
            logger.error(f"Error creando estrategia de filtro: {e}")
            return None

    def process_message(self, message: bytes):
        """
        OPTIMIZADO: Procesa CSV directo sin deserialización completa.
        Maneja EOF con contador para sincronización entre nodos.
        """
        try:
            dto = TransactionBatchDTO.from_bytes_fast(message)

            # Manejo de EOF con contador
            if dto.batch_type == "EOF":
                return self._handle_eof_message(dto)
            
            if dto.batch_type == "CONTROL":
                logger.info("Señal CONTROL recibida - propagando al siguiente nodo")
                if self.output_middleware:
                    self.output_middleware.send(message)
                return False  # No detener consuming
            
            filtered_csv = self.filter_strategy.filter_csv_batch(dto.transactions)
            
            if not filtered_csv.strip():
                return

            filtered_dto = TransactionBatchDTO(filtered_csv, batch_type="RAW_CSV")
            serialized_data = filtered_dto.to_bytes_fast()

            if self.output_middleware:
                self.output_middleware.send(serialized_data)
                
            if self.output_middleware_exchange:
                self._send_to_exchange_by_semester(filtered_csv)
                
            return False  # No detener consuming para mensajes normales

        except Exception as e:
            logger.error(f"Error procesando mensaje: {e}")
            return False  # No detener consuming en caso de error

    def _handle_eof_message(self, dto: TransactionBatchDTO):
        """
        Maneja mensajes EOF con contador para sincronización.
        
        Estrategia:
        - Si counter < total_filters: reenvía EOF:(counter+1) a INPUT queue y termina
        - Si counter == total_filters: reenvía EOF:1 a OUTPUT queue y termina
        """
        try:
            # Extraer el contador del mensaje
            eof_data = dto.transactions.strip()
            if ':' in eof_data:
                counter = int(eof_data.split(':')[1])
            else:
                counter = 1  # Fallback si no tiene formato
            
            logger.info(f"EOF recibido con counter={counter}, total_filters={self.total_filters}")
            
            if counter < self.total_filters:
                # Reenviar a INPUT queue con counter+1
                new_counter = counter + 1
                eof_dto = TransactionBatchDTO(f"EOF:{new_counter}", batch_type="EOF")
                
                # Enviar de vuelta a la input queue
                input_middleware_sender = MessageMiddlewareQueue(
                    host=self.rabbitmq_host,
                    queue_name=self.input_queue
                )
                input_middleware_sender.send(eof_dto.to_bytes_fast())
                input_middleware_sender.close()
                
                logger.info(f"EOF:{new_counter} reenviado a INPUT queue {self.input_queue}")
                
            elif counter == self.total_filters:
                # Todos los nodos de este tipo ya procesaron - enviar EOF:1 a OUTPUT
                if self.output_middleware:
                    eof_dto = TransactionBatchDTO("EOF:1", batch_type="EOF")
                    self.output_middleware.send(eof_dto.to_bytes_fast())
                    logger.info(f"EOF:1 enviado a OUTPUT queue {self.output_q1}")
                
            # En ambos casos, dejar de leer de la cola
            logger.info("Finalizando procesamiento por EOF")
            return True  # Señal para terminar el loop de consuming
            
        except Exception as e:
            logger.error(f"Error manejando EOF: {e}")
            return False
            
    @staticmethod
    def get_month_from_csv_line(line):
        """Extract month from CSV line"""
        fields = line.split(',')
        if len(fields) >= 9:
            date_str = fields[8]  
            return int(date_str[5:7])
        return None
            
    def _send_to_exchange_by_semester(self, csv_data: str):
        """
        Separa datos por semestre y envía con routing key apropiada.
        """
        semester_1_lines = []
        semester_2_lines = []
        
        for line in csv_data.split('\n'):
            if not line.strip():
                continue
            
            month = self.get_month_from_csv_line(line)
            if month:
                if month <= 6:
                    semester_1_lines.append(line)
                else:
                    semester_2_lines.append(line)
        
        # Enviar semestre 1
        if semester_1_lines:
            csv_s1 = '\n'.join(semester_1_lines)
            dto_s1 = TransactionBatchDTO(csv_s1, batch_type="RAW_CSV")
            self.output_middleware_exchange.send(
                dto_s1.to_bytes_fast(), 
                routing_key='semester.1'
            )
        
        # Enviar semestre 2
        if semester_2_lines:
            csv_s2 = '\n'.join(semester_2_lines)
            dto_s2 = TransactionBatchDTO(csv_s2, batch_type="RAW_CSV")
            self.output_middleware_exchange.send(
                dto_s2.to_bytes_fast(), 
                routing_key='semester.2'
            )

    def on_message_callback(self, ch, method, properties, body):
        """
        Callback para RabbitMQ cuando llega un mensaje.
        """
        try:
            should_stop = self.process_message(body)
            if should_stop:
                logger.info("EOF procesado - deteniendo consuming")
                ch.stop_consuming()
                return
                
        except Exception as e:
            logger.error(f"Error en callback: {e}")

    def start(self):
        try:
            self.input_middleware.start_consuming(self.on_message_callback)
        except KeyboardInterrupt:
            logger.info("Filtro detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión de entrada cerrada")
                
            if self.output_middleware:
                self.output_middleware.close()
                logger.info("Conexión de salida cerrada")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")

if __name__ == "__main__":
    filter_node = FilterNode()
    filter_node.start()