import logging
import os
from typing import Dict, List
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos import TransactionBatchDTO, StoreBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JoinNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        # Exchange que recibe tanto stores como TPV con diferentes routing keys
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'join.exchange')
        
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.input_exchange,
            route_keys=['stores.data', 'tpv.data', 'stores.eof', 'tpv.eof']
        )
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['q3.data', 'q3.eof']
        )
        
        self.stores_data: Dict[str, Dict] = {}  # store_id -> store_info
        self.tpv_data: List[Dict] = []  # Lista de resultados TPV
        self.joined_data: List[Dict] = []  # Datos después del JOIN
        
        self.groupby_eof_count = 0
        self.stores_loaded = False
        self.expected_groupby_nodes = 2  # Semestre 1 y 2

        self.store_dto_helper = StoreBatchDTO("", BatchType.RAW_CSV)

        logger.info(f"JoinNode inicializado:")
        logger.info(f"  Exchange: {self.input_exchange}")
        logger.info(f"  Routing keys: stores.data, tpv.data, stores.eof, tpv.eof")
    
    def _process_store_batch(self, csv_data: str):
        """
        Procesa un batch de datos de stores y los guarda en memoria.
        Estructura esperada: store_id,store_name,street,postal_code,city,state,latitude,longitude
        """
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line:
                continue
            
            try:
                store_id = self.store_dto_helper.get_column_value(line, 'store_id')
                store_name = self.store_dto_helper.get_column_value(line, 'store_name')
                
                if store_id and store_name:
                    self.stores_data[store_id] = {
                        'store_id': store_id,
                        'store_name': store_name,
                        'raw_line': line
                    }
                    processed_count += 1
                    
            except Exception as e:
                logger.warning(f"Error procesando línea de store: {line}, error: {e}")
                continue
        
        logger.info(f"Stores procesados: {processed_count}. Total en memoria: {len(self.stores_data)}")
    
    def _process_tpv_batch(self, csv_data: str):
        """
        Procesa un batch de resultados TPV de los nodos GroupBy.
        Formato esperado: year_half_created_at,store_id,total_payment_value,transaction_count
        """
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('year_half_created_at'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) >= 4:
                    tpv_record = {
                        'year_half_created_at': parts[0],
                        'store_id': parts[1],
                        'total_payment_value': float(parts[2]),
                        'transaction_count': int(parts[3])
                    }
                    self.tpv_data.append(tpv_record)
                    processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de TPV: {line}, error: {e}")
                continue
        
        logger.info(f"TPV procesados: {processed_count}. Total en memoria: {len(self.tpv_data)}")
        
        if self.stores_loaded:
            self._perform_join()
    
    def _perform_join(self):
        """
        Hace JOIN entre stores y TPV data en memoria.
        Resultado: year_half_created_at, store_name, tpv
        """
        if not self.stores_data:
            logger.warning("No hay datos de stores para hacer JOIN")
            return
        
        if not self.tpv_data:
            logger.warning("No hay datos de TPV para hacer JOIN")
            return
        
        self.joined_data.clear() 
        joined_count = 0
        missing_stores = set()
        
        for tpv_record in self.tpv_data:
            store_id = tpv_record['store_id']
            
            if store_id in self.stores_data:
                store_name = self.stores_data[store_id]['store_name']
            else:
                missing_stores.add(store_id)
                store_name = f"Store_{store_id}"
            
            joined_record = {
                'year_half_created_at': tpv_record['year_half_created_at'],
                'store_name': store_name,
                'tpv': tpv_record['total_payment_value']
            }
            
            self.joined_data.append(joined_record)
            joined_count += 1
        
        if missing_stores:
            logger.warning(f"Stores no encontrados: {missing_stores}")
        
        logger.info(f"JOIN completado: {joined_count} registros en memoria")

    
    def _handle_store_eof(self):
        """Maneja EOF de datos de stores."""
        logger.info("EOF recibido de datos de stores")
        self.stores_loaded = True
        
        if self.tpv_data:
            self._perform_join()
        
        if self.groupby_eof_count >= self.expected_groupby_nodes:
            logger.info("Stores y todos los GroupBy completados - enviando resultados")
            self._send_results_to_exchange()
            return True  
        
        return False
    
    def _handle_tpv_eof(self):
        """Maneja EOF de los nodos GroupBy."""
        self.groupby_eof_count += 1
        
        if self.groupby_eof_count >= self.expected_groupby_nodes:
            
            if self.stores_loaded and not self.joined_data:
                self._perform_join()
            
            if self.stores_loaded:
                logger.info("Stores y GroupBy completados - enviando resultados")
                self._send_results_to_exchange()
                return True  
            else:
                logger.info("Esperando EOF de stores...")
        
        return False
    
    def _send_results_to_exchange(self):
        """Envía los resultados del JOIN al exchange de reportes."""
        try:
            if not self.joined_data:
                logger.warning("No hay datos joinados para enviar")
                return
            
            csv_lines = ["year_half_created_at,store_name,tpv"]
            for record in self.joined_data:
                csv_lines.append(f"{record['year_half_created_at']},{record['store_name']},{record['tpv']:.2f}")
            
            results_csv = '\n'.join(csv_lines)
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q3.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q3.eof')
            
            logger.info(f"Resultados Q3 enviados: {len(self.joined_data)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados: {e}")
    
    def process_message(self, message: bytes, routing_key: str) -> bool:
        """
        Procesa mensajes según el routing key.
        
        Args:
            message: Mensaje binario
            routing_key: Key que identifica el tipo de dato
        """
        try:
            if routing_key in ['stores.data', 'stores.eof']:
                dto = StoreBatchDTO.from_bytes_fast(message)
                
                if dto.batch_type == BatchType.EOF:
                     return self._handle_store_eof()
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_store_batch(dto.data)
                    
            elif routing_key in ['tpv.data', 'tpv.eof']:
                dto = TransactionBatchDTO.from_bytes_fast(message)
                
                if dto.batch_type == BatchType.EOF:
                    return self._handle_tpv_eof()
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_tpv_batch(dto.data)
            
            return False
            
        except Exception as e:
            logger.error(f"Error procesando mensaje con routing key {routing_key}: {e}")
            return False
    
    def on_message_callback(self, ch, method, properties, body):
        """Callback para RabbitMQ que incluye routing key."""
        try:
            routing_key = method.routing_key
            should_stop = self.process_message(body, routing_key)
            if should_stop:
                logger.info("JOIN completado - deteniendo consuming")
                ch.stop_consuming()
        except Exception as e:
            logger.error(f"Error en callback: {e}")
    
    def start(self):
        """Inicia el nodo JOIN."""
        try:
            logger.info("Iniciando JoinNode...")
            self.input_middleware.start_consuming(self.on_message_callback)
            
        except KeyboardInterrupt:
            logger.info("JoinNode detenido manualmente")
        except Exception as e:
            logger.error(f"Error durante el consumo: {e}")
            raise
        finally:
            self._cleanup()
    
    def _cleanup(self):
        """Limpia recursos al finalizar."""
        try:
            if self.input_middleware:
                self.input_middleware.close()
                logger.info("Conexión cerrada")
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
    
    def get_joined_data(self) -> List[Dict]:
        """Retorna los datos después del JOIN para uso externo."""
        return self.joined_data.copy()


if __name__ == "__main__":
    node = JoinNode()
    node.start()
