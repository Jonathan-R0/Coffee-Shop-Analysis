import logging
import os
from typing import Dict, List
from rabbitmq.middleware import MessageMiddlewareExchange
from dtos.dto import TransactionBatchDTO, StoreBatchDTO, UserBatchDTO, BatchType

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class JoinNode:
    def __init__(self):
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost')
        
        # Exchange que recibe tanto stores como TPV con diferentes routing keys
        self.input_exchange = os.getenv('INPUT_EXCHANGE', 'join.exchange')
        self.output_exchange = os.getenv('OUTPUT_EXCHANGE', 'report.exchange')
        
        self.input_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.input_exchange,
            route_keys=['stores.data', 'tpv.data', 'stores.eof', 'tpv.eof', 
                       'top_customers.data', 'top_customers.eof',
                       'users.data', 'users.eof']
        )
        self.output_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_exchange,
            route_keys=['q3.data', 'q3.eof', 'q4.data', 'q4.eof']
        )
        
        self.stores_data: Dict[str, Dict] = {}  # store_id -> store_info
        self.users_data: Dict[str, Dict] = {}  # user_id -> user_info  
        self.tpv_data: List[Dict] = []  # Lista de resultados TPV
        self.top_customers_data: List[Dict] = []  # Lista de top customers por store
        self.joined_data: List[Dict] = []  # Datos después del JOIN
        
        self.groupby_eof_count = 0
        self.stores_loaded = False
        self.users_loaded = False
        self.top_customers_loaded = False
        self.expected_groupby_nodes = 2  # Semestre 1 y 2
        self.top_customers_sent = False  # Para evitar múltiples envíos de Q4
        self.q3_results_sent = False  # ← AGREGAR ESTA LÍNEA

        self.store_dto_helper = StoreBatchDTO("", BatchType.RAW_CSV)
        self.user_dto_helper = UserBatchDTO("", BatchType.RAW_CSV)

        logger.info(f"JoinNode inicializado:")
        logger.info(f"  Exchange: {self.input_exchange}")
        logger.info(f"  Routing keys: stores.data, users.data, tpv.data, top_customers.data, stores.eof, users.eof, tpv.eof, top_customers.eof")
    
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
    
    def _process_user_batch(self, csv_data: str):
        """
        Procesa un batch de datos de users y los guarda en memoria.
        Estructura real: user_id	gender	birthdate	registered_at (separado por TABS)
        """
        processed_count = 0
        
        logger.info(f"Procesando batch de users, longitud: {len(csv_data)}")
        lines = csv_data.split('\n')
        logger.info(f"Total líneas en batch: {len(lines)}")
        
        for i, line in enumerate(lines):
            line = line.strip()
            if not line:
                continue
            
            if i < 3:  # Debug: mostrar primeras 3 líneas
                logger.info(f"Línea {i}: '{line}'")
            
            try:
                user_id = self.user_dto_helper.get_column_value(line, 'user_id')
                birthdate = self.user_dto_helper.get_column_value(line, 'birthdate')  # Usar 'birthdate' no 'birth_date'
                gender = self.user_dto_helper.get_column_value(line, 'gender')
                registered_at = self.user_dto_helper.get_column_value(line, 'registered_at')
                
                if i < 3:  # Debug: mostrar valores extraídos
                    logger.info(f"Extraído - user_id: '{user_id}', birthdate: '{birthdate}', gender: '{gender}'")
                
                # Verificar que user_id y birthdate no estén vacíos y no sea header
                if user_id and birthdate and user_id != 'user_id':
                    self.users_data[user_id] = {
                        'user_id': user_id,
                        'gender': gender,
                        'birth_date': birthdate,  # Mantener como birth_date para consistencia en el resto del código
                        'registered_at': registered_at,
                        'raw_line': line
                    }
                    processed_count += 1
                    
            except Exception as e:
                if i < 5:  # Solo mostrar errores de las primeras líneas
                    logger.warning(f"Error procesando línea de user {i}: {line}, error: {e}")
                continue
        
        logger.info(f"Users procesados: {processed_count}. Total en memoria: {len(self.users_data)}")
    
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
    
    def _process_top_customers_batch(self, csv_data: str):
        """
        Procesa un batch de resultados de top customers.
        Formato esperado: store_id,user_id,purchases_qty
        """
        processed_count = 0
        
        for line in csv_data.split('\n'):
            line = line.strip()
            if not line or line.startswith('store_id,user_id,purchases_qty'):
                continue
            
            try:
                parts = line.split(',')
                if len(parts) >= 3:
                    # Limpiar user_id: convertir de "12345.0" a "12345"
                    raw_user_id = parts[1]
                    if '.' in raw_user_id and raw_user_id.endswith('.0'):
                        user_id = raw_user_id[:-2]  # Remover ".0"
                    else:
                        user_id = raw_user_id
                    
                    top_customer_record = {
                        'store_id': parts[0],
                        'user_id': user_id,
                        'purchases_qty': int(parts[2])
                    }
                    self.top_customers_data.append(top_customer_record)
                    processed_count += 1
                    
            except (ValueError, IndexError) as e:
                logger.warning(f"Error procesando línea de top customers: {line}, error: {e}")
                continue
        
        logger.info(f"Top customers procesados: {processed_count}. Total en memoria: {len(self.top_customers_data)}")
        
        if self.stores_loaded and not self.top_customers_sent:
            self._perform_top_customers_join()
            self.top_customers_sent = True
            logger.info("Finalizando procesamiento por EOF de top customers")
            

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
                'store_id': store_id,  # Agregar store_id para el sorting
                'store_name': store_name,
                'tpv': tpv_record['total_payment_value']
            }
            
            self.joined_data.append(joined_record)
            joined_count += 1
        
        if missing_stores:
            logger.warning(f"Stores no encontrados: {missing_stores}")
        
        logger.info(f"JOIN completado: {joined_count} registros en memoria")

    def _perform_top_customers_join(self):
        """
        Hace JOIN entre top customers y users data para generar tabla final.
        Q4 NO necesita stores - puede usar store_id directamente.
        """
        if not self.top_customers_data:
            logger.warning("No hay datos de top customers para hacer JOIN")
            return
        
        if not self.users_data:
            logger.warning("No hay datos de users para hacer JOIN de top customers")
            return
        
        joined_top_customers = []
        joined_count = 0
        missing_users = set()
        found_users = 0
        
        for customer_record in self.top_customers_data:
            store_id = customer_record['store_id']
            user_id = customer_record['user_id']
            
            # Para Q4, usar store_id como store_name si no hay stores cargados
            if self.stores_loaded and store_id in self.stores_data:
                store_name = self.stores_data[store_id]['store_name']
            else:
                store_name = f"Store_{store_id}"  # Usar store_id como fallback
            
            # Buscar user birthday
            if user_id in self.users_data:
                birthdate = self.users_data[user_id]['birth_date']
                found_users += 1
            else:
                missing_users.add(user_id)
                birthdate = 'UNKNOWN'
            
            joined_record = {
                'store_id': store_id,
                'store_name': store_name,
                'birthdate': birthdate
            }
            
            joined_top_customers.append(joined_record)
            joined_count += 1
        
        logger.info(f"Users encontrados: {found_users}/{len(self.top_customers_data)} ({found_users/len(self.top_customers_data)*100:.1f}%)")
        
        if missing_users:
            logger.warning(f"Users no encontrados para top customers (primeros 10): {list(missing_users)[:10]}")
        
        logger.info(f"Top customers JOIN completado: {joined_count} registros (store_name, birthdate)")
        
        # Enviar resultados de Q4 (top customers)
        self._send_top_customers_results(joined_top_customers)

    
    def _handle_store_eof(self):
        """Maneja EOF de datos de stores."""
        logger.info("EOF recibido de datos de stores")
        self.stores_loaded = True
        
        # Query 3: Si hay datos TPV y todos los GroupBy han terminado, enviar Q3
        if self.tpv_data and self.groupby_eof_count >= self.expected_groupby_nodes and not self.q3_results_sent:
            self._perform_join()
            logger.info("Stores cargados y GroupBy completados - enviando resultados Q3")
            self._send_results_to_exchange()
            self.q3_results_sent = True  # ← AGREGAR ESTA LÍNEA
        
        # Query 4: Si hay datos de top customers y users están cargados, enviar Q4
        if self.top_customers_data and self.users_loaded and not self.top_customers_sent:
            logger.info("Stores y users cargados - enviando resultados Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True

        logger.info("Finalizando procesamiento por EOF de stores")
        return False  # No cerrar el nodo, puede haber más datos llegando

    def _handle_user_eof(self):
        """Maneja EOF de datos de users."""
        logger.info("EOF recibido de datos de users")
        self.users_loaded = True
        
        # Query 4: Solo necesita users y top_customers, NO stores
        if self.top_customers_loaded and not self.top_customers_sent:
            logger.info("Users y top customers completados - enviando resultados Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True
        else:
            missing = []
            if not self.top_customers_loaded:
                missing.append("top_customers")
            logger.info(f"Users completado, esperando: {', '.join(missing)} para enviar Q4")
        
        return False  # No cerrar el nodo  
      
    def _handle_tpv_eof(self):
        """Maneja EOF de los nodos GroupBy."""
        self.groupby_eof_count += 1
        logger.info(f"EOF TPV recibido: {self.groupby_eof_count}/{self.expected_groupby_nodes}")
        
        # Query 3: Solo enviar cuando todos los GroupBy hayan terminado Y stores estén cargados
        if self.groupby_eof_count >= self.expected_groupby_nodes and not self.q3_results_sent:
            if self.stores_loaded:
                if not self.joined_data:  # Solo hacer JOIN si no se ha hecho ya
                    self._perform_join()
                logger.info("Todos los GroupBy completados y stores cargados - enviando resultados Q3")
                self._send_results_to_exchange()
                self.q3_results_sent = True  # ← AGREGAR ESTA LÍNEA
            else:
                logger.info("Todos los GroupBy completados, esperando EOF de stores para enviar Q3...")
        
        return False  # No cerrar el nodo

    def _handle_top_customers_eof(self):
        """Maneja EOF de top customers."""
        logger.info("EOF recibido de top customers")
        self.top_customers_loaded = True
        
        # Query 4: Solo necesita users y top_customers, NO stores
        if self.users_loaded and not self.top_customers_sent:
            logger.info("Top customers y users completados - enviando resultados Q4")
            self._perform_top_customers_join()
            self.top_customers_sent = True
        else:
            missing = []
            if not self.users_loaded:
                missing.append("users")
            logger.info(f"Top customers completado, esperando EOF de: {', '.join(missing)} para enviar Q4")
        
        return False  # No cerrar el nodo
    
    def _send_top_customers_results(self, joined_top_customers):
        """Envía los resultados de top customers al exchange de reportes con formato store_name,birthdate."""
        try:
            if not joined_top_customers:
                logger.warning("No hay datos de top customers joinados para enviar")
                return
            
            # Ordenar por store_id para mantener el orden esperado por pandas
            sorted_top_customers = sorted(joined_top_customers, key=lambda x: int(x['store_id']))
            
            # Formato final: store_name,birthdate
            csv_lines = ["store_name,birthdate"]
            for record in sorted_top_customers:
                csv_lines.append(f"{record['store_name']},{record['birthdate']}")
            
            results_csv = '\n'.join(csv_lines)
            
            logger.info(f"Resultados Q4 generados:")
            logger.info(f"  Total registros: {len(joined_top_customers)}")
            logger.info(f"  Formato: store_name,birthdate")
            logger.info(f"  Primeras líneas:")
            for i, line in enumerate(csv_lines[:5]):  # Mostrar primeras 5 líneas
                logger.info(f"    {line}")
            
            result_dto = TransactionBatchDTO(results_csv, BatchType.RAW_CSV)
            self.output_middleware.send(result_dto.to_bytes_fast(), routing_key='q4.data')
            
            eof_dto = TransactionBatchDTO("EOF:1", BatchType.EOF)
            self.output_middleware.send(eof_dto.to_bytes_fast(), routing_key='q4.eof')
            
            logger.info(f"Resultados Q4 (top customers con birthdate) enviados: {len(joined_top_customers)} registros")
            
        except Exception as e:
            logger.error(f"Error enviando resultados de top customers: {e}")
    
    def _send_results_to_exchange(self):
        """Envía los resultados del JOIN al exchange de reportes."""
        try:
            if not self.joined_data:
                logger.warning("No hay datos joinados para enviar")
                return
            
            sorted_joined_data = sorted(self.joined_data, key=lambda x: (x['year_half_created_at'], int(x['store_id'])))
            
            csv_lines = ["year_half_created_at,store_name,tpv"]
            for record in sorted_joined_data:
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
                    
            elif routing_key in ['users.data', 'users.eof']:
                dto = UserBatchDTO.from_bytes_fast(message)
                
                if dto.batch_type == BatchType.EOF:
                     return self._handle_user_eof()
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_user_batch(dto.data)
                    
            elif routing_key in ['tpv.data', 'tpv.eof']:
                dto = TransactionBatchDTO.from_bytes_fast(message)
                
                if dto.batch_type == BatchType.EOF:
                    return self._handle_tpv_eof()
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_tpv_batch(dto.data)
            
            elif routing_key in ['top_customers.data', 'top_customers.eof']:
                dto = TransactionBatchDTO.from_bytes_fast(message)
                
                if dto.batch_type == BatchType.EOF:
                    return self._handle_top_customers_eof()
                
                if dto.batch_type == BatchType.RAW_CSV:
                    self._process_top_customers_batch(dto.data)
            
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
