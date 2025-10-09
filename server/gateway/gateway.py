from asyncio import Lock
import time
import socket
import os
import json
import signal
import sys
import threading
from common.protocol import Protocol, ProtocolMessage 
from common.new_protocolo import ProtocolNew
from rabbitmq.middleware import MessageMiddlewareQueue, MessageMiddlewareExchange
from dataclasses import asdict
from logger import get_logger
from dtos.dto import TransactionBatchDTO, BatchType, StoreBatchDTO,UserBatchDTO, ReportBatchDTO, TransactionItemBatchDTO, MenuItemBatchDTO
from gateway_acceptor import GatewayAcceptor
from report_dispatcher import ReportDispatcher

logger = get_logger(__name__)

class Gateway:

    def __init__(self, port, listener_backlog, rabbitmq_host, output_year_node_exchange, output_join_node, input_reports=None, shutdown_handler=None):
        self.shutdown = shutdown_handler
        
        #self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #self._server_socket.bind(('', port))
        #self._server_socket.listen(listener_backlog)
        self._acceptor = GatewayAcceptor(port, listener_backlog, shutdown_handler, rabbitmq_host,input_reports,self)

        self._is_running = False
        self.rabbitmq_host = rabbitmq_host
        self.output_year_node_exchange = output_year_node_exchange
        self.output_join_node = output_join_node
        self.output_filter_year_nodes_middleware = None
        self._join_middleware = None
        self._reports_exchange = input_reports
        self._report_dispatcher = None
        self._clients_lock = threading.Lock()
        self._clients_by_id = {}
        self.setup_common_middleware()

    def setup_common_middleware(self):
        self.output_filter_year_nodes_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_year_node_exchange,
            route_keys=['transactions', 'transaction_items']
        )
        
        self._join_middleware = MessageMiddlewareExchange(
            host=self.rabbitmq_host,
            exchange_name=self.output_join_node,
            route_keys=['stores.data', 'users.data', 'users.eof', 'menu_items.data']
        )
        
        if self.shutdown:
            if hasattr(self.output_filter_year_nodes_middleware, 'shutdown'):
                self.output_filter_year_nodes_middleware.shutdown = self.shutdown
            if hasattr(self._join_middleware, 'shutdown'):
                self._join_middleware.shutdown = self.shutdown

        if self._reports_exchange:
            self._start_report_dispatcher()

    def _start_report_dispatcher(self):
        if self._report_dispatcher:
            return
        self._report_dispatcher = ReportDispatcher(
            rabbitmq_host=self.rabbitmq_host,
            reports_exchange=self._reports_exchange,
            gateway=self,
            shutdown_handler=self.shutdown,
        )
        self._report_dispatcher.start()

    def get_output_middleware(self):
        return self.output_filter_year_nodes_middleware

    def get_join_middleware(self):
        return self._join_middleware

    def register_client(self, client_handler):
        with self._clients_lock:
            self._clients_by_id[client_handler.client_id] = client_handler
            logger.info(f"Cliente {client_handler.client_id} registrado en Gateway")

    def unregister_client(self, client_id):
        with self._clients_lock:
            if client_id in self._clients_by_id:
                del self._clients_by_id[client_id]
                logger.info(f"Cliente {client_id} removido del Gateway")

    def dispatch_report_to_client(self, client_id, routing_key, body):
        with self._clients_lock:
            client_handler = self._clients_by_id.get(client_id)

        if not client_handler:
            logger.warning(f"No se encontró handler para cliente {client_id}, descartando reporte")
            return

        client_handler.enqueue_report_message(routing_key, body)

    def start(self):
        self._is_running = True
        logger.info("Iniciando Gateway...")
        
        try:
            self._acceptor.start()  
        except Exception as e:
            logger.error(f"Error en el Gateway: {e}")
            raise
        finally:
            self._cleanup()

    def _cleanup(self):
        logger.info("Iniciando cleanup del Gateway...")
        self._is_running = False
        
        try:
            if self._acceptor:
                self._acceptor.cleanup()
                self._acceptor.join()
            
            if self.output_filter_year_nodes_middleware:
                self.output_filter_year_nodes_middleware.close()
            if self._join_middleware:
                self._join_middleware.close()
            if self._report_dispatcher:
                try:
                    self._report_dispatcher.stop()
                    self._report_dispatcher.join(timeout=3.0)
                except Exception as e:
                    logger.error(f"Error deteniendo report dispatcher: {e}")
                
        except Exception as e:
            logger.error(f"Error durante cleanup: {e}")
        
        logger.info("Gateway cerrado completamente")
            

