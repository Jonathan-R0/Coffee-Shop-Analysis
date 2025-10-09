import threading
from typing import Optional
from rabbitmq.middleware import MessageMiddlewareQueue
from logger import get_logger

logger = get_logger(__name__)


class ReportDispatcher(threading.Thread):
    """Background dispatcher that routes report batches from RabbitMQ to active clients."""

    REPORT_QUEUE_NAME = "gateway_reports_shared"
    ROUTING_PATTERN = "client.#"

    def __init__(
        self,
        rabbitmq_host: str,
        reports_exchange: str,
        gateway,
        shutdown_handler=None,
    ):
        super().__init__(daemon=True)
        self._gateway = gateway
        self._shutdown = shutdown_handler
        self._host = rabbitmq_host
        self._exchange = reports_exchange
        self._middleware = MessageMiddlewareQueue(
            host=self._host,
            queue_name=self.REPORT_QUEUE_NAME,
            exchange_name=self._exchange,
            routing_keys=[self.ROUTING_PATTERN],
        )

        if self._shutdown and hasattr(self._middleware, "shutdown"):
            self._middleware.shutdown = self._shutdown

        self._stopped = threading.Event()

    def run(self):
        logger.info("ReportDispatcher iniciado")
        try:
            self._middleware.start_consuming(self._on_message)
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.error(f"ReportDispatcher detenido por error: {exc}")
        finally:
            self._stopped.set()
            self.cleanup()
            logger.info("ReportDispatcher detenido")

    def stop(self):
        """Solicita detener el consumo."""
        logger.info("Deteniendo ReportDispatcher...")
        try:
            self._middleware.stop_consuming()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.error(f"Error deteniendo ReportDispatcher: {exc}")

    def cleanup(self):
        try:
            self._middleware.close()
        except Exception as exc:  # pragma: no cover - defensive guard
            logger.error(f"Error cerrando middleware de reportes: {exc}")

    def _on_message(self, channel, method, properties, body):
        if self._shutdown and self._shutdown.is_shutting_down():
            logger.info("Shutdown detectado en ReportDispatcher")
            channel.stop_consuming()
            return

        client_id = self._extract_client_id(properties)
        if client_id is None:
            logger.warning(
                "Mensaje de reporte sin client_id, descartando. routing_key=%s",
                getattr(method, "routing_key", "unknown"),
            )
            return

        routing_key = getattr(method, "routing_key", "")
        logger.debug(
            "Despachando mensaje de reporte para cliente %s (routing %s)",
            client_id,
            routing_key,
        )
        self._gateway.dispatch_report_to_client(client_id, routing_key, body)

    @staticmethod
    def _extract_client_id(properties) -> Optional[int]:
        if not properties or not getattr(properties, "headers", None):
            return None
        client_id = properties.headers.get("client_id")
        if client_id is None:
            return None
        try:
            return int(client_id)
        except (TypeError, ValueError):
            logger.warning("client_id inválido en headers: %s", client_id)
            return None
