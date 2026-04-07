import pika
import signal
from .middleware import (
    MessageMiddlewareQueue,
    MessageMiddlewareExchange,
    MessageMiddlewareDisconnectedError,
    MessageMiddlewareMessageError,
    MessageMiddlewareCloseError,
)


def _build_pika_callback(on_message_callback):
    """
    Wrappea el callback de pika para exponer la interfaz (message, ack, nack)
    definida por la clase abstracta, ocultando los detalles de pika al consumidor.
    """
    def _callback(ch, method, properties, body):
        ack = lambda: ch.basic_ack(delivery_tag=method.delivery_tag)
        nack = lambda: ch.basic_nack(delivery_tag=method.delivery_tag)
        on_message_callback(body, ack, nack)
    return _callback


class MessageMiddlewareQueueRabbitMQ(MessageMiddlewareQueue):

    def __init__(self, host, queue_name):
        self._queue_name = queue_name
        self._channel = None
        self._connection = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.queue_declare(queue=queue_name, durable=True)
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.close()

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")

    def stop_consuming(self):
        """
        Detiene el loop de consumo si estaba activo. Si no, no tiene efecto.
        """
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stopping: {e}")

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange='',
                routing_key=self._queue_name,
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while sending: {e}")

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error while closing: {e}")


class MessageMiddlewareExchangeRabbitMQ(MessageMiddlewareExchange):

    def __init__(self, host, exchange_name, routing_keys):
        self._exchange_name = exchange_name
        self._routing_keys = routing_keys
        self._channel = None
        self._connection = None
        self._queue_name = None

        try:
            self._connection = pika.BlockingConnection(
                pika.ConnectionParameters(host=host)
            )
            self._channel = self._connection.channel()
            self._channel.exchange_declare(
                exchange=exchange_name,
                exchange_type='direct',
                durable=True
            )
            # Cola exclusiva y anónima por instancia: garantiza que en broadcast
            # cada consumer recibe su propia copia de los mensajes.
            # RabbitMQ la elimina automáticamente cuando la conexión se cierra.
            result = self._channel.queue_declare(queue='', exclusive=True)
            self._queue_name = result.method.queue
            for routing_key in routing_keys:
                self._channel.queue_bind(
                    exchange=exchange_name,
                    queue=self._queue_name,
                    routing_key=routing_key
                )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Failed to connect to RabbitMQ: {e}")

        signal.signal(signal.SIGTERM, self._handle_sigterm)

    def _handle_sigterm(self, signum, frame):
        self.close()

    def start_consuming(self, on_message_callback):
        try:
            self._channel.basic_consume(
                queue=self._queue_name,
                on_message_callback=_build_pika_callback(on_message_callback),
                auto_ack=False
            )
            self._channel.start_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while consuming: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while consuming: {e}")

    def stop_consuming(self):
        """
        Detiene el loop de consumo si estaba activo. Si no, no tiene efecto.
        """
        try:
            if self._channel and self._channel.is_open:
                self._channel.stop_consuming()
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while stopping: {e}")

    def send(self, message):
        try:
            self._channel.basic_publish(
                exchange=self._exchange_name,
                routing_key=self._routing_keys[0],
                body=message,
                properties=pika.BasicProperties(delivery_mode=2)
            )
        except pika.exceptions.AMQPConnectionError as e:
            raise MessageMiddlewareDisconnectedError(f"Connection lost while sending: {e}")
        except Exception as e:
            raise MessageMiddlewareMessageError(f"Error while sending: {e}")

    def close(self):
        try:
            if self._channel and self._channel.is_open:
                self._channel.close()
            if self._connection and self._connection.is_open:
                self._connection.close()
        except Exception as e:
            raise MessageMiddlewareCloseError(f"Error while closing: {e}")