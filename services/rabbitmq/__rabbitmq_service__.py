import pika
import pika.exceptions
import logging
import json

from settings import config
from datetime import datetime
from tenacity import retry, stop_after_attempt, wait_exponential, after_log
from circuitbreaker import circuit
from threading import local

logger = logging.getLogger(__name__)

@circuit(failure_threshold=5, recovery_timeout=60)
class RabbitMQService:
    def __init__(self):
        self.config = config.RABBITMQ_PARAMS
        self.connection = None
        self.channel_local = local()
        self.publisher_confirm_mode = self.config['publisher_confirm_mode']
        self._connected = False

    @retry(
        stop=stop_after_attempt(5),
        wait=wait_exponential(multiplier=1, max=10),
        after=after_log(logger, logging.INFO)
    )
    def _connect(self):
        """Establish or re-establish connection to RabbitMQ"""
        try:
            credentials = pika.PlainCredentials(
                username=self.config['username'],
                password=self.config['password']
            )

            parameters = pika.ConnectionParameters(
                client_properties={'connection_name': 'poc'},
                host=self.config['host'],
                port=self.config['port'],
                credentials=credentials,
                heartbeat=self.config['heartbeat'],
                blocked_connection_timeout=self.config['blocked_connection_timeout']
            )
            self.connection = pika.BlockingConnection(parameters)
            self._setup_channel()
            self._connected = True
            logger.info("Successfully connected to RabbitMQ.")
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("RabbitMQ connection failed: %s", str(e))
            self._connected = False
        except Exception as e:
            logger.error("Unexpected error during connection: %s", str(e))
            self._connected = False

    def is_connected(self):
        """Check if the connection is established"""
        return self.connection and not self.connection.is_closed

    def _setup_channel(self):
        """Create a new channel for the current thread"""
        if hasattr(self.channel_local, 'channel') and not self.channel_local.channel.is_closed:
            self.channel_local.channel.close()
        
        self.channel_local.channel = self.connection.channel()
        if self.publisher_confirm_mode:
            self.channel_local.channel.confirm_delivery()

    def get_channel(self):
        """Get a thread-local channel with automatic recovery"""
        try:
            if not self.connection or self.connection.is_closed:
                logger.info("Connection is closed, connecting...")
                self._connect()
            
            if not hasattr(self.channel_local, 'channel') or self.channel_local.channel.is_closed:
                self._setup_channel()
                logger.info("Created new channel for thread.")
              
            return self.channel_local.channel         
        except Exception as e:
            logger.error("Channel management failed: %s", str(e))
            self._connected = False
            return None

    def TryDeclareExchange(self, name: str, exchange_type: str = 'topic', durable: bool = True, auto_delete: bool = False, arguments: dict = None) -> bool:
        """Declare an exchange with idempotent behavior"""
        try:
            self.get_channel().exchange_declare(
                exchange=name,
                exchange_type=exchange_type,
                auto_delete=auto_delete,
                durable=durable,
                arguments=arguments,
                passive=False
            )
            logger.info("Exchange '%s' declared successfully", name)
            return True
            
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 406:  # PRECONDITION_FAILED
                logger.warning("Exchange '%s' already exists with different parameters", name)
                return True  # Consider as success for idempotency
            logger.error("Broker closed channel during exchange declaration: %s", str(e))
            return False
            
        except pika.exceptions.ChannelError as e:
            logger.error("Channel error during exchange declaration: %s", str(e))
            return False
            
        except Exception as e:
            logger.error("Unexpected error declaring exchange: %s", str(e))
            return False

    def TryDeclareQueue(self, name: str, durable: bool = True, auto_delete: bool = False, arguments: dict = None) -> bool:
        """Declare a queue with idempotent behavior"""
        try:
            self.get_channel().queue_declare(
                queue=name,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments,
                passive=False
            )
            logger.info("Queue '%s' declared successfully", name)
            return True
            
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 406:  # PRECONDITION_FAILED
                logger.warning("Queue '%s' already exists with different parameters", name)
                return True  # Consider as success for idempotency
            logger.error("Broker closed channel during queue declaration: %s", str(e))
            return False
            
        except pika.exceptions.ChannelError as e:
            logger.error("Channel error during queue declaration: %s", str(e))
            return False
            
        except Exception as e:
            logger.error("Unexpected error declaring queue: %s", str(e))
            return False

    def TryBindQueue(self, queue_name: str, exchange_name: str, routing_key: str, arguments: dict = None) -> bool:
        """Bind queue to exchange with error handling"""
        try:
            self.get_channel().queue_bind(
                queue=queue_name,
                exchange=exchange_name,
                routing_key=routing_key,
                arguments=arguments or {}
            )
            logger.info("Queue '%s' bound to exchange '%s' with routing key '%s'", 
                       queue_name, exchange_name, routing_key)
            return True
            
        except pika.exceptions.ChannelClosedByBroker as e:
            logger.error("Broker closed channel during queue binding: %s", str(e))
            return False
            
        except pika.exceptions.ChannelError as e:
            logger.error("Channel error during queue binding: %s", str(e))
            return False
            
        except Exception as e:
            logger.error("Unexpected error during queue binding: %s", str(e))
            return False

    def PublishMessage(self, exchange: str, routing_key: str, message: dict) -> bool:
        """Publish a message with robust error handling"""
        try:
            if not exchange or not isinstance(exchange, str):
                logger.error("Invalid exchange name")
                return False
            
            if not routing_key or not isinstance(routing_key, str):
                logger.error("Invalid routing key")
                return False

            logger.info("Publishing message to exchange '%s' with routing key '%s'", exchange, routing_key)

            body = json.dumps(message, default=str, ensure_ascii=False) 
            
            self.get_channel().basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=pika.BasicProperties(
                    delivery_mode=2,
                    content_type='application/json'
                )
            )
            
            logger.info("Message published to exchange '%s' with routing key '%s'", exchange, routing_key)
            return True
        except pika.exceptions.UnroutableError:
            logger.error("Message was unroutable")
            return False
        except pika.exceptions.AMQPConnectionError as e:
            logger.error("Connection error during publishing: %s", str(e))
        except Exception as e:
            logger.error("Unexpected error during publishing: %s", str(e))

    def CheckExchangeExists(self, exchange_name: str) -> bool:
        try:
            self.get_channel().exchange_declare(
                exchange=exchange_name,
                exchange_type="topic",
                passive=True
            )
            return True
        except pika.exceptions.ChannelClosedByBroker as e:
            if e.reply_code == 404:
                return False
            return False