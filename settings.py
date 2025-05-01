from dotenv import load_dotenv

import os
import logging

load_dotenv()

logging.getLogger('pika').setLevel(logging.WARNING)

logging.basicConfig(
    level=os.getenv('LOG_LEVEL', 'INFO').upper(),
    format='%(asctime)s [%(levelname)s] %(module)s.%(funcName)s:%(lineno)d - %(message)s'
)

class config:
    APP_PARAMS = {
        'port': int(os.getenv('APP_PORT', 8000)),
        'host': os.getenv('APP_HOST', '0.0.0.0'),
        'log_level': os.getenv('LOG_LEVEL', 'info'),
    }
    
    RABBITMQ_PARAMS = {
        'host': os.getenv('RABBITMQ_HOST', 'localhost'),
        'port': os.getenv('RABBITMQ_PORT', 5672),
        'username': os.getenv('RABBITMQ_USER', 'guest'),
        'password': os.getenv('RABBITMQ_PASSWORD', 'guest'),
        'heartbeat': int(os.getenv('RABBITMQ_HEARTBEAT', 600)),
        'blocked_connection_timeout': int(os.getenv('RABBITMQ_BLOCKED_CONNECTION_TIMEOUT', 300)),
        'publisher_confirm_mode': bool(os.getenv('RABBITMQ_PUBLISHER_CONFIRM_MODE', True)),
    }