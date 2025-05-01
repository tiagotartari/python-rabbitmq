from typing import Optional, Dict, Any
from datetime import datetime
from uuid import uuid4
from services.rabbitmq.__rabbitmq_service__ import RabbitMQService

import logging

logger = logging.getLogger(__name__)

class RabbitMQPublisher:
    def __init__(self):
        self._operation_id = str(uuid4())
        self._success = True
        self._exchange: Optional[str] = None
        self._queue: Optional[str] = None
        self._routing_key: Optional[str] = None
        self._step = "init"
        self._service = RabbitMQService()
        self._log("info", "Fluent workflow initialized.")

    @property
    def success(self) -> bool:
        return self._success
    
    def _log(self, level: str, message: str, **kwargs):
        """Log workflow events with operation context"""
        log_data = {
            "operation_id": self._operation_id,
            "timestamp": datetime.utcnow().isoformat(),
            "step": self._step,
            "status": "success" if level != "error" else "failed",
            "exchange": self._exchange,
            "queue": self._queue,
            "routing_key": self._routing_key,
            "current_success": self._success
        }
        
        getattr(logger, level)(
            f"FluentRabbitMQ: {message}",
            extra=log_data
        )    

    def declare_exchange(self, name: str, exchange_type: str = 'topic', **kwargs):
        """Declare exchange with fluent interface"""
        if not self._success:
            return self
            
        self._step = "declare_exchange"
        self._log("info", "Starting exchange declaration.")
        
        try:
            if not self._service.CheckExchangeExists(name):
                success = self._service.TryDeclareExchange(name, exchange_type, **kwargs)
                self._success = success
                if success:
                    self._exchange = name
                    self._log("info", "Exchange declared successfully.")
                else:
                    self._log("error", "Exchange declaration failed.")
            else:
                self._log("info", f"Exchange '{name}' already exists. Skipping declaration.")
                self._exchange = name
                self._success = True

        except Exception as e:
            self._success = False
            self._log("error", f"Exchange declaration error: {str(e)}", error=str(e))
        return self        
    
    def declare_queue(self, name: str, **kwargs):
        """Declare queue with fluent interface"""
        if not self._success:
            return self
            
        self._step = "declare_queue"
        self._log("info", "Starting queue declaration.")
        
        try:
            success = self._service.TryDeclareQueue(name, **kwargs)
            self._success = success
            if success:
                self._queue = name
                self._log("info", "Queue declared successfully.")
            else:
                self._log("error", "Queue declaration failed.")
        except Exception as e:
            self._success = False
            self._log("error", f"Queue declaration error: {str(e)}", error=str(e))
            
        return self
    
    def bind_queue(self, queue_name: str, exchange_name: str, routing_key: str, **kwargs):
        """Bind queue to exchange with fluent interface"""
        if not self._success:
            return self
            
        self._step = "bind_queue"
        self._log("info", "Starting queue binding.")
        
        try:
            success = self._service.TryBindQueue(queue_name, exchange_name, routing_key, **kwargs)
            self._success = success
            if success:
                self._queue = queue_name
                self._exchange = exchange_name
                self._routing_key = routing_key
                self._log("info", "Queue bound successfully.")
            else:
                self._log("error", "Queue binding failed.")
        except Exception as e:
            self._success = False
            self._log("error", f"Queue binding error: {str(e)}", error=str(e))
            
        return self
    
    def publish(self, exchange: str, routing_key: str, message: str, headers: Dict[str, Any] = None):
        """Publish message with fluent interface"""
        if not self._success:
            return self
            
        self._step = "publish"
        self._log("info", "Starting message publication.")
        
        try:
            success = self._service.PublishMessage(exchange, routing_key, message)
            self._success = success
            if success:
                self._log("info", "Message published successfully.")
            else:
                self._log("error", "Message publication failed.")
        except Exception as e:
            self._success = False
            self._log("error", f"Message publication error: {str(e)}", error=str(e))
            
        return self
    
    def result(self) -> Dict[str, Any]:
        """Get final workflow result"""
        return {
            "operation_id": self._operation_id,
            "success": self._success,
            "exchange": self._exchange,
            "queue": self._queue,
            "routing_key": self._routing_key
        }
    
    def reset(self):
        """Reset workflow for reuse"""
        self._log("info", "Resetting workflow.")
        self._success = True
        self._exchange = None
        self._queue = None
        self._routing_key = None