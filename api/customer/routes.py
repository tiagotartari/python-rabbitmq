from fastapi import APIRouter, Depends, HTTPException
from datetime import datetime
from api.customer.models import CustomerRequest, CustomerResponse
from services.rabbitmq.rabbitmq_publisher import RabbitMQPublisher

import requests
import logging
import uuid

logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

router = APIRouter(prefix="/customer", tags=["customer"])
rabbitmq_publisher = RabbitMQPublisher()

DEFAULT_EXCHANGE = "customer_events"

@router.post("", response_model=CustomerResponse, status_code=201, summary="Create a new customer", description="Receives customer data")
async def create_customer(
    request: CustomerRequest,
    rabbitmq_publisher: RabbitMQPublisher = Depends(lambda: rabbitmq_publisher)
):
    """
    Create a new customer by publishing event to RabbitMQ

    Args:
        request: Customer data (name, email, uuid)
        rabbitmq_service: RabbitMQ service instance
        
    Returns:
        CustomerResponse: Echoes back the input data
    """
   
    logger.info("Received request to create customer: %s", str(request))
    routing_key = "customer.created"
    payload = {
        "id": str(uuid.uuid4()),
        "timestamp": datetime.utcnow().isoformat(),
        "routing_key": routing_key,
        "data": request.dict()
    }
    logger.info("Payload to be sent to RabbitMQ: %s", str(payload))

    try:
        result = (
            rabbitmq_publisher
                .declare_exchange(name=DEFAULT_EXCHANGE, exchange_type="topic")
                .declare_queue(name="customer.created")
                .bind_queue(queue_name="customer.created", exchange_name=DEFAULT_EXCHANGE, routing_key=routing_key)
                .publish(exchange=DEFAULT_EXCHANGE,routing_key=routing_key, message=payload)
                .result()              
        )
        
        rabbitmq_publisher.reset()

        success = bool(result['success'])
        if not success:
            logger.error("Failed to publish message to RabbitMQ %s", str(payload))
            raise HTTPException(status_code=500, detail="Failed to process customer creation.")

        logger.info("Customer creation event published successfully: %s", str(payload))
        
        return CustomerResponse(
            name=request.name,
            email=request.email,
            uuid=request.uuid
        )
    
    except HTTPException as e:
        logger.error("HTTPException: %s", str(e.detail))
        raise e
    except requests.RequestException as e:
        logger.error("RequestException: %s", str(e))
        raise HTTPException(status_code=500, detail="Failed to process customer creation.")
    except Exception as e:
        logger.error("Unexpected error: %s", str(e))
        raise HTTPException(status_code=500, detail="Failed to process customer creation.")


