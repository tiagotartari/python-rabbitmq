from fastapi import FastAPI, Request

from .health.routes import router as health_router
from .customer.routes import router as customer_router
from datetime import datetime

import logging
import json
import time

app = FastAPI(title="Infracommerce - RabbitMQ", version="0.1.0")

logger = logging.getLogger("http")
logger.setLevel(logging.INFO)

class JsonFormatter(logging.Formatter):
    def format(self, record):
        return json.dumps(record.msg)

handler = logging.StreamHandler()
handler.setFormatter(JsonFormatter())
logger.addHandler(handler)

@app.middleware("http")
async def log_requests(request: Request, call_next):
    start_time = time.time()
    response = await call_next(request)
    process_time = (time.time() - start_time) * 1000  # ms
    logger.info(f"{request.method} {request.url.path} {response.status_code} {process_time:.2f}ms")
    return response

app.include_router(customer_router)
app.include_router(health_router)