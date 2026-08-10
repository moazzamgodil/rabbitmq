import json

import aio_pika
from fastapi import FastAPI

app = FastAPI()

RABBITMQ_URL = "amqp://localhost/"


@app.on_event("startup")
async def startup():

    connection = await aio_pika.connect_robust(
        RABBITMQ_URL
    )

    app.state.connection = connection

    app.state.channel = await connection.channel()

    # Create orders exchange
    app.state.orders_exchange = await app.state.channel.declare_exchange(
        "orders",
        aio_pika.ExchangeType.TOPIC,
        durable=True
    )

    print("Connected to RabbitMQ")


@app.on_event("shutdown")
async def shutdown():

    await app.state.connection.close()


@app.post("/orders")
async def create_order(order: dict):

    body = json.dumps(order).encode()

    message = aio_pika.Message(
        body=body
    )

    await app.state.orders_exchange.publish(
        message,
        routing_key="order.created"
    )

    print("Order published:", order)

    return {
        "message": "Order published",
        "order": order
    }