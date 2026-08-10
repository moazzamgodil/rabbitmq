import asyncio
import json

import aio_pika


RABBITMQ_URL = "amqp://localhost/"


async def main():

    connection = await aio_pika.connect_robust(
        RABBITMQ_URL
    )

    channel = await connection.channel()

    # -----------------------------
    # Orders exchange
    # -----------------------------

    orders_exchange = await channel.declare_exchange(
        "orders",
        aio_pika.ExchangeType.TOPIC,
        durable=True
    )

    # -----------------------------
    # Notifications exchange
    # -----------------------------

    notifications_exchange = await channel.declare_exchange(
        "notifications",
        aio_pika.ExchangeType.TOPIC,
        durable=True
    )

    # -----------------------------
    # Order queue
    # -----------------------------

    queue = await channel.declare_queue(
        "order-service",
        durable=True
    )

    # -----------------------------
    # Binding
    # -----------------------------

    await queue.bind(
        orders_exchange,
        routing_key="order.created"
    )

    print("Order Service started...")

    # -----------------------------
    # Consume
    # -----------------------------

    async with queue.iterator() as queue_iter:

        async for message in queue_iter:

            async with message.process():

                order = json.loads(
                    message.body.decode()
                )

                print("\nORDER SERVICE")
                print("Order received:")
                print(order)

                print(
                    f"Processing order "
                    f"{order['order_id']}"
                )

                # -----------------------------
                # Create notification
                # -----------------------------

                notification = {
                    "type": "ORDER_CREATED",
                    "order_id": order["order_id"],
                    "customer": order["customer"],
                    "message": (
                        f"Your order "
                        f"{order['order_id']} was created"
                    )
                }

                # -----------------------------
                # Publish notification
                # -----------------------------

                notification_message = aio_pika.Message(
                    body=json.dumps(
                        notification
                    ).encode()
                )

                await notifications_exchange.publish(
                    notification_message,
                    routing_key="notification.send"
                )

                print(
                    "Notification event published"
                )


if __name__ == "__main__":
    asyncio.run(main())