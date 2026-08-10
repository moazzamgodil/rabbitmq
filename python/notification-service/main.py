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
    # Notifications exchange
    # -----------------------------

    exchange = await channel.declare_exchange(
        "notifications",
        aio_pika.ExchangeType.TOPIC,
        durable=True
    )

    # -----------------------------
    # Notification queue
    # -----------------------------

    queue = await channel.declare_queue(
        "notification-service",
        durable=True
    )

    # -----------------------------
    # Binding
    # -----------------------------

    await queue.bind(
        exchange,
        routing_key="notification.send"
    )

    print(
        "Notification Service started..."
    )

    # -----------------------------
    # Consume
    # -----------------------------

    async with queue.iterator() as queue_iter:

        async for message in queue_iter:

            async with message.process():

                notification = json.loads(
                    message.body.decode()
                )

                print("\nNOTIFICATION SERVICE")

                print(
                    "Notification received:"
                )

                print(notification)

                print(
                    f"Sending notification "
                    f"for order "
                    f"{notification['order_id']}"
                )


if __name__ == "__main__":
    asyncio.run(main())