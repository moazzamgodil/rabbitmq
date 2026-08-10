const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://localhost";

async function start() {

    const connection = await amqp.connect(
        RABBITMQ_URL
    );

    const channel = await connection.createChannel();

    await channel.assertExchange(
        "notifications",
        "topic",
        {
            durable: true
        }
    );

    const queue = await channel.assertQueue(
        "notification-service",
        {
            durable: true
        }
    );

    await channel.bindQueue(
        queue.queue,
        "notifications",
        "notification.send"
    );

    console.log(
        "Notification Service started..."
    );

    channel.consume(
        queue.queue,
        async (message) => {

            if (!message) return;

            const notification = JSON.parse(
                message.content.toString()
            );

            console.log(
                "Notification received:"
            );

            console.log(notification);


            // Send email / SMS / push notification

            console.log(
                `Sending notification for order ${notification.order_id}`
            );


            // Message successfully processed
            channel.ack(message);
        }
    );
}

start().catch(console.error);