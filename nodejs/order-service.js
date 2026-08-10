const amqp = require("amqplib");

const RABBITMQ_URL = "amqp://localhost";

async function start() {

    const connection = await amqp.connect(
        RABBITMQ_URL
    );

    const channel = await connection.createChannel();

    // Exchange
    await channel.assertExchange(
        "orders",
        "topic",
        { durable: true }
    );

    await channel.assertExchange(
        "notifications",
        "topic",
        { durable: true }
    );

    // Create order queue
    const orderQueue = await channel.assertQueue(
        "order-service",
        { durable: true }
    );

    // Binding: Order queue receives order.created
    await channel.bindQueue(
        orderQueue.queue,
        "orders",
        "order.created"
    );

    console.log("Order Service started...");

    channel.consume(
        orderQueue.queue,
        (message) => {

            if (!message) {
                return;
            }

            const order = JSON.parse(
                message.content.toString()
            );

            console.log("Order received:");
            console.log(order);

            console.log(
                `Processing order ${order.order_id}`
            );

            // Create notification event
            const notification = {
                type: "ORDER_CREATED",
                order_id: order.order_id,
                customer: order.customer,
                message: `Your order ${order.order_id} was created`
            };

            channel.publish(
                "notifications",
                "notification.send",
                Buffer.from(JSON.stringify(notification))
            );

             console.log(
                "Notification event published"
            );

            // Tell RabbitMQ:
            // message processed successfully

            channel.ack(message);
        }
    );
}

start().catch(console.error);