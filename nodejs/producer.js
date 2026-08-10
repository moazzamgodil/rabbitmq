const express = require("express");
const amqp = require("amqplib");

const app = express();

app.use(express.json());

const RABBITMQ_URL = "amqp://localhost";

let channel;

async function connectRabbitMQ() {

    const connection = await amqp.connect(RABBITMQ_URL);

    channel = await connection.createChannel();

    await channel.assertExchange(
        "orders",
        "topic",
        {
            durable: true
        }
    );

    console.log("Connected to RabbitMQ");
}

app.post("/orders", async (req, res) => {

    const order = {
        order_id: Date.now(),
        customer: req.body.customer,
        amount: req.body.amount
    };

    channel.publish(
        "orders",
        "order.created",
        Buffer.from(JSON.stringify(order))
    );

    console.log("Order published:", order);

    res.json({
        message: "Order published",
        order
    });
});


connectRabbitMQ()
    .then(() => {

        app.listen(8000, () => {
            console.log("API running on port 8000");
        });

    })
    .catch(console.error);