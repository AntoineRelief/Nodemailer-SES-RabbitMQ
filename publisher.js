'use strict';

const amqplib = require('amqplib/callback_api');
const config = require('./config');
const dotenv = require('dotenv');
dotenv.config();

// Create connection to AMQP server
amqplib.connect(`amqp://${process.env.RABBITMQ_DEFAULT_USER}:${process.env.RABBITMQ_DEFAULT_PASS}@${process.env.RABBITMQ_HOST}:5672?heartbeat=30`, (err, connection) => {
    if (err) {
        console.error(err.stack);
        return process.exit(1);
    }

    // Create channel
    connection.createChannel((err, channel) => {
        if (err) {
            console.error(err.stack);
            return process.exit(1);
        }

        // Ensure queue for messages
        channel.assertQueue(config.queue, {
            // Ensure that the queue is not deleted when server restarts
            durable: true
        }, err => {
            if (err) {
                console.error(err.stack);
                return process.exit(1);
            }

            // Create a function to send objects to the queue
            // Javascript object is converted to JSON and then into a Buffer
            let sender = (content, next) => {
                let sent = channel.sendToQueue(config.queue, Buffer.from(JSON.stringify(content)), {
                    // Store queued elements on disk
                    persistent: true,
                    contentType: 'application/json'
                });
                if (sent) {
                    return next();
                } else {
                    channel.once('drain', () => next());
                }
            };

            // push 100 messages to queue
            let sent = 0;
            let sendNext = () => {
                if (sent >= 1) {
                    console.log('All messages sent!');
                    // Close connection to AMQP server
                    // We need to call channel.close first, otherwise pending
                    // messages are not written to the queue
                    return channel.close(() => connection.close());
                }
                sent++;
                sender({
                    to: 'antoine@reliefapplications.org',
                    subject: 'Test message #' + sent,
                    // text: 'hello world!'
                    html: '<table><thead><th>login</th></thead><tr><td>test</td></tr></table>'
                }, sendNext);
            };

            sendNext();

        });
    });
});
