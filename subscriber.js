const config = require('./config');
const amqplib = require('amqplib/callback_api');
const nodemailer = require('nodemailer');
const dotenv = require('dotenv');
let aws = require('aws-sdk');
dotenv.config();

// Setup Nodemailer transport
const transport = nodemailer.createTransport({
    SES: new aws.SES({
        apiVersion: '2010-12-01',
        region: process.env.AWS_REGION,
        accessKeyId: process.env.AWS_ACCESS_KEY_ID,
        secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY
    })
});

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

        channel.assertExchange(config.exchange, 'fanout', {
            durable: true
        });

        // Ensure queue for messages
        channel.assertQueue(config.queue, {
            // Ensure that the queue is not deleted when server restarts
            // exclusive: true,
            durable: true
        }, err => {
            if (err) {
                console.error(err.stack);
                return process.exit(1);
            }

            console.log(' [*] Waiting for messages in %s. To exit press CTRL+C', config.queue);
            channel.bindQueue(config.queue, config.exchange, '');

            // Only request 1 unacked message from queue
            // This value indicates how many messages we want to process in parallel
            channel.prefetch(1);

            // Set up callback to handle messages received from the queue
            channel.consume(config.queue, data => {
                if (data === null) {
                    return;
                }

                // Decode message contents
                let message = JSON.parse(data.content.toString());
                const headers = Object.keys(message[0].data);
                let html = '<table><thead><tr>' +
                    headers.map(x => `<th>${x}</th>`).join('') +
                    '</tr></thead><tbody>' +
                    message.map(x => `<tr>${headers.map(key => `<td>${x.data[key] ? x.data[key] : '-'}</td>`).join('')}</tr>`).join('') +
                    '</tbody></table>';

                // attach message specific authentication options
                // this is needed if you want to send different messages from
                // different user accounts
                // message.auth = {
                //     user: 'testuser',
                //     pass: 'testpass'
                // };

                // Send the message using the previously set up Nodemailer transport
                transport.sendMail({
                    to: 'antoine@reliefapplications.org',
                    from: 'antoine@reliefapplications.org',
                    subject: 'test',
                    // text: message
                    html
                }, (err, info) => {
                    if (err) {
                        console.error(err.stack);
                        // put the failed message item back to queue
                        return channel.nack(data);
                    }
                    console.log('Delivered message %s', info.messageId);
                    // remove message item from the queue
                    channel.ack(data);
                });
            });
        });
    });
});
