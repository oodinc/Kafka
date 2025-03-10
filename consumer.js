const { Kafka } = require('kafkajs');

const kafka = new Kafka({
    clientId: 'my-kafka-consumer',
    brokers: ['192.168.50.230:9092'], 
});

const consumer = kafka.consumer({ groupId: 'my-group' });

const topic = 'my-topic'; 
consumer.subscribe({ topic });

const runConsumer = async () => {
    await consumer.run({
        eachMessage: async ({ topic, partition, message }) => {
            console.log(`Received message from topic ${topic}, partition ${partition}: ${message.value.toString()}`);
        },
    });
};

runConsumer().catch((err) => {
    console.error('Error in Kafka consumer:', err);
});
