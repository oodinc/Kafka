const { Kafka } = require('kafkajs');
const readline = require('readline');

const kafka = new Kafka({
    clientId: 'my-kafka-producer',
    brokers: ['192.168.50.230:9092'], 
});

const producer = kafka.producer();
const topic = 'my-topic'; 

const rl = readline.createInterface({
    input: process.stdin,
    output: process.stdout,
});

const runProducer = async () => {
    await producer.connect();
    console.log("Kafka Producer connected. Type a message to send, or type 'exit' to quit.");
    askForInput();
};

const askForInput = () => {
    rl.question('Enter message: ', async (input) => {
        if (input.toLowerCase() === 'exit') {
            console.log('Exiting...');
            await producer.disconnect();
            rl.close();
            process.exit(0);
        }

        try {
            await producer.send({
                topic,
                messages: [{ key: 'user-input', value: input }],
            });
            console.log('Message sent:', input);
        } catch (err) {
            console.error('Error sending message:', err);
        }

        askForInput(); 
    });
};

runProducer().catch((err) => console.error('Error starting producer:', err));