const { Kafka } = require('kafkajs');
const readline = require('readline');

const kafka = new Kafka({
  clientId: 'my-kafka-producer',
  brokers: ['localhost:9092'],
});

const producer = kafka.producer();
const topic = 'my-topic';

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const runProducer = async () => {
  await producer.connect();
  console.log("Kafka Producer connected. Type a message to send, or type 'exit' to restart.");
  askForGroup();
};

const askForGroup = () => {
  rl.question('Enter consumer group (1, 2, 3): ', (group) => {
    if (group.toLowerCase() === 'quit') {
      exitProducer();
      return;
    }
    
    if (group.toLowerCase() === 'exit') {
      console.log("Restarting selection process...");
      askForGroup();
      return;
    }
    
    const consumerGroup = `group-${group}`;
    askForPartition(consumerGroup);
  });
};

const askForPartition = (consumerGroup) => {
  rl.question('Enter partition (0, 1, 2): ', (partition) => {
    if (partition.toLowerCase() === 'quit') {
      exitProducer();
      return;
    }
    
    if (partition.toLowerCase() === 'exit') {
      console.log("Restarting selection process...");
      askForGroup();
      return;
    }
    
    const partitionNum = parseInt(partition, 10);
    if (isNaN(partitionNum) || partitionNum < 0 || partitionNum > 2) {
      console.log('Invalid partition. Please enter 0, 1, or 2');
      askForPartition(consumerGroup);
      return;
    }
    
    askForInput(consumerGroup, partitionNum);
  });
};

const askForInput = (consumerGroup, partition) => {
  rl.question('Enter message (type "exit" to change group/partition, "quit" to exit): ', async (input) => {
    if (input.toLowerCase() === 'quit') {
      exitProducer();
      return;
    }
    
    if (input.toLowerCase() === 'exit') {
      console.log("Returning to group selection...");
      askForGroup();
      return;
    }
    
    try {
      await producer.send({
        topic,
        messages: [
          {
            key: consumerGroup,
            value: input,
            partition: partition
          }
        ],
      });
      console.log(`Message sent to ${consumerGroup}, partition ${partition}: ${input}`);
    } catch (err) {
      console.error('Error sending message:', err);
    }
    
    askForInput(consumerGroup, partition);
  });
};

const exitProducer = async () => {
  console.log('Exiting program completely...');
  await producer.disconnect();
  rl.close();
  process.exit(0);
};

runProducer().catch((err) => console.error('Error starting producer:', err));