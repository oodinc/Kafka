const { Kafka } = require('kafkajs');
const readline = require('readline');

const kafka = new Kafka({
  clientId: 'my-kafka-consumer',
  brokers: ['localhost:9092'],
});

const topic = 'my-topic';

let consumer;
let currentGroup;
let currentPartitions = [];

const rl = readline.createInterface({
  input: process.stdin,
  output: process.stdout,
});

const setupConsumer = () => {
  rl.question('Enter consumer group ID (1, 2, 3): ', async (groupNum) => {
    if (groupNum.toLowerCase() === 'exit') {
      console.log('Exiting...');
      if (consumer) await consumer.disconnect();
      rl.close();
      process.exit(0);
    }
    
    currentGroup = `group-${groupNum}`;
    
    if (consumer) {
      await consumer.disconnect();
    }
    
    consumer = kafka.consumer({ 
      groupId: currentGroup,
      fromBeginning: true 
    });
    
    console.log(`Created consumer with group ID: ${currentGroup}`);
    askForPartitions();
  });
};

const askForPartitions = () => {
  rl.question('Enter partitions to listen to (e.g., 0,1,2 or all): ', async (partitionsInput) => {
    if (partitionsInput.toLowerCase() === 'exit') {
      console.log('Exiting...');
      if (consumer) await consumer.disconnect();
      rl.close();
      process.exit(0);
    }
    
    if (partitionsInput.toLowerCase() === 'all') {
      currentPartitions = [0, 1, 2]; 
    } else {
      try {
        currentPartitions = partitionsInput.split(',')
          .map(p => parseInt(p.trim(), 10))
          .filter(p => !isNaN(p) && p >= 0 && p <= 2);
      } catch (err) {
        console.log('Invalid partition format. Using all partitions.');
        currentPartitions = [0, 1, 2];
      }
    }
    
    if (currentPartitions.length === 0) {
      console.log('No valid partitions specified. Using all partitions.');
      currentPartitions = [0, 1, 2];
    }
    
    console.log(`Listening to partitions: ${currentPartitions.join(', ')}`);
    startConsumer();
  });
};

const startConsumer = async () => {
  try {
    await consumer.connect();
    
    await consumer.subscribe({ 
      topic,
      fromBeginning: true
    });
    
    await consumer.run({
      partitionsConsumedConcurrently: currentPartitions.length,
      eachMessage: async ({ topic, partition, message }) => {
        if (currentPartitions.includes(partition)) {
          const key = message.key ? message.key.toString() : 'none';
          const value = message.value.toString();
          
          if (key === currentGroup) {
            console.log(`[${currentGroup}] Received from partition ${partition}: ${value}`);
          }
        }
      },
    });
    
    console.log(`Consumer with group ${currentGroup} is now listening on partitions ${currentPartitions.join(', ')}`);
    console.log(`Only showing messages for group ${currentGroup}`);
    console.log('Press Ctrl+C to exit or type "exit" to reconfigure...');
    
    listenForCommands();
  } catch (err) {
    console.error('Error in Kafka consumer:', err);
  }
};

const listenForCommands = () => {
  rl.question('', async (command) => {
    if (command.toLowerCase() === 'exit') {
      console.log('Disconnecting current consumer...');
      await consumer.disconnect();
      setupConsumer();
    } else {
      listenForCommands();
    }
  });
};

console.log('Kafka Consumer Setup');
setupConsumer();