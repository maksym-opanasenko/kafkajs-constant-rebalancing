const { Kafka, logLevel } = require('kafkajs');

class CustomKafkaConsumer {
	constructor(kafkaConfig) {
		this.kafkaConfig = kafkaConfig;
	}

	async start() {
		const kafka = new Kafka({
			clientId: this.kafkaConfig.clientId,
			brokers: this.kafkaConfig.brokers,
			logLevel: logLevel.INFO,
		});
		const consumer = kafka.consumer({ groupId: this.kafkaConfig.groupId });

		await consumer.connect();
		await consumer.subscribe({ topic: this.kafkaConfig.topic, fromBeginning: true });
		await consumer.run({
			eachMessage: async ({ message }) => {
				console.log(`Processing message: ${message.key?.toString()}`);
				await new Promise(resolve => setTimeout(resolve, 5000));
			},
		});
	}
}

// Example usage:
const kafkaConfig = {
	brokers: ['localhost:9092'],
	topic: 'topic-name',
	groupId: 'group-id',
	clientId: 'client-id',
	heartbeatInterval: 3000,
};

const consumer = new CustomKafkaConsumer(kafkaConfig);
consumer.start().catch(console.error);
