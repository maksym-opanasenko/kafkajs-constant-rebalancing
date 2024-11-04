const { Kafka, logLevel, KafkaJSProtocolError } = require('kafkajs');

class CustomKafkaConsumer {
	constructor(kafkaConfig) {
		this.kafkaConfig = kafkaConfig;
		this.isRebalancing = false;
		this.heartbeatTimerId = null;
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

		const topics = [{ topic: this.kafkaConfig.topic }];

		// The REBALANCING event is not emitted for some reason because of an issue with the library, but is emitted after
		// the consumer is paused manually. Let's handle pausing here just in case;
		consumer.on(consumer.events.REBALANCING, () => {
			console.log('REBALANCING: Pausing consumer');
			this.isRebalancing = true;
			consumer.pause(topics);
		});

		consumer.on(consumer.events.GROUP_JOIN, () => {
			if (this.isRebalancing) {
				console.log('GROUP_JOIN: Resuming consumer');
				this.isRebalancing = false;
				consumer.resume(topics);
			}
		});

		await consumer.run({
			eachMessage: async ({ message, heartbeat }) => {
				// The problem is that there is no other way of getting the heartbeat function outside `eachMessage` and `eachBatch`
				// in the current version if kafkajs.
				this.scheduleHeartbeatIfNeeded(heartbeat);

				if (this.isRebalancing) {
					// Pausing the consumer manually as kafkajs still has messages in its internal `eachBatch` to call `eachMessage` with;
					consumer.pause(topics);
					console.log('Consumer is rebalancing, pausing');
					return;
				}

				// the message processing goes here, don't forget to await if you process async
				console.log(`Processing message: ${message.key.toString()}`);
				await new Promise(resolve => setTimeout(resolve, 5000));
			},
		});
	}

	// If messages processing can take long time so we can periodically send heartbeats to the Kafka brokers to signal that the
	// consumer is still alive, and should not be removed from its group. The timerId is stored as a property of the class
	// to make sure we always run a single timer and avoid spamming with the calls
	scheduleHeartbeatIfNeeded(heartbeat) {
		if (this.heartbeatTimerId) return;

		this.heartbeatTimerId = setInterval(async () => {
			try {
				await heartbeat();
			} catch (error) {
				if (CustomKafkaConsumer.isKafkaJsRebalancingError(error)) {
					console.warn('Rebalancing in progress; pausing heartbeat');
					this.isRebalancing = true;
					return;
				}
				console.error('Error sending heartbeat:', error);
				throw error;
			}
		}, this.kafkaConfig.heartbeatInterval);
	}

	static isKafkaJsRebalancingError(error) {
		return (
			error instanceof KafkaJSProtocolError &&
			['REBALANCE_IN_PROGRESS', 'NOT_COORDINATOR_FOR_GROUP', 'ILLEGAL_GENERATION'].includes(error.type)
		);
	}
}

// Example usage:
const kafkaConfig = {
	brokers: ['localhost:9092'],
	topic: 'ContentUpdatedTopic',
	groupId: 'pro-content-enricher-consumer-group',
	clientId: 'pro-content-enricher',
	heartbeatInterval: 3000,
};

const consumer = new CustomKafkaConsumer(kafkaConfig);
consumer.start().catch(console.error);
