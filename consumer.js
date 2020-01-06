const { Kafka, logLevel } = require('kafkajs')

class Consumer {

    /**
     * 
     * @param {String} nodeId 
     * @param {String} groupId 
     * @param {Array} brokers
     * @param {Object} param2 
     */
    constructor(nodeId, groupId, brokers, { verbose }) {
        this.kafka = new Kafka({
            logLevel: logLevel.ERROR,
            brokers: brokers.map(b => `${b.host}:${b.port}`),
            clientId: nodeId || 'example-producer',
        });
        this.consumer = this.kafka.consumer({ groupId: groupId || 'test-group' });
        this.verbose = verbose === true;
        this.callbacks = new Map();
    }

    async connect() {
        await this.consumer.connect();
    }

    /**
     * 
     * @param {Object} topicOptions 
     * @param {String} topicOptions.topic
     * @param {boolean} topicOptions.fromBeginning
     * @param {Function} callback 
     */
    async bind(topicOptions, callback) {
        const { topic } = topicOptions;
        let recoveredList = this.callbacks.get(topic);

        if (!recoveredList) {
            await this.consumer.subscribe(topicOptions);
            this.callbacks.set(topic, recoveredList = new Array());
        }

        recoveredList.push(callback);
    }

    async init() {
        this.consumer.run({
            eachMessage: async (message) => {
                const { topic } = message;
                const recoveredList = this.callbacks.get(topic);
                if (recoveredList) {
                    recoveredList.forEach(cb => cb({ topic: topic, data: JSON.parse(message.message.value || '{}'), key: message.message.key }));
                }
            }
        })
    }
}


module.exports = Consumer;
