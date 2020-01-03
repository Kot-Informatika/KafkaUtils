const { Kafka, CompressionTypes, logLevel } = require('kafkajs')
class Producer {
/**
 * 
 * @param {String} nodeId 
 * @param {Array} brokers 
 * @param {Object} param2 
 */
    constructor(nodeId, brokers, { verbose }) {
        this.kafka = new Kafka({
            logLevel: logLevel.ERROR,
            brokers: brokers.map(b => `${b.host}:${b.port}`),
            clientId: nodeId || 'example-producer',
        });
        this.producer = this.kafka.producer()
        this.verbose = verbose === true;
    }

    async connect() {
        await this.producer.connect();
    }

    /**
     * 
     * @param {String} topic 
     * @param {String} key 
     * @param {Object} value 
     */
    async sendMessage(topic, key, value) {
        try {
            const response = await this.producer
                .send({
                    topic,
                    compression: CompressionTypes.GZIP,
                    messages: [
                        {
                            key,
                            value: JSON.stringify(value)
                        }
                    ]
                })
            return response;
        } catch (ex) {
            if (this.verbose) {
                console.error(`[kafka] ${e.message}`, e);
            }
            throw ex;
        }
    }
}

module.exports = Producer;
