const { Producer, Consumer } = require('./index');

async function go() {

    const brokers = [{
        host: 'localhost',
        port: 9092,
    }];
    const producer = new Producer('test-producer', brokers, { verbose: true });
    const consumer = new Consumer('test-consumer', 'test-group', brokers, { verbose: true });

    await producer.connect();
    console.log('Producer connected.');
    await consumer.connect();
    console.log('Consumer connected.');

    await consumer.bind({ topic: 'test-topic', fromBeginning: true }, ({ topic, message }) => {
        console.log(`[topic '${topic}'] - ${message.key} > ${message.value}`);
    })

    await consumer.init();

    setTimeout(() => {
        setInterval(() => { producer.sendMessage('test-topic', 'topic_id', { name: 'Ruan', age: 22 }) }, 3000);
    }, 2000);

}

go();