const fp = require("fastify-plugin");
const rabbitMQPlugin = require("fastify-rabbitmq");

module.exports = fp(
  async function (fastify) {
    let connectionEstablished = false;

    // @ts-ignore
    await fastify.register(rabbitMQPlugin, {
      connection: process.env.RABBITMQ_CONNECTION,
    });

    fastify.rabbitmq.once("connection", () => {
      connectionEstablished = true;
      fastify.log.info("RabbitMQ connected");
    });

    fastify.rabbitmq.on("error", (err) => {
      fastify.log.error(
        `RabbitMQ connection error: ${JSON.stringify(err, null, 2)}`
      );
    });

    fastify.ready().then(async () => {
      const dlxConsumer = fastify.rabbitmq.createConsumer(
        {
          queue: "dlq.consumer-1",
          queueOptions: { durable: true },
          queueBindings: [
            { exchange: "dlx.consumer-1", routingKey: "users.failed" },
          ],
          noAck: false,
        },
        async (msg) => {
          fastify.log.warn(
            `DLQ message received: ${JSON.stringify(msg, null, 2)}`
          );
        }
      );

      const consumer = fastify.rabbitmq.createConsumer(
        {
          queue: "q.consumer-1",
          queueOptions: {
            durable: true,
            arguments: {
              "x-dead-letter-exchange": "dlx.consumer-1",
              "x-dead-letter-routing-key": "users.failed",
            },
          },
          queueBindings: [
            { exchange: process.env.RABBITMQ_EXCHANGE, routingKey: "users" },
          ],
          noAck: false,
        },
        async (msg) => {
          fastify.log.info(`Received message: ${JSON.stringify(msg, null, 2)}`);
        }
      );

      fastify.log.info(
        `Consumer stats: ${JSON.stringify(consumer.stats, null, 2)}`
      );

      consumer.on("error", (err) => {
        fastify.log.error(`Consumer error: ${JSON.stringify(err, null, 2)}`);
      });

      dlxConsumer.on("error", (err) => {
        fastify.log.error(
          `DLQ Consumer error: ${JSON.stringify(err, null, 2)}`
        );
      });
    });

    fastify.addHook("onClose", async (instance, done) => {
      try {
        fastify.log.info("Closing RabbitMQ connection...");
        await fastify.rabbitmq.close();
        fastify.log.info("RabbitMQ connection closed successfully");
        done();
      } catch (error) {
        fastify.log.error(
          `Error closing RabbitMQ connection: ${error.message}`
        );
        done(error);
      }
    });
  },
  { name: "rabbitmq" }
);
