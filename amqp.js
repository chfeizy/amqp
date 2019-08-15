'use strict';
const config = require("../config/config");
const logger = require('./logger').logger('amqp');
let url = 'amqp://' + config.rabbitmq.username + ':' + config.rabbitmq.password + '@' + config.rabbitmq.hostname + ':' + config.rabbitmq.port + (typeof config.rabbitmq.vhost !== "undefined" ? ('/' + config.rabbitmq.vhost) : '');

let conn = null;
let amqpChPublisher = null;
let amqpChConsumer = null;
const getConn = async (io) => {
  if (conn == null) {
    logger.info("amqpConn", process.pid);
    try {
      let amqp = require('amqplib').connect(url);
      conn = await amqp;
      conn.on('close', async function () {
        logger.error("amqpConn close");
        conn.close();
        conn = null;
        amqpChPublisher = null;
        amqpChConsumer = null;
        setTimeout(async () => {
          await reContent(io);
        }, 5000);
      });
      conn.setMaxListeners(0);
      amqpChPublisher = await conn.createChannel();
      amqpChPublisher.setMaxListeners(0);
      amqpChConsumer = await conn.createChannel();
      amqpChConsumer.setMaxListeners(0);
    } catch (e) {
      logger.error("amqp conn", e);
      conn = null;
      setTimeout(async () => {
        await reContent(io);
      }, 5000);
      return null;
    }
  }
};

const reContent = async (io) => {
  logger.info("reContenting");
  if (conn == null) {
    try {
      let amqp = require('amqplib').connect(url);
      conn = await amqp;
      conn.on('close', async function () {
        logger.error("reContent close");
        conn = null;
        amqpChPublisher = null;
        amqpChConsumer = null;
        setTimeout(async () => {
          await reContent(io);
        }, 5000);
      });
      conn.setMaxListeners(0);
      amqpChPublisher = await conn.createChannel();
      amqpChPublisher.setMaxListeners(0);
      amqpChConsumer = await conn.createChannel();
      amqpChConsumer.setMaxListeners(0);
      const consumers = require('require-all')({
        dirname: __dirname + '/consumer'
      });
      Object.keys(consumers).forEach(function (key) {
        (async () => {
          try {
            await consumers[key](io);
          } catch (e) {
            logger.error(key, e, process.pid)
          }
        })();
      });
    } catch (e) {
      logger.error("reContent conn", e);
      conn = null;
      amqpChPublisher = null;
      amqpChConsumer = null;
      setTimeout(async () => {
        await reContent(io);
      }, 5000);
    }
  }
};
function publisherBack(){
  setTimeout(async ()=>{
    await publisher(this.queue, this.exchange, this.msg);
  },1000);
}
//生产
const publisher = async (queue, exchange, msg) => {
  try {
    if (amqpChPublisher == null) {
      if (conn != null) {
        amqpChPublisher = await conn.createChannel();
        amqpChPublisher.setMaxListeners(0);
      }
      if (amqpChPublisher == null) {
        logger.error("publisher ch null");
        publisherBack.call({queue:queue,exchange:exchange,msg:msg});
        return;
      }
    }
    // ch.setMaxListeners(0);

      if (queue !== '') {
        await amqpChPublisher.assertQueue(queue);
      }
      await amqpChPublisher.assertExchange(exchange, 'fanout');
      if (queue !== '') {
        await amqpChPublisher.bindQueue(queue, exchange);
      }
      await amqpChPublisher.publish(exchange, '', Buffer.from(msg));
      //ch.close();

  } catch (e) {
    logger.error("publisher", e);
    //amqpChPublisher.close();
    //amqpChPublisher = null;
  }
};
//消费
const consumer = async (queue, exchange, cb) => {
  try {
    if (amqpChConsumer == null) {
      logger.error("consumer ch null");
      return;
    }
    await amqpChConsumer.prefetch(1, true);
    await amqpChConsumer.assertQueue(queue);
    await amqpChConsumer.assertExchange(exchange, 'fanout');
    await amqpChConsumer.bindQueue(queue, exchange);
    return amqpChConsumer.consume(queue, async (msg) => {
      if (msg !== null) {
        try {
          await cb(msg.content.toString());
        } catch (e) {
          logger.error("consumer error", e, msg)
        }
        amqpChConsumer.ack(msg);
      }
    });

  } catch (e) {
    logger.error("consumer", e);
  }
};
module.exports.getConn = getConn;
module.exports.publisher = publisher;
module.exports.consumer = consumer;
