# amqp
node.js amqplib Conn reContent publisher consumer Package
(async () => {
    await amqp.getConn(io);
    Object.keys(consumers).forEach(function (key) {
      (async () => {
        try {
          await consumers[key](io);
        } catch (e) {
          logger.error(key, e, process.pid)
        }
      })();
    });
  })();
