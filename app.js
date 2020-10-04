const amqp = require("amqplib/callback_api");
const mongoose = require("mongoose");
const Request = require("./request.model");

let amqpConf = {
  conn: null,
  chan: null,
};

mongoose
  .connect("mongodb://localhost:27017/ide", {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    useFindAndModify: false
  })
  .then(() => {
    console.log("dbservice   |   successfully connected to mongodb!");
  })
  .catch((err) => {
    throw err;
  });

amqp.connect("amqp://guest:guest@localhost:5672/", (err, connection) => {
  if (err) {
    throw err;
  }
  console.log("dbservice   |   successfully connected to RabbitMQ:5672");
  amqpConf.conn = connection;

  connection.createChannel((err, ch) => {
    if (err) {
      throw err;
    }

    amqpConf.chan = ch;

    console.log("dbservice   |   successfully created rabbitmq channel");

    ch.assertQueue("DB_UPDATE", { durable: false }, (err, queue) => {
      if (err) {
        throw err;
      }

      console.log("dbservice   |   created queue", queue);

      ch.consume(
        "DB_UPDATE",
        (msg) => {
          const updateBody = JSON.parse(msg.content.toString());

          Request.findOneAndUpdate({ id: updateBody.id }, updateBody).then(
            () => {
              console.log("updated", updateBody.id);
            }
          );
        },
        { noAck: true }
      );
    });
  });
});
