

var amqp = require('amqplib/callback_api');

function productMS(){
amqp.connect('amqp://admin:adminkaishu@192.168.1.211', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var ex = 'campass.source';
    var msg = process.argv.slice(2).join(' ') || 'Hello World!';
    ch.assertExchange(ex, 'topic', {durable: true});
//    ch.assertQueue(queue_name, {exclusive: true});
    //ch.bindQueue(queue_name, ex, 'testKey');
    ch.publish(ex, "file.result.load", new Buffer(msg), {deliveryMode : 2});
    console.log(" [x] Sent %s", msg);
  });

  setTimeout(function() { conn.close(); process.exit(0) }, 1000);
});

}

function consumeMS(){

amqp.connect('amqp://admin:adminkaishu@192.168.1.211', function(err, conn) {
  conn.createChannel(function(err, ch) {
    var ex = 'logs';
    var queue_name = 'testQueue';
    ch.assertExchange(ex, 'topic', {durable: false});

    ch.assertQueue(queue_name, {exclusive: true}, function(err, q) {
      console.log(" [*] Waiting for messages in %s. To exit press CTRL+C", q.queue);
      ch.bindQueue(q.queue, ex, '');

      ch.consume(q.queue, function(msg) {
        console.log(" [x] %s", msg.content.toString());
      }, {noAck: false});
    });
  });
});

}

productMS();
