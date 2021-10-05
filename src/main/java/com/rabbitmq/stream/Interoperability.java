package com.rabbitmq.stream;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class Interoperability {

  private static final String[] REGIONS = new String[] {"amer", "emea", "apac"};

  static void log(String format, Object... arguments) {
    System.out.println(String.format(format, arguments));
  }

  public static class CreateTopology {

    public static void main(String[] args) throws Exception {
      ConnectionFactory connectionFactory = new ConnectionFactory();
      log("Connecting...");
      try (Connection connection = connectionFactory.newConnection()) {
        log("Connecting...");
        log("Connected");
        Channel channel = connection.createChannel();
        log("Creating 'events' topic exchange...");
        channel.exchangeDeclare("events", BuiltinExchangeType.TOPIC);
        for (String region : REGIONS) {
          log("Creating '%s' queue and binding it to 'events' exchange...", region);
          channel.queueDeclare(region, true, false, false, null);
          channel.queueBind(region, "events", region);
        }
        log("Creating 'world' stream and binding it to 'events' exchange...");
        channel.queueDeclare(
            "world", true, false, false, Collections.singletonMap("x-queue-type", "stream"));
        channel.queueBind("world", "events", "*");
        log("Closing connection");
      }
    }
  }

  public static class Publish {

    public static void main(String[] args) throws Exception {
      ConnectionFactory connectionFactory = new ConnectionFactory();
      log("Connecting...");
      try (Connection connection = connectionFactory.newConnection()) {
        log("Connected");
        Channel channel = connection.createChannel();
        channel.confirmSelect();
        int messageCount = 100;
        log("Sending %,d messages", messageCount);
        for (int i = 0; i < messageCount; i++) {
          channel.basicPublish(
              "events",
              REGIONS[i % REGIONS.length],
              new AMQP.BasicProperties.Builder()
                  .messageId(String.valueOf(i))
                  .contentType("text/plain")
                  .build(),
              ("message " + i).getBytes(StandardCharsets.UTF_8));
        }
        channel.basicPublish("events", "whatever", null, "poison".getBytes(StandardCharsets.UTF_8));
        log("Messages sent, waiting for confirmation...");
        channel.waitForConfirmsOrDie();
        log("Messages confirmed");
        log("Closing connection");
      }
    }
  }

  public static class Consume {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment =
          Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {

        log("Connected");

        CountDownLatch latch = new CountDownLatch(1);
        log("Start consumer...");
        Consumer consumer =
            environment.consumerBuilder().stream("world")
                .offset(OffsetSpecification.first())
                .messageHandler(
                    (context, message) -> {
                      String body = new String(message.getBodyAsBinary());
                      if ("poison".equals(body)) {
                        latch.countDown();
                      } else {
                        log(
                            "Message #%s, content type '%s', from exchange %s with routing key %s",
                            message.getProperties().getMessageId(),
                            message.getProperties().getContentType(),
                            message.getMessageAnnotations().get("x-exchange"),
                            message.getMessageAnnotations().get("x-routing-key"));
                      }
                    })
                .build();

        boolean done = latch.await(60, TimeUnit.SECONDS);
        if (done) {
          log("Received poison message, stopping...");
        } else {
          log("Stopping consuming...");
        }

        log("Closing environment...");
      }
      log("Environment closed");
    }
  }
}
