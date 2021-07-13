package com.rabbitmq.stream;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.IntStream;

public class FirstApplication {
  static void log(String format, Object... arguments) {
    System.out.println(String.format(format, arguments));
  }

  public static class Publish {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment =
          Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {

        log("Connected");

        log("Creating stream...");
        environment.streamCreator().stream("first-application-stream").create();
        log("Stream created");

        log("Creating producer...");
        Producer producer =
            environment.producerBuilder().stream("first-application-stream").build();
        log("Producer created");

        long start = System.currentTimeMillis();
        int messageCount = 1_000_000;
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        log("Sending %,d messages", messageCount);
        IntStream.range(0, messageCount)
            .forEach(
                i -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .properties()
                          .creationTime(System.currentTimeMillis())
                          .messageId(i)
                          .messageBuilder()
                          .addData("hello world".getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> confirmLatch.countDown());
                });
        log("Messages sent, waiting for confirmation...");
        boolean done = confirmLatch.await(1, TimeUnit.MINUTES);
        log(
            "All messages confirmed? %s (%d ms)",
            done ? "yes" : "no", (System.currentTimeMillis() - start));
        log("Closing environment...");
      }
      log("Environment closed");
    }
  }

  public static class Consume {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment =
          Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {

        log("Connected");

        AtomicInteger messageConsumed = new AtomicInteger(0);
        long start = System.currentTimeMillis();
        log("Start consumer...");
        Consumer consumer =
            environment.consumerBuilder().stream("first-application-stream")
                .offset(OffsetSpecification.first())
                .messageHandler((context, message) -> messageConsumed.incrementAndGet())
                .build();

        Utils.waitAtMost(60, () -> messageConsumed.get() >= 1_000_000);
        log(
            "Consumed %,d messages in %s ms",
            messageConsumed.get(), (System.currentTimeMillis() - start));
        log("Closing environment...");
      }
      log("Environment closed");
    }
  }
}
