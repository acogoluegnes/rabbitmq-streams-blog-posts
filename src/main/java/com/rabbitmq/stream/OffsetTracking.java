package com.rabbitmq.stream;

import java.nio.charset.StandardCharsets;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class OffsetTracking {
  static void log(String format, Object... arguments) {
    System.out.println(String.format(format, arguments));
  }

  public static class PublishFirstWave {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment =
          Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {

        log("Connected");

        log("Creating stream...");
        environment.streamCreator().stream("offset-tracking-stream").create();
        log("Stream created");

        log("Creating producer...");
        Producer producer = environment.producerBuilder().stream("offset-tracking-stream").build();
        log("Producer created");

        long start = System.currentTimeMillis();
        int messageCount = 500_000;
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        log("Sending %,d messages", messageCount);
        IntStream.range(0, messageCount)
            .forEach(
                i -> {
                  String body = i == messageCount - 1 ? "poison" : "first wave";
                  Message message =
                      producer
                          .messageBuilder()
                          .properties()
                          .creationTime(System.currentTimeMillis())
                          .messageId(i)
                          .messageBuilder()
                          .addData(body.getBytes(StandardCharsets.UTF_8))
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
        Set<String> bodies = ConcurrentHashMap.newKeySet(10);
        CountDownLatch consumeLatch = new CountDownLatch(1);
        Consumer consumer =
            environment.consumerBuilder().stream("offset-tracking-stream")
                .offset(OffsetSpecification.first())
                .name("my-application")
                .manualTrackingStrategy()
                .builder()
                .messageHandler(
                    (context, message) -> {
                      String body = new String(message.getBodyAsBinary());
                      bodies.add(body);
                      if (messageConsumed.incrementAndGet() % 10_000 == 0) {
                        context.storeOffset();
                      }
                      if ("poison".equals(body)) {
                        context.storeOffset();
                        consumeLatch.countDown();
                      }
                    })
                .build();

        boolean done = consumeLatch.await(60, TimeUnit.SECONDS);
        if (!done) {
          log("Did not receive poison message to stop consuming");
        }

        log(
            "Consumed %,d messages in %s ms (bodies: %s)",
            messageConsumed.get(),
            (System.currentTimeMillis() - start),
            bodies.stream().collect(Collectors.joining(", ")));
        log("Closing environment...");
      }
      log("Environment closed");
    }
  }

  public static class PublishSecondWave {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment =
          Environment.builder().uri("rabbitmq-stream://localhost:5552").build()) {

        log("Connected");

        log("Creating producer...");
        Producer producer = environment.producerBuilder().stream("offset-tracking-stream").build();
        log("Producer created");

        long start = System.currentTimeMillis();
        int messageCount = 100_000;
        CountDownLatch confirmLatch = new CountDownLatch(messageCount);
        log("Sending %,d messages", messageCount);
        IntStream.range(0, messageCount)
            .forEach(
                i -> {
                  String body = i == messageCount - 1 ? "poison" : "second wave";
                  Message message =
                      producer
                          .messageBuilder()
                          .properties()
                          .creationTime(System.currentTimeMillis())
                          .messageId(i)
                          .messageBuilder()
                          .addData(body.getBytes(StandardCharsets.UTF_8))
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
}
