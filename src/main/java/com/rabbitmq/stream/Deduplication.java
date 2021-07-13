package com.rabbitmq.stream;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class Deduplication {

  static void log(String format, Object... arguments) {
    System.out.println(String.format(format, arguments));
  }

  static List<String> messages(int count) {
    return IntStream.range(0, count).mapToObj(i -> "message " + i).collect(Collectors.toList());
  }

  public static class CreateEmptyStream {

    public static void main(String[] args) {
      log("Connection...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected. Trying to delete stream if it exists.");
        try {
          environment.deleteStream("deduplication-stream");
          log("Stream deleted.");
        } catch (StreamException e) {
          if (e.getCode() != Constants.RESPONSE_CODE_STREAM_DOES_NOT_EXIST) {
            log("Error while trying to delete stream: %d", e.getCode());
          }
        }
        log("Creating 'deduplication-stream' stream.");
        environment.streamCreator().stream("deduplication-stream").create();
        log("Stream created.");
      }
    }
  }

  public static class PublishFirstDay {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        Producer producer = environment.producerBuilder().stream("deduplication-stream").build();
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        log("Publishing %d messages.", messageCount);
        messages(messageCount)
            .forEach(
                payload -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .addData(payload.getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> latch.countDown());
                });
        boolean done = latch.await(10, TimeUnit.SECONDS);
        log("Messages confirmed? %s", done ? "yes" : "no");
      }
    }
  }

  public static class PublishSecondDay {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        Producer producer = environment.producerBuilder().stream("deduplication-stream").build();
        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        log("Publishing %d messages.", messageCount);
        messages(messageCount)
            .forEach(
                payload -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .addData(payload.getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> latch.countDown());
                });
        boolean done = latch.await(10, TimeUnit.SECONDS);
        log("Messages confirmed? %s", done ? "yes" : "no");
      }
    }
  }

  public static class PublishDedupFirstDay {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        Producer producer =
            environment.producerBuilder().stream("deduplication-stream").name("app-1").build();
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        List<String> messages = messages(messageCount);
        log("Publishing %d message with deduplication enabled.");
        for (int i = 0; i < messageCount; i++) {
          String payload = messages.get(i);
          Message message =
              producer
                  .messageBuilder()
                  .publishingId(i)
                  .addData(payload.getBytes(StandardCharsets.UTF_8))
                  .build();
          producer.send(message, confirmationStatus -> latch.countDown());
        }
        boolean done = latch.await(10, TimeUnit.SECONDS);
        log("Messages confirmed? %s", done ? "yes" : "no");
      }
    }
  }

  public static class PublishDedupSecondDay {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        Producer producer =
            environment.producerBuilder().stream("deduplication-stream").name("app-1").build();
        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        List<String> messages = messages(messageCount);
        log("Publishing %d message with deduplication enabled.");
        for (int i = 0; i < messageCount; i++) {
          String payload = messages.get(i);
          Message message =
              producer
                  .messageBuilder()
                  .publishingId(i)
                  .addData(payload.getBytes(StandardCharsets.UTF_8))
                  .build();
          producer.send(message, confirmationStatus -> latch.countDown());
        }
        boolean done = latch.await(10, TimeUnit.SECONDS);
        log("Messages confirmed? %s", done ? "yes" : "no");
      }
    }
  }

  public static class PublishSmartDedupSecondDay {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        Producer producer =
            environment.producerBuilder().stream("deduplication-stream").name("app-1").build();
        int messageCount = 20;
        List<String> messages = messages(messageCount);
        int start = (int) producer.getLastPublishingId() + 1;
        log("Starting publishing at %s", start);
        log("Publishing %d message with deduplication enabled.", messageCount - start);
        CountDownLatch latch = new CountDownLatch(messageCount - start);
        for (int i = start; i < messageCount; i++) {
          String payload = messages.get(i);
          Message message =
              producer
                  .messageBuilder()
                  .publishingId(i)
                  .addData(payload.getBytes(StandardCharsets.UTF_8))
                  .build();
          producer.send(message, confirmationStatus -> latch.countDown());
        }
        boolean done = latch.await(10, TimeUnit.SECONDS);
        log("Messages confirmed? %s", done ? "yes" : "no");
      }
    }
  }

  public static class Consume {

    public static void main(String[] args) throws Exception {
      log("Connecting...");
      try (Environment environment = Environment.builder().build()) {
        log("Connected.");
        log("Starting consuming, press Enter to exit...");
        environment.consumerBuilder().stream("deduplication-stream")
            .offset(OffsetSpecification.first())
            .messageHandler(
                (context, message) ->
                    System.out.println(
                        new String(message.getBodyAsBinary(), StandardCharsets.UTF_8)))
            .build();
        System.in.read();
      }
    }
  }
}
