package com.rabbitmq.stream;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Deduplication {

  static void log(String format, Object... arguments) {
    System.out.println(String.format(format, arguments));
  }

  static Stream<Record> records(long start, long end) {
    return IntStream.range((int) start, (int) end).mapToObj(i -> new Record(i, "message " + i));
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
        records(0, messageCount)
            .forEach(
                record -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .addData(record.content().getBytes(StandardCharsets.UTF_8))
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
        records(0, messageCount)
            .forEach(
                record -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .addData(record.content().getBytes(StandardCharsets.UTF_8))
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
            environment.producerBuilder().stream("deduplication-stream")
                .name("app-1")
                .confirmTimeout(Duration.ZERO)
                .build();
        int messageCount = 10;
        CountDownLatch latch = new CountDownLatch(messageCount);
        log("Publishing %d messages with deduplication enabled.", messageCount);
        records(0, messageCount)
            .forEach(
                record -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .publishingId(record.id())
                          .addData(record.content().getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> latch.countDown());
                });
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
            environment.producerBuilder().stream("deduplication-stream")
                .name("app-1")
                .confirmTimeout(Duration.ZERO)
                .build();
        int messageCount = 20;
        CountDownLatch latch = new CountDownLatch(messageCount);
        log("Publishing %d messages with deduplication enabled.", messageCount);
        records(0, messageCount)
            .forEach(
                record -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .publishingId(record.id())
                          .addData(record.content().getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> latch.countDown());
                });
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
            environment.producerBuilder().stream("deduplication-stream")
                .name("app-1")
                .confirmTimeout(Duration.ZERO)
                .build();
        int messageCount = 20;
        long start = producer.getLastPublishingId() + 1;
        log("Starting publishing at %s", start);
        log("Publishing %d message with deduplication enabled.", messageCount - start);
        CountDownLatch latch = new CountDownLatch(messageCount - (int) start);
        records(start, messageCount)
            .forEach(
                record -> {
                  Message message =
                      producer
                          .messageBuilder()
                          .publishingId(record.id())
                          .addData(record.content().getBytes(StandardCharsets.UTF_8))
                          .build();
                  producer.send(message, confirmationStatus -> latch.countDown());
                });
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

  static class Record {

    private final long id;
    private final String content;

    Record(long id, String content) {
      this.id = id;
      this.content = content;
    }

    public long id() {
      return id;
    }

    public String content() {
      return content;
    }
  }
}
