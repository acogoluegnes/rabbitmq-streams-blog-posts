package com.rabbitmq.stream;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;

public class Utils {

  public static Duration waitUntil(BooleanSupplier condition) throws InterruptedException {
    return waitAtMost(10, condition, null);
  }

  public static Duration waitAtMost(int timeoutInSeconds, BooleanSupplier condition)
      throws InterruptedException {
    return waitAtMost(timeoutInSeconds, condition, null);
  }

  static Duration waitAtMost(
      int timeoutInSeconds, BooleanSupplier condition, Supplier<String> message)
      throws InterruptedException {
    if (condition.getAsBoolean()) {
      return Duration.ZERO;
    }
    int waitTime = 100;
    int waitedTime = 0;
    int timeoutInMs = timeoutInSeconds * 1000;
    while (waitedTime <= timeoutInMs) {
      Thread.sleep(waitTime);
      waitedTime += waitTime;
      if (condition.getAsBoolean()) {
        return Duration.ofMillis(waitedTime);
      }
    }
    if (message == null) {
      throw new IllegalStateException(
          "Waited " + timeoutInSeconds + " second(s), condition never got true");
    } else {
      throw new IllegalStateException(
          "Waited " + timeoutInSeconds + " second(s), " + message.get());
    }
  }
}
