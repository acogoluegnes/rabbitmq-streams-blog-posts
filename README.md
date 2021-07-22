# RabbitMQ Streams Blog Posts Code Samples

Code for RabbitMQ Streams Blog Posts.

## Pre-requisites

* JDK 8 or more
* Local instance of RabbitMQ 3.9+ with stream plugin enabled.

To start RabbitMQ:

```shell
docker run -it --rm --name rabbitmq -p 5552:5552 \
    -e RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost -rabbit loopback_users "none"' \
    rabbitmq:3.9-rc
```

In another terminal tab, enable the stream plugin:

```shell
docker exec rabbitmq rabbitmq-plugins enable rabbitmq_stream
```

## First Application

[Blog Post](https://blog.rabbitmq.com/posts/2021/07/rabbitmq-streams-first-application/)

Publishing:

```
./mvnw -q compile exec:java -Dexec.mainClass='com.rabbitmq.stream.FirstApplication$Publish'
```

Consuming:

```
./mvnw -q compile exec:java -Dexec.mainClass='com.rabbitmq.stream.FirstApplication$Consume'
```

NB: remove the `-q` option if nothing is output on the console, this will help to diagnose problems.