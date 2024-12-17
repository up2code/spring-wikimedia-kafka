# Kafka with Spring Boot Webflux

This project to demonstrate how to integrate with Kafka in Spring Boot.

## Overview

```mermaid
sequenceDiagram
    Wikimedia->>Spring Boot: Event
    Spring Boot->>Kafka: Produce
    Kafka->>Spring Boot: Consume
    Spring Boot->>OpenSearch: Push
```