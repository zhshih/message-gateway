# Message Gateway Service

## A Gateway Service for Bridging Communication between Inbound and Outbound Message Service

This simple services consists of the following features:

- **...**: ...

This service aims to bridge the inbound and outbound communication between two different message services, primarily in Edge and Cloud environments. 

## Features
- ...

## Usage

* Compile Service

```bash
cargo build --release
```

* Launch service:

```bash
RUST_LOG=info ./target/release/message_gateway_service 
```

## Overview

```mermaid
graph LR;
    cloud{{MQTTBroker}} --> MQTTClient
    MQTTClient --> RedisClient
    RedisClient --> Edge
    Edge --> RedisClient
    RedisClient --> MQTTClient
    MQTTClient --> cloud{{MQTTBroker}}
```

