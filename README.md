# go-messaging-lib

## Overview

This project provides simple interfaces to interact with Veritone's core eventing system. There are two main patterns currently supported by this library:

* Pub-Sub
* Streaming

Depending on the underlying eventing system, user should use the corresponding package to initialize the its client. The supported systems are Kafka and NSQ.

## Goals

* Offers simple setup and just work out of the box.
* Provides consistent interfaces that should work for various eventing systems (Kafka, RabbitMQ, NSQ, etc.), thus preventing major breaking changes.
* Provides multiple examples to jump start.
* Handles edge cases and difficult technical requirements behind the scene.
* Exposes monitoring statistics with prometheus.

## Usage

Please see the [instructions](example/README.md)

## Notes

This repo is still a WIP. It's not yet suitable for production use.
