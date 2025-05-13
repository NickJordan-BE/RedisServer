# Redis Clone in Go

A high-performance, in-memory key-value store built from scratch in Go, replicating core Redis functionality including support for multiple data types, persistence, transactions, expiration, and stream processing.

## Overview

This project demonstrates systems-level engineering, network protocol design, and concurrent programming in Go. The server supports multiple concurrent client connections over TCP, interprets RESP (Redis Serialization Protocol), and emulates Redis-like functionality with performance and scalability in mind.

## Key Features

- Supports over 20 core Redis commands including SET, GET, DEL, HSET, HGET, EXPIRE, and more
- Implements Append Only File (AOF) and RDB-style snapshot backups with configurable durability settings
- Built-in support for Redis Streams (`XADD`, `XREAD`, and basic consumer groups)
- Key expiration system managed with goroutines and timers
- Basic transactional support (MULTI/EXEC emulation)
- RESP2 protocol-compliant parser and encoder
- Thread-safe in-memory store with fine-grained locking
- Containerized with Docker and deployable to Kubernetes via Minikube

## Technologies

- Go (Golang)
- TCP Sockets
- Custom RESP encoder/decoder (RESP2, RESP3 in progress)
- File I/O for persistence (AOF and RDB)
- Docker, Docker Compose, Kubernetes (Minikube)
