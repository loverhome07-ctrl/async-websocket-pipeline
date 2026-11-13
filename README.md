# Async WebSocket Pipeline ⚡

A high-throughput, resilient asynchronous WebSocket streaming engine in Python built for real-time telemetry, financial/IoT feeds, and event-driven architectures.

## 🚀 Key Architectural Features

* **Producer-Consumer Decoupling (`asyncio.Queue`):** Non-blocking frame ingestion with configurable backpressure handling and concurrent worker pools.
* **Automated Fault Recovery:** Exponential backoff reconnection surgery to handle transient socket drops without data pipeline crashes.
* **Heartbeat & Latency Telemetry:** Built-in monotonic timestamp tracking for stream health monitoring.

## 🛠️ Quick Start

```bash
python stream_engine.py
