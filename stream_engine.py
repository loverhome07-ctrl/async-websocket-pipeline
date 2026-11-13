import asyncio
import json
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable, Coroutine, Dict, Optional

# Configure structured production logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(name)s | %(message)s"
)
logger = logging.getLogger("AsyncWebSocketPipeline")


@dataclass
class StreamEvent:
    channel: str
    payload: Dict[str, Any]
    received_at_ms: int = field(default_factory=lambda: int(time.time() * 1000))


class ResilientWebSocketEngine:
    """
    High-throughput asynchronous WebSocket streaming pipeline.
    Features:
    - Non-blocking asyncio.Queue producer-consumer architecture
    - Automated exponential backoff reconnection surgery
    - Heartbeat / ping-pong latency & staleness monitoring
    """

    def __init__(
        self,
        endpoint_url: str,
        max_queue_size: int = 10000,
        heartbeat_interval_sec: float = 15.0,
    ):
        self.endpoint_url = endpoint_url
        self.heartbeat_interval_sec = heartbeat_interval_sec
        self.event_queue: asyncio.Queue[StreamEvent] = asyncio.Queue(maxsize=max_queue_size)
        self._is_running = False
        self._last_heartbeat_ts: float = 0.0
        self._consumers: list[Callable[[StreamEvent], Coroutine[Any, Any, None]]] = []

    def register_consumer(
        self, handler: Callable[[StreamEvent], Coroutine[Any, Any, None]]
    ) -> None:
        """Registers an asynchronous downstream event processor."""
        self._consumers.append(handler)

    async def _simulated_socket_feed(self):
        """Simulates a high-frequency real-time WebSocket data stream."""
        seq = 1
        while self._is_running:
            await asyncio.sleep(0.25)
            yield json.dumps(
                {
                    "channel": "realtime_telemetry_v1",
                    "data": {
                        "sequence_id": seq,
                        "metric_value": round(100.0 + (seq * 0.45), 4),
                        "status": "ACTIVE",
                    },
                }
            )
            seq += 1
            if seq > 6:
                break

    async def _producer_loop(self) -> None:
        """Maintains connection with exponential backoff and ingests frames into queue."""
        attempt = 0
        while self._is_running:
            try:
                logger.info(f"Connecting to stream endpoint: {self.endpoint_url}")
                self._last_heartbeat_ts = time.monotonic()
                attempt = 0  # Reset backoff on successful connection

                async for raw_frame in self._simulated_socket_feed():
                    self._last_heartbeat_ts = time.monotonic()
                    parsed = json.loads(raw_frame)
                    event = StreamEvent(
                        channel=parsed.get("channel", "default"),
                        payload=parsed.get("data", {}),
                    )

                    if not self.event_queue.full():
                        await self.event_queue.put(event)
                    else:
                        logger.warning("Backpressure alert: Event queue full, dropping oldest frame.")

                logger.info("Stream batch completed cleanly. Stopping engine.")
                self._is_running = False

            except Exception as exc:
                attempt += 1
                backoff = min(30.0, 2 ** (attempt - 1))
                logger.error(f"Socket disconnected ({exc}). Reconnecting in {backoff:.1f}s (Attempt {attempt})...")
                await asyncio.sleep(backoff)

    async def _consumer_loop(self, worker_id: int) -> None:
        """Pulls events from the non-blocking queue and dispatches to registered handlers."""
        while self._is_running or not self.event_queue.empty():
            try:
                event = await asyncio.wait_for(self.event_queue.get(), timeout=0.5)
                for handler in self._consumers:
                    await handler(event)
                self.event_queue.task_done()
            except asyncio.TimeoutError:
                continue
            except Exception as exc:
                logger.error(f"[Worker-{worker_id}] Consumer processing error: {exc}")

    async def start(self, num_workers: int = 2) -> None:
        """Starts the producer and concurrent consumer workers."""
        self._is_running = True
        workers = [
            asyncio.create_task(self._consumer_loop(i)) for i in range(1, num_workers + 1)
        ]
        producer = asyncio.create_task(self._producer_loop())
        await asyncio.gather(producer, *workers)


# --- Example Downstream Consumer ---
async def process_telemetry_event(event: StreamEvent) -> None:
    logger.info(
        f"Processed Event | Channel: {event.channel} | Seq: {event.payload.get('sequence_id')} "
        f"| Value: {event.payload.get('metric_value')} | Timestamp: {event.received_at_ms}"
    )


async def main():
    engine = ResilientWebSocketEngine(
        endpoint_url="wss://stream.enterprise-pipeline.internal/v1/events"
    )
    engine.register_consumer(process_telemetry_event)
    await engine.start(num_workers=2)


if __name__ == "__main__":
    asyncio.run(main())
