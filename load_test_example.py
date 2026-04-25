#!/usr/bin/env python3
"""
Load test script for the event ingestion API.

Simulates realistic production traffic: ~20 events/sec on average, but with
variable spikes (5-50 events/sec) to test behavior under bursty load.

Usage:
    python load_test_example.py http://localhost:5000

The script runs for approximately 5 minutes and reports:
    - Total events sent
    - Success/error rate
    - Response latencies (min, max, p50, p95, p99)
"""

from __future__ import annotations

import argparse
import json
import random
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional

import requests


@dataclass
class LoadTestConfig:
    """Configuration for the load test."""
    endpoint: str
    duration_seconds: int = 300  # 5 minutes
    target_rps: float = 20.0  # 20 events per second on average
    max_burst_rps: float = 50.0
    min_rps: float = 5.0
    batch_probability: float = 0.3  # 30% of requests use /batch endpoint


@dataclass
class LoadTestResult:
    """Results from a single request."""
    success: bool
    status_code: Optional[int]
    latency_ms: float
    error_message: Optional[str] = None


class EventGenerator:
    """Generates realistic random events."""

    STORES = [f"store_{i}" for i in range(1, 11)]  # 10 sample stores
    EVENT_TYPES = ["page_view", "add_to_cart", "checkout_start", "checkout_success"]
    PRODUCTS = [f"prod_{i}" for i in range(1, 51)]  # 50 sample products

    @staticmethod
    def random_session_id() -> str:
        """Generate a random session ID."""
        return f"sess_{random.randint(100000, 999999)}"

    @staticmethod
    def random_ip() -> str:
        """Generate a random IP address."""
        return ".".join(str(random.randint(1, 255)) for _ in range(4))

    @classmethod
    def generate_event(cls) -> dict:
        """Generate a single random event."""
        event_type = random.choice(cls.EVENT_TYPES)

        # Determine event_object_id based on event type
        if event_type == "page_view":
            event_object_id = random.choice(["/", "/products", "/cart", "/checkout"])
        elif event_type == "add_to_cart":
            event_object_id = random.choice(cls.PRODUCTS)
        else:  # checkout_start or checkout_success
            event_object_id = f"checkout_{random.randint(10000, 99999)}"

        return {
            "store_id": random.choice(cls.STORES),
            "event_type": event_type,
            "session_id": cls.random_session_id(),
            "timestamp": datetime.utcnow().isoformat() + "Z",
            "user_ip": cls.random_ip(),
            "event_object_id": event_object_id,
        }

    @classmethod
    def generate_batch(cls, size: int) -> list[dict]:
        """Generate a batch of random events."""
        return [cls.generate_event() for _ in range(size)]


class LoadTester:
    """Runs the load test against the API."""

    def __init__(self, config: LoadTestConfig):
        self.config = config
        self.results: list[LoadTestResult] = []
        self.start_time: float = 0
        self.end_time: float = 0

    def run(self) -> None:
        """Run the load test."""
        print(f"Starting load test against {self.config.endpoint}")
        print(f"Duration: {self.config.duration_seconds}s")
        print(f"Target RPS: {self.config.target_rps} (avg), {self.config.min_rps}-{self.config.max_burst_rps} (range)")
        print()

        self.start_time = time.time()
        request_count = 0
        last_rate_change = self.start_time

        while time.time() - self.start_time < self.config.duration_seconds:
            # Every ~10 seconds, pick a new random request rate (burst pattern)
            now = time.time()
            if now - last_rate_change > 10 or last_rate_change == self.start_time:
                current_rps = random.uniform(self.config.min_rps, self.config.max_burst_rps)
                last_rate_change = now
                print(f"[{self._elapsed():.1f}s] Switching to {current_rps:.1f} events/sec")

            # Sleep to maintain target RPS
            sleep_time = 1.0 / current_rps
            time.sleep(sleep_time)

            # Send request
            if random.random() < self.config.batch_probability:
                # Batch request (1-10 events)
                batch_size = random.randint(1, 10)
                self._send_batch(batch_size)
            else:
                # Single event
                self._send_event()

            request_count += 1

            # Print progress every 50 requests
            if request_count % 50 == 0:
                self._print_progress()

        self.end_time = time.time()
        self._print_results()

    def _send_event(self) -> None:
        """Send a single event."""
        event = EventGenerator.generate_event()
        start = time.time()
        try:
            response = requests.post(
                f"{self.config.endpoint}/events",
                json=event,
                timeout=5,
            )
            latency_ms = (time.time() - start) * 1000
            self.results.append(LoadTestResult(
                success=response.status_code == 202,
                status_code=response.status_code,
                latency_ms=latency_ms,
            ))
        except requests.RequestException as e:
            latency_ms = (time.time() - start) * 1000
            self.results.append(LoadTestResult(
                success=False,
                status_code=None,
                latency_ms=latency_ms,
                error_message=str(e),
            ))

    def _send_batch(self, size: int) -> None:
        """Send a batch of events."""
        events = EventGenerator.generate_batch(size)
        start = time.time()
        try:
            response = requests.post(
                f"{self.config.endpoint}/events/batch",
                json=events,
                timeout=5,
            )
            latency_ms = (time.time() - start) * 1000
            self.results.append(LoadTestResult(
                success=response.status_code == 202,
                status_code=response.status_code,
                latency_ms=latency_ms,
            ))
        except requests.RequestException as e:
            latency_ms = (time.time() - start) * 1000
            self.results.append(LoadTestResult(
                success=False,
                status_code=None,
                latency_ms=latency_ms,
                error_message=str(e),
            ))

    def _elapsed(self) -> float:
        """Return elapsed time in seconds."""
        return time.time() - self.start_time

    def _print_progress(self) -> None:
        """Print current progress."""
        elapsed = self._elapsed()
        success_count = sum(1 for r in self.results if r.success)
        error_count = len(self.results) - success_count
        rps = len(self.results) / elapsed if elapsed > 0 else 0
        print(f"[{elapsed:.1f}s] {len(self.results)} requests, "
              f"{success_count} success, {error_count} errors, "
              f"{rps:.1f} actual RPS")

    def _print_results(self) -> None:
        """Print final results."""
        elapsed = self.end_time - self.start_time
        success_count = sum(1 for r in self.results if r.success)
        error_count = len(self.results) - success_count
        success_rate = (success_count / len(self.results) * 100) if self.results else 0

        latencies = [r.latency_ms for r in self.results]
        latencies.sort()

        print()
        print("=" * 60)
        print("LOAD TEST RESULTS")
        print("=" * 60)
        print(f"Duration:           {elapsed:.2f}s")
        print(f"Total requests:     {len(self.results)}")
        print(f"Successful:         {success_count} ({success_rate:.1f}%)")
        print(f"Failed:             {error_count}")
        print(f"Actual RPS:         {len(self.results) / elapsed:.2f}")
        print()
        print("Response latencies (ms):")
        print(f"  Min:              {min(latencies) if latencies else 0:.2f}")
        print(f"  Max:              {max(latencies) if latencies else 0:.2f}")
        print(f"  Avg:              {sum(latencies) / len(latencies) if latencies else 0:.2f}")
        print(f"  p50:              {self._percentile(latencies, 50):.2f}")
        print(f"  p95:              {self._percentile(latencies, 95):.2f}")
        print(f"  p99:              {self._percentile(latencies, 99):.2f}")
        print()

        if error_count > 0:
            print("Sample errors:")
            for result in [r for r in self.results if not r.success][:5]:
                print(f"  - {result.status_code}: {result.error_message}")

    @staticmethod
    def _percentile(data: list[float], percentile: int) -> float:
        """Calculate percentile of a list."""
        if not data:
            return 0
        index = int(len(data) * percentile / 100)
        return data[min(index, len(data) - 1)]


def main() -> None:
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Load test script for the event ingestion API.",
    )
    parser.add_argument(
        "endpoint",
        help="Base URL of the API (e.g., http://localhost:5000)",
    )
    parser.add_argument(
        "--duration",
        type=int,
        default=300,
        help="Duration of the test in seconds (default: 300 = 5 minutes)",
    )
    parser.add_argument(
        "--target-rps",
        type=float,
        default=20.0,
        help="Target average events per second (default: 20)",
    )

    args = parser.parse_args()

    config = LoadTestConfig(
        endpoint=args.endpoint,
        duration_seconds=args.duration,
        target_rps=args.target_rps,
    )

    tester = LoadTester(config)
    try:
        tester.run()
    except KeyboardInterrupt:
        print("\nTest interrupted by user.")
        tester._print_results()
        sys.exit(0)


if __name__ == "__main__":
    main()
