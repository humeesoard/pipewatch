"""Core metrics data structures and collection for pipeline health monitoring."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional


@dataclass
class PipelineMetric:
    """Represents a single pipeline health metric snapshot."""

    pipeline_name: str
    records_processed: int
    records_failed: int
    throughput_per_sec: float
    latency_ms: float
    timestamp: datetime = field(default_factory=datetime.utcnow)
    error_message: Optional[str] = None

    @property
    def error_rate(self) -> float:
        """Calculate the error rate as a percentage."""
        total = self.records_processed + self.records_failed
        if total == 0:
            return 0.0
        return (self.records_failed / total) * 100.0

    @property
    def is_healthy(self) -> bool:
        """Determine if the pipeline is considered healthy."""
        return self.error_rate < 5.0 and self.latency_ms < 1000.0

    def to_dict(self) -> dict:
        """Serialize metric to a plain dictionary."""
        return {
            "pipeline_name": self.pipeline_name,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "throughput_per_sec": self.throughput_per_sec,
            "latency_ms": self.latency_ms,
            "error_rate": round(self.error_rate, 2),
            "is_healthy": self.is_healthy,
            "timestamp": self.timestamp.isoformat(),
            "error_message": self.error_message,
        }


def collect_metric(
    pipeline_name: str,
    records_processed: int,
    records_failed: int,
    throughput_per_sec: float,
    latency_ms: float,
    error_message: Optional[str] = None,
) -> PipelineMetric:
    """Factory function to create and return a new PipelineMetric."""
    return PipelineMetric(
        pipeline_name=pipeline_name,
        records_processed=records_processed,
        records_failed=records_failed,
        throughput_per_sec=throughput_per_sec,
        latency_ms=latency_ms,
        error_message=error_message,
    )
