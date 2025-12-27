import time
import functools
import psutil
import os
from typing import Dict, Any, Optional

class PerformanceMonitor:
    def __init__(self):
        self.metrics = {}

    def reset_metrics(self):
        self.metrics = {}

    def measure_time(self, operation_name: str):
        class TimerContext:
            def __init__(self, monitor, name):
                self.monitor = monitor
                self.name = name
                self.start_time = 0
                self.start_memory = 0

            def __enter__(self):
                self.start_time = time.perf_counter()
                process = psutil.Process(os.getpid())
                self.start_memory = process.memory_info().rss
                return self

            def __exit__(self, exc_type, exc_val, exc_tb):
                end_time = time.perf_counter()
                process = psutil.Process(os.getpid())
                end_memory = process.memory_info().rss
                
                duration = end_time - self.start_time
                memory_diff = end_memory - self.start_memory
                
                self.monitor.record_metric(self.name, "duration_seconds", duration)
                self.monitor.record_metric(self.name, "memory_diff_bytes", memory_diff)
                self.monitor.record_metric(self.name, "peak_memory_bytes", end_memory)

        return TimerContext(self, operation_name)

    def record_metric(self, operation_name: str, metric_type: str, value: Any):
        if operation_name not in self.metrics:
            self.metrics[operation_name] = {}
        self.metrics[operation_name][metric_type] = value

    def get_report(self) -> Dict[str, Any]:
        return self.metrics

    def get_metrics(self) -> Dict[str, Any]:
        return {"operations": self.metrics}

    def print_report(self):
        print("\n=== Performance Report ===")
        for op, metrics in self.metrics.items():
            print(f"Operation: {op}")
            for k, v in metrics.items():
                print(f"  {k}: {v}")
        print("==========================\n")
