# app/monitoring/performance_monitor.py
"""
Performance monitoring system for the EDI Claims Processor.
Tracks processing throughput, response times, and system performance metrics.
"""
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from contextlib import contextmanager
import psutil

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.metrics import MetricsCollector

logger = get_logger('app.monitoring.performance_monitor')

@dataclass
class PerformanceMetric:
    """Represents a single performance measurement."""
    name: str
    value: float
    unit: str
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: Dict[str, str] = field(default_factory=dict)
    
@dataclass
class ThroughputMetric:
    """Tracks throughput over time."""
    operation: str
    count: int
    duration_seconds: float
    rate_per_second: float
    timestamp: datetime = field(default_factory=datetime.utcnow)

class PerformanceTimer:
    """Context manager for timing operations."""
    
    def __init__(self, monitor: 'PerformanceMonitor', operation_name: str, labels: Dict[str, str] = None):
        self.monitor = monitor
        self.operation_name = operation_name
        self.labels = labels or {}
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.perf_counter()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.perf_counter()
        duration = self.end_time - self.start_time
        
        self.monitor.record_timing(self.operation_name, duration, self.labels)
        
        if exc_type is None:
            self.monitor.record_counter(f"{self.operation_name}_success", 1, self.labels)
        else:
            error_labels = self.labels.copy()
            error_labels['error_type'] = exc_type.__name__
            self.monitor.record_counter(f"{self.operation_name}_error", 1, error_labels)

class PerformanceMonitor:
    """
    Comprehensive performance monitoring system.
    Tracks timing, throughput, errors, and system resources.
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.monitoring_config = self.config.get('monitoring', {})
        
        self._metrics_lock = threading.RLock()
        self._timings = defaultdict(lambda: deque(maxlen=1000))
        self._counters = defaultdict(int)
        self._gauges = defaultdict(float)
        self._throughput_data = defaultdict(lambda: deque(maxlen=100))
        
        self._operation_stats = defaultdict(lambda: {
            'total_count': 0,
            'total_duration': 0.0,
            'min_duration': float('inf'),
            'max_duration': 0.0,
            'error_count': 0
        })
        
        self._system_monitor_enabled = self.monitoring_config.get('system_monitoring_enabled', True)
        self._system_monitor_interval = self.monitoring_config.get('system_monitor_interval_seconds', 60)
        self._system_monitor_thread = None
        
        self.metrics_collector = MetricsCollector()
        
        logger.info("PerformanceMonitor initialized")
        
        if self._system_monitor_enabled:
            self.start_system_monitoring()
    
    def start_system_monitoring(self):
        """Start background system resource monitoring."""
        if self._system_monitor_thread and self._system_monitor_thread.is_alive():
            return
        
        def monitor_system():
            while self._system_monitor_enabled:
                try:
                    cpu_percent = psutil.cpu_percent(interval=1)
                    self.record_gauge('system_cpu_percent', cpu_percent)
                    
                    memory = psutil.virtual_memory()
                    self.record_gauge('system_memory_percent', memory.percent)
                    self.record_gauge('system_memory_available_gb', memory.available / (1024**3))
                    
                    disk_io = psutil.disk_io_counters()
                    if disk_io:
                        self.record_gauge('system_disk_read_bytes_per_sec', disk_io.read_bytes)
                        self.record_gauge('system_disk_write_bytes_per_sec', disk_io.write_bytes)
                    
                    net_io = psutil.net_io_counters()
                    if net_io:
                        self.record_gauge('system_network_bytes_sent_per_sec', net_io.bytes_sent)
                        self.record_gauge('system_network_bytes_recv_per_sec', net_io.bytes_recv)
                    
                    process = psutil.Process()
                    self.record_gauge('process_memory_mb', process.memory_info().rss / (1024**2))
                    self.record_gauge('process_cpu_percent', process.cpu_percent())
                    self.record_gauge('process_num_threads', process.num_threads())
                    
                    time.sleep(self._system_monitor_interval)
                    
                except Exception as e:
                    logger.error(f"Error in system monitoring: {e}", exc_info=True)
                    time.sleep(self._system_monitor_interval)
        
        self._system_monitor_thread = threading.Thread(target=monitor_system, daemon=True)
        self._system_monitor_thread.start()
        logger.info(f"System monitoring started (interval: {self._system_monitor_interval}s)")
    
    def stop_system_monitoring(self):
        """Stop background system resource monitoring."""
        self._system_monitor_enabled = False
        if self._system_monitor_thread:
            self._system_monitor_thread.join(timeout=5)
        logger.info("System monitoring stopped")
    
    @contextmanager
    def timer(self, operation_name: str, labels: Dict[str, str] = None):
        """Context manager for timing operations."""
        timer = PerformanceTimer(self, operation_name, labels)
        with timer:
            yield timer
    
    def record_timing(self, operation_name: str, duration_seconds: float, labels: Dict[str, str] = None):
        """Record a timing measurement."""
        labels = labels or {}
        
        with self._metrics_lock:
            timing_data = {
                'duration': duration_seconds,
                'timestamp': datetime.utcnow(),
                'labels': labels.copy()
            }
            self._timings[operation_name].append(timing_data)
            
            stats = self._operation_stats[operation_name]
            stats['total_count'] += 1
            stats['total_duration'] += duration_seconds
            stats['min_duration'] = min(stats['min_duration'], duration_seconds)
            stats['max_duration'] = max(stats['max_duration'], duration_seconds)
        
        self.metrics_collector.record_timing(operation_name, duration_seconds, labels)
        
        logger.debug(f"Recorded timing: {operation_name} = {duration_seconds:.4f}s")
    
    def record_counter(self, counter_name: str, value: int = 1, labels: Dict[str, str] = None):
        """Record a counter increment."""
        labels = labels or {}
        
        with self._metrics_lock:
            self._counters[counter_name] += value
            
            if 'error' in counter_name:
                operation = counter_name.replace('_error', '').replace('_success', '')
                if operation in self._operation_stats:
                    self._operation_stats[operation]['error_count'] += value
        
        self.metrics_collector.record_counter(counter_name, value, labels)
        
        logger.debug(f"Recorded counter: {counter_name} += {value}")
    
    def record_gauge(self, gauge_name: str, value: float, labels: Dict[str, str] = None):
        """Record a gauge measurement."""
        labels = labels or {}
        
        with self._metrics_lock:
            self._gauges[gauge_name] = value
        
        self.metrics_collector.record_gauge(gauge_name, value, labels)
        
        logger.debug(f"Recorded gauge: {gauge_name} = {value}")
    
    def record_throughput(self, operation: str, count: int, duration_seconds: float, labels: Dict[str, str] = None):
        """Record throughput measurement."""
        labels = labels or {}
        rate_per_second = count / duration_seconds if duration_seconds > 0 else 0
        
        throughput_metric = ThroughputMetric(
            operation=operation,
            count=count,
            duration_seconds=duration_seconds,
            rate_per_second=rate_per_second
        )
        
        with self._metrics_lock:
            self._throughput_data[operation].append(throughput_metric)

    def get_operation_statistics(self, operation_name: Optional[str] = None) -> Dict[str, Any]:
        """Get statistics for a specific operation or all operations."""
        with self._metrics_lock:
            if operation_name:
                return self._operation_stats.get(operation_name, {})
            return dict(self._operation_stats)

    def get_system_metrics(self) -> Dict[str, float]:
        """Get latest system metrics."""
        with self._metrics_lock:
            return {
                'system_cpu_percent': self._gauges.get('system_cpu_percent', 0.0),
                'system_memory_percent': self._gauges.get('system_memory_percent', 0.0),
                'process_memory_mb': self._gauges.get('process_memory_mb', 0.0)
            }
            
    def get_throughput_metrics(self, operation: Optional[str] = None, window_seconds: int = 60) -> Dict[str, Any]:
        """Calculate throughput for specific operations."""
        throughput_report = {}
        with self._metrics_lock:
            operations_to_check = [operation] if operation else list(self._throughput_data.keys())
            
            for op_name in operations_to_check:
                recent_data = [
                    d for d in self._throughput_data[op_name]
                    if d.timestamp > datetime.utcnow() - timedelta(seconds=window_seconds)
                ]
                
                if not recent_data:
                    throughput_report[op_name] = {'count': 0, 'duration': 0, 'avg_rate': 0.0}
                    continue
                
                total_count = sum(d.count for d in recent_data)
                total_duration = sum(d.duration_seconds for d in recent_data)
                avg_rate = total_count / total_duration if total_duration > 0 else 0
                
                throughput_report[op_name] = {
                    'count': total_count,
                    'duration': total_duration,
                    'avg_rate': avg_rate
                }
        return throughput_report
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate a comprehensive performance report dictionary."""
        operations_stats = self.get_operation_statistics()
        for op_name, stats in operations_stats.items():
            if stats['total_count'] > 0:
                stats['error_rate'] = stats['error_count'] / stats['total_count']
                stats['avg_duration_ms'] = (stats['total_duration'] / stats['total_count']) * 1000
            else:
                stats['error_rate'] = 0.0
                stats['avg_duration_ms'] = 0.0

        return {
            "operations": operations_stats,
            "system_metrics": self.get_system_metrics(),
            "throughput": self.get_throughput_metrics(),
            "timestamp": datetime.utcnow().isoformat()
        }
