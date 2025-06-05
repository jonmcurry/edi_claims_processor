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
        
        # Record the timing
        self.monitor.record_timing(self.operation_name, duration, self.labels)
        
        # Record success/failure
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
        
        # Thread-safe storage for metrics
        self._metrics_lock = threading.Lock()
        self._timings = defaultdict(lambda: deque(maxlen=1000))  # Store last 1000 measurements
        self._counters = defaultdict(int)
        self._gauges = defaultdict(float)
        self._throughput_data = defaultdict(lambda: deque(maxlen=100))  # Store last 100 throughput measurements
        
        # Performance tracking
        self._operation_stats = defaultdict(lambda: {
            'total_count': 0,
            'total_duration': 0.0,
            'min_duration': float('inf'),
            'max_duration': 0.0,
            'error_count': 0
        })
        
        # System monitoring
        self._system_monitor_enabled = self.monitoring_config.get('system_monitoring_enabled', True)
        self._system_monitor_interval = self.monitoring_config.get('system_monitor_interval_seconds', 60)
        self._system_monitor_thread = None
        
        # Metrics collector integration
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
                    # CPU metrics
                    cpu_percent = psutil.cpu_percent(interval=1)
                    self.record_gauge('system_cpu_percent', cpu_percent)
                    
                    # Memory metrics
                    memory = psutil.virtual_memory()
                    self.record_gauge('system_memory_percent', memory.percent)
                    self.record_gauge('system_memory_available_gb', memory.available / (1024**3))
                    
                    # Disk I/O metrics
                    disk_io = psutil.disk_io_counters()
                    if disk_io:
                        self.record_gauge('system_disk_read_bytes_per_sec', disk_io.read_bytes)
                        self.record_gauge('system_disk_write_bytes_per_sec', disk_io.write_bytes)
                    
                    # Network I/O metrics
                    net_io = psutil.net_io_counters()
                    if net_io:
                        self.record_gauge('system_network_bytes_sent_per_sec', net_io.bytes_sent)
                        self.record_gauge('system_network_bytes_recv_per_sec', net_io.bytes_recv)
                    
                    # Process-specific metrics
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
            # Store raw timing data
            timing_data = {
                'duration': duration_seconds,
                'timestamp': datetime.utcnow(),
                'labels': labels.copy()
            }
            self._timings[operation_name].append(timing_data)
            
            # Update operation statistics
            stats = self._operation_stats[operation_name]
            stats['total_count'] += 1
            stats['total_duration'] += duration_seconds
            stats['min_duration'] = min(stats['min_duration'], duration_seconds)
            stats['max_duration'] = max(stats['max_duration'], duration_seconds)
        
        # Send to metrics collector
        self.metrics_collector.record_timing(operation_name, duration_seconds, labels)
        
        logger.debug(f"Recorded timing: {operation_name} = {duration_seconds:.4f}s")
    
    def record_counter(self, counter_name: str, value: int = 1, labels: Dict[str, str] = None):
        """Record a counter increment."""
        labels = labels or {}
        
        with self._metrics_lock:
            self._counters[counter_name] += value
            
            # Track errors separately
            if 'error' in counter_name:
                operation = counter_name.replace('_error', '').replace('_success', '')
                if operation in self._operation_stats:
                    self._operation_stats[operation]['error_count'] += value
        
        # Send to metrics collector
        self.metrics_collector.record_counter(counter_name, value, labels)
        
        logger.debug(f"Recorded counter: {counter_name} += {value}")
    
    def record_gauge(self, gauge_name: str, value: float, labels: Dict[str, str] = None):
        """Record a gauge measurement."""
        labels = labels or {}
        
        with self._metrics_lock:
            self._gauges[gauge_name] = value
        
        # Send to metrics collector
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
            self._throughput_data