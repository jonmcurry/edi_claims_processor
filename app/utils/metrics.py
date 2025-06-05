# app/utils/metrics.py
"""
Application metrics collection system for the EDI Claims Processor.
Provides centralized metrics collection, aggregation, and export capabilities.
"""
import time
import threading
from collections import defaultdict, deque
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Union
from dataclasses import dataclass, field
from enum import Enum
import json
import os

from app.utils.logging_config import get_logger, get_correlation_id

logger = get_logger('app.utils.metrics')

class MetricType(Enum):
    COUNTER = "counter"
    GAUGE = "gauge"
    HISTOGRAM = "histogram"
    TIMER = "timer"

@dataclass
class MetricPoint:
    """Represents a single metric measurement."""
    name: str
    value: Union[int, float]
    metric_type: MetricType
    timestamp: datetime = field(default_factory=datetime.utcnow)
    labels: Dict[str, str] = field(default_factory=dict)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'value': self.value,
            'type': self.metric_type.value,
            'timestamp': self.timestamp.isoformat(),
            'labels': self.labels
        }

@dataclass
class AggregatedMetric:
    """Represents aggregated metric data over a time period."""
    name: str
    metric_type: MetricType
    count: int
    sum_value: float
    min_value: float
    max_value: float
    avg_value: float
    labels: Dict[str, str] = field(default_factory=dict)
    start_time: datetime = field(default_factory=datetime.utcnow)
    end_time: datetime = field(default_factory=datetime.utcnow)
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            'name': self.name,
            'type': self.metric_type.value,
            'count': self.count,
            'sum': self.sum_value,
            'min': self.min_value,
            'max': self.max_value,
            'avg': self.avg_value,
            'labels': self.labels,
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat()
        }

class MetricsCollector:
    """
    Centralized metrics collection system.
    Collects, aggregates, and exports application metrics.
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.metrics_config = self.config.get('metrics', {})
        
        # Thread-safe storage
        self._metrics_lock = threading.Lock()
        self._raw_metrics: deque = deque(maxlen=10000)  # Store last 10k metrics
        self._counters: Dict[str, float] = defaultdict(float)
        self._gauges: Dict[str, float] = {}
        self._histograms: Dict[str, List[float]] = defaultdict(list)
        self._timers: Dict[str, List[float]] = defaultdict(list)
        
        # Aggregation windows
        self._aggregation_windows = [
            timedelta(minutes=1),
            timedelta(minutes=5),
            timedelta(minutes=15),
            timedelta(hours=1),
            timedelta(hours=24)
        ]
        
        # Export settings
        self.export_enabled = self.metrics_config.get('export_enabled', True)
        self.export_interval = self.metrics_config.get('export_interval_seconds', 60)
        self.export_file_path = self.metrics_config.get('export_file_path', 'logs/metrics.jsonl')
        
        # Background aggregation and export
        self._background_thread = None
        self._stop_background = False
        
        # Business metrics tracking
        self._business_metrics = {
            'claims_processed_total': 0,
            'claims_failed_total': 0,
            'claims_pending': 0,
            'average_processing_time_seconds': 0,
            'throughput_claims_per_second': 0,
            'error_rate_percentage': 0,
            'system_health_score': 100
        }
        
        logger.info("MetricsCollector initialized")
        
        if self.metrics_config.get('auto_start_background', True):
            self.start_background_processing()
    
    def record_counter(self, name: str, value: Union[int, float] = 1, labels: Dict[str, str] = None):
        """Record a counter metric (monotonically increasing)."""
        labels = labels or {}
        
        with self._metrics_lock:
            metric_key = self._create_metric_key(name, labels)
            self._counters[metric_key] += value
            
            # Store raw metric
            metric_point = MetricPoint(
                name=name,
                value=value,
                metric_type=MetricType.COUNTER,
                labels=labels
            )
            self._raw_metrics.append(metric_point)
        
        logger.debug(f"Recorded counter: {name} += {value} {labels}")
    
    def record_gauge(self, name: str, value: Union[int, float], labels: Dict[str, str] = None):
        """Record a gauge metric (instantaneous value)."""
        labels = labels or {}
        
        with self._metrics_lock:
            metric_key = self._create_metric_key(name, labels)
            self._gauges[metric_key] = value
            
            # Store raw metric
            metric_point = MetricPoint(
                name=name,
                value=value,
                metric_type=MetricType.GAUGE,
                labels=labels
            )
            self._raw_metrics.append(metric_point)
        
        logger.debug(f"Recorded gauge: {name} = {value} {labels}")
    
    def record_histogram(self, name: str, value: Union[int, float], labels: Dict[str, str] = None):
        """Record a histogram metric (distribution of values)."""
        labels = labels or {}
        
        with self._metrics_lock:
            metric_key = self._create_metric_key(name, labels)
            self._histograms[metric_key].append(value)
            
            # Keep histogram size manageable
            if len(self._histograms[metric_key]) > 1000:
                self._histograms[metric_key] = self._histograms[metric_key][-500:]
            
            # Store raw metric
            metric_point = MetricPoint(
                name=name,
                value=value,
                metric_type=MetricType.HISTOGRAM,
                labels=labels
            )
            self._raw_metrics.append(metric_point)
        
        logger.debug(f"Recorded histogram: {name} = {value} {labels}")
    
    def record_timing(self, name: str, duration_seconds: float, labels: Dict[str, str] = None):
        """Record a timing metric (specialized histogram for durations)."""
        labels = labels or {}
        
        with self._metrics_lock:
            metric_key = self._create_metric_key(name, labels)
            self._timers[metric_key].append(duration_seconds)
            
            # Keep timer size manageable
            if len(self._timers[metric_key]) > 1000:
                self._timers[metric_key] = self._timers[metric_key][-500:]
            
            # Store raw metric
            metric_point = MetricPoint(
                name=name,
                value=duration_seconds,
                metric_type=MetricType.TIMER,
                labels=labels
            )
            self._raw_metrics.append(metric_point)
        
        logger.debug(f"Recorded timing: {name} = {duration_seconds:.4f}s {labels}")
    
    def _create_metric_key(self, name: str, labels: Dict[str, str]) -> str:
        """Create a unique key for metric storage."""
        if not labels:
            return name
        
        label_str = ','.join(f"{k}={v}" for k, v in sorted(labels.items()))
        return f"{name}{{{label_str}}}"
    
    def get_counter_value(self, name: str, labels: Dict[str, str] = None) -> float:
        """Get current counter value."""
        metric_key = self._create_metric_key(name, labels or {})
        with self._metrics_lock:
            return self._counters.get(metric_key, 0)
    
    def get_gauge_value(self, name: str, labels: Dict[str, str] = None) -> Optional[float]:
        """Get current gauge value."""
        metric_key = self._create_metric_key(name, labels or {})
        with self._metrics_lock:
            return self._gauges.get(metric_key)
    
    def get_histogram_stats(self, name: str, labels: Dict[str, str] = None) -> Dict[str, float]:
        """Get histogram statistics."""
        metric_key = self._create_metric_key(name, labels or {})
        with self._metrics_lock:
            values = self._histograms.get(metric_key, [])
            
            if not values:
                return {}
            
            sorted_values = sorted(values)
            count = len(sorted_values)
            
            return {
                'count': count,
                'sum': sum(sorted_values),
                'min': min(sorted_values),
                'max': max(sorted_values),
                'avg': sum(sorted_values) / count,
                'p50': sorted_values[int(count * 0.5)],
                'p90': sorted_values[int(count * 0.9)],
                'p95': sorted_values[int(count * 0.95)],
                'p99': sorted_values[int(count * 0.99)]
            }
    
    def get_timer_stats(self, name: str, labels: Dict[str, str] = None) -> Dict[str, float]:
        """Get timer statistics (same as histogram but with time-specific naming)."""
        stats = self.get_histogram_stats(name, labels)
        if stats:
            # Convert to milliseconds for readability
            time_stats = {}
            for key, value in stats.items():
                if key in ['count']:
                    time_stats[key] = value
                else:
                    time_stats[f"{key}_ms"] = value * 1000
            return time_stats
        return {}
    
    def calculate_business_metrics(self):
        """Calculate high-level business metrics."""
        with self._metrics_lock:
            # Claims processing metrics
            processed_total = self.get_counter_value('claims_processed_success')
            failed_total = self.get_counter_value('claims_processed_error')
            pending = self.get_gauge_value('claims_pending_count') or 0
            
            # Calculate rates over last hour
            one_hour_ago = datetime.utcnow() - timedelta(hours=1)
            recent_metrics = [m for m in self._raw_metrics if m.timestamp >= one_hour_ago]
            
            processed_last_hour = len([m for m in recent_metrics 
                                     if m.name == 'claims_processed_success'])
            failed_last_hour = len([m for m in recent_metrics 
                                  if m.name == 'claims_processed_error'])
            
            # Throughput (claims per second over last hour)
            throughput = processed_last_hour / 3600.0 if processed_last_hour > 0 else 0
            
            # Error rate
            total_attempts = processed_last_hour + failed_last_hour
            error_rate = (failed_last_hour / total_attempts * 100) if total_attempts > 0 else 0
            
            # Average processing time
            processing_times = [m.value for m in recent_metrics 
                              if m.name == 'claim_processing_duration_seconds']
            avg_processing_time = sum(processing_times) / len(processing_times) if processing_times else 0
            
            # System health score (simplified calculation)
            health_score = 100
            if error_rate > 10:
                health_score -= 30
            elif error_rate > 5:
                health_score -= 15
            
            if throughput < 1000:  # Below target throughput
                health_score -= 20
            
            if avg_processing_time > 10:  # Slow processing
                health_score -= 15
            
            health_score = max(0, health_score)
            
            # Update business metrics
            self._business_metrics.update({
                'claims_processed_total': processed_total,
                'claims_failed_total': failed_total,
                'claims_pending': pending,
                'average_processing_time_seconds': round(avg_processing_time, 3),
                'throughput_claims_per_second': round(throughput, 2),
                'error_rate_percentage': round(error_rate, 2),
                'system_health_score': round(health_score, 1)
            })
    
    def get_business_metrics(self) -> Dict[str, Any]:
        """Get current business metrics."""
        self.calculate_business_metrics()
        return self._business_metrics.copy()
    
    def aggregate_metrics(self, window: timedelta) -> List[AggregatedMetric]:
        """Aggregate metrics over a time window."""
        cutoff_time = datetime.utcnow() - window
        aggregated = []
        
        with self._metrics_lock:
            # Group metrics by name and labels
            metric_groups = defaultdict(list)
            
            for metric in self._raw_metrics:
                if metric.timestamp >= cutoff_time:
                    key = (metric.name, metric.metric_type, tuple(sorted(metric.labels.items())))
                    metric_groups[key].append(metric)
            
            # Calculate aggregations
            for (name, metric_type, label_items), metrics in metric_groups.items():
                if not metrics:
                    continue
                
                labels = dict(label_items)
                values = [m.value for m in metrics]
                
                if metric_type in [MetricType.COUNTER, MetricType.GAUGE]:
                    # For counters and gauges, use the latest value
                    latest_metric = max(metrics, key=lambda x: x.timestamp)
                    aggregated_metric = AggregatedMetric(
                        name=name,
                        metric_type=metric_type,
                        count=len(metrics),
                        sum_value=latest_metric.value,
                        min_value=latest_metric.value,
                        max_value=latest_metric.value,
                        avg_value=latest_metric.value,
                        labels=labels,
                        start_time=cutoff_time,
                        end_time=datetime.utcnow()
                    )
                else:
                    # For histograms and timers, calculate statistics
                    aggregated_metric = AggregatedMetric(
                        name=name,
                        metric_type=metric_type,
                        count=len(values),
                        sum_value=sum(values),
                        min_value=min(values),
                        max_value=max(values),
                        avg_value=sum(values) / len(values),
                        labels=labels,
                        start_time=cutoff_time,
                        end_time=datetime.utcnow()
                    )
                
                aggregated.append(aggregated_metric)
        
        return aggregated
    
    def export_metrics_to_file(self, file_path: str = None):
        """Export current metrics to a JSON Lines file."""
        if not self.export_enabled:
            return
        
        file_path = file_path or self.export_file_path
        
        try:
            # Ensure directory exists
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Aggregate metrics for different time windows
            export_data = {
                'timestamp': datetime.utcnow().isoformat(),
                'correlation_id': get_correlation_id(),
                'business_metrics': self.get_business_metrics(),
                'aggregated_metrics': {}
            }
            
            # Add aggregated metrics for each time window
            for window in self._aggregation_windows:
                window_name = self._format_timedelta(window)
                aggregated = self.aggregate_metrics(window)
                export_data['aggregated_metrics'][window_name] = [
                    metric.to_dict() for metric in aggregated
                ]
            
            # Append to JSON Lines file
            with open(file_path, 'a') as f:
                f.write(json.dumps(export_data) + '\n')
            
            logger.debug(f"Exported metrics to {file_path}")
            
        except Exception as e:
            logger.error(f"Error exporting metrics to file: {e}", exc_info=True)
    
    def _format_timedelta(self, td: timedelta) -> str:
        """Format timedelta for use as dictionary key."""
        total_seconds = int(td.total_seconds())
        if total_seconds < 60:
            return f"{total_seconds}s"
        elif total_seconds < 3600:
            return f"{total_seconds // 60}m"
        elif total_seconds < 86400:
            return f"{total_seconds // 3600}h"
        else:
            return f"{total_seconds // 86400}d"
    
    def generate_metrics_report(self) -> Dict[str, Any]:
        """Generate a comprehensive metrics report."""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'business_metrics': self.get_business_metrics(),
            'system_metrics': {
                'total_metrics_collected': len(self._raw_metrics),
                'active_counters': len(self._counters),
                'active_gauges': len(self._gauges),
                'active_histograms': len(self._histograms),
                'active_timers': len(self._timers)
            },
            'performance_metrics': {
                'claim_processing': self.get_timer_stats('claim_processing_duration_seconds'),
                'edi_parsing': self.get_timer_stats('edi_parsing_duration_seconds'),
                'validation': self.get_timer_stats('validation_duration_seconds'),
                'ml_prediction': self.get_timer_stats('ml_prediction_duration_seconds'),
                'database_operations': self.get_timer_stats('database_operation_duration_seconds')
            },
            'throughput_metrics': {
                'claims_per_minute_1m': self._calculate_rate('claims_processed_success', timedelta(minutes=1)),
                'claims_per_minute_5m': self._calculate_rate('claims_processed_success', timedelta(minutes=5)),
                'claims_per_minute_15m': self._calculate_rate('claims_processed_success', timedelta(minutes=15)),
                'claims_per_hour': self._calculate_rate('claims_processed_success', timedelta(hours=1))
            },
            'error_metrics': {
                'error_rate_1m': self._calculate_error_rate(timedelta(minutes=1)),
                'error_rate_5m': self._calculate_error_rate(timedelta(minutes=5)),
                'error_rate_15m': self._calculate_error_rate(timedelta(minutes=15)),
                'error_rate_1h': self._calculate_error_rate(timedelta(hours=1))
            }
        }
    
    def _calculate_rate(self, metric_name: str, window: timedelta) -> float:
        """Calculate rate of a metric over a time window."""
        cutoff_time = datetime.utcnow() - window
        
        with self._metrics_lock:
            count = len([m for m in self._raw_metrics 
                        if m.name == metric_name and m.timestamp >= cutoff_time])
            
            return count / window.total_seconds() * 60  # Per minute
    
    def _calculate_error_rate(self, window: timedelta) -> float:
        """Calculate error rate over a time window."""
        cutoff_time = datetime.utcnow() - window
        
        with self._metrics_lock:
            success_count = len([m for m in self._raw_metrics 
                               if m.name == 'claims_processed_success' and m.timestamp >= cutoff_time])
            error_count = len([m for m in self._raw_metrics 
                             if m.name == 'claims_processed_error' and m.timestamp >= cutoff_time])
            
            total = success_count + error_count
            return (error_count / total * 100) if total > 0 else 0
    
    def start_background_processing(self):
        """Start background thread for metrics aggregation and export."""
        if self._background_thread and self._background_thread.is_alive():
            return
        
        def background_loop():
            logger.info(f"Metrics background processing started (export interval: {self.export_interval}s)")
            
            while not self._stop_background:
                try:
                    # Export metrics
                    if self.export_enabled:
                        self.export_metrics_to_file()
                    
                    # Clean up old metrics (keep last 24 hours)
                    self._cleanup_old_metrics()
                    
                    time.sleep(self.export_interval)
                    
                except Exception as e:
                    logger.error(f"Error in metrics background processing: {e}", exc_info=True)
                    time.sleep(self.export_interval)
        
        self._background_thread = threading.Thread(target=background_loop, daemon=True)
        self._background_thread.start()
    
    def _cleanup_old_metrics(self):
        """Remove metrics older than 24 hours to prevent memory growth."""
        cutoff_time = datetime.utcnow() - timedelta(hours=24)
        
        with self._metrics_lock:
            # Clean raw metrics
            original_count = len(self._raw_metrics)
            self._raw_metrics = deque(
                (m for m in self._raw_metrics if m.timestamp >= cutoff_time),
                maxlen=10000
            )
            
            cleaned_count = original_count - len(self._raw_metrics)
            if cleaned_count > 0:
                logger.debug(f"Cleaned up {cleaned_count} old metrics")
            
            # Clean histogram and timer data
            for metric_name in list(self._histograms.keys()):
                if len(self._histograms[metric_name]) > 500:
                    self._histograms[metric_name] = self._histograms[metric_name][-250:]
            
            for metric_name in list(self._timers.keys()):
                if len(self._timers[metric_name]) > 500:
                    self._timers[metric_name] = self._timers[metric_name][-250:]
    
    def stop_background_processing(self):
        """Stop background processing thread."""
        self._stop_background = True
        if self._background_thread:
            self._background_thread.join(timeout=5)
        logger.info("Metrics background processing stopped")
    
    def reset_metrics(self, metric_type: MetricType = None):
        """Reset metrics for testing or debugging."""
        with self._metrics_lock:
            if metric_type is None:
                self._raw_metrics.clear()
                self._counters.clear()
                self._gauges.clear()
                self._histograms.clear()
                self._timers.clear()
                logger.info("All metrics reset")
            elif metric_type == MetricType.COUNTER:
                self._counters.clear()
                logger.info("Counter metrics reset")
            elif metric_type == MetricType.GAUGE:
                self._gauges.clear()
                logger.info("Gauge metrics reset")
            elif metric_type == MetricType.HISTOGRAM:
                self._histograms.clear()
                logger.info("Histogram metrics reset")
            elif metric_type == MetricType.TIMER:
                self._timers.clear()
                logger.info("Timer metrics reset")


# Global metrics collector instance
_metrics_collector = None

def get_metrics_collector(config: dict = None) -> MetricsCollector:
    """Get the global metrics collector instance."""
    global _metrics_collector
    if _metrics_collector is None:
        _metrics_collector = MetricsCollector(config)
    return _metrics_collector

# Decorator for automatic metrics collection
def collect_metrics(metric_name: str = None, labels: Dict[str, str] = None):
    """Decorator to automatically collect timing and success/error metrics."""
    def decorator(func):
        nonlocal metric_name
        if metric_name is None:
            metric_name = f"{func.__module__}.{func.__name__}"
        
        def wrapper(*args, **kwargs):
            collector = get_metrics_collector()
            start_time = time.perf_counter()
            
            try:
                result = func(*args, **kwargs)
                
                # Record success
                collector.record_counter(f"{metric_name}_success", 1, labels)
                
                # Record timing
                duration = time.perf_counter() - start_time
                collector.record_timing(f"{metric_name}_duration_seconds", duration, labels)
                
                return result
                
            except Exception as e:
                # Record error
                error_labels = (labels or {}).copy()
                error_labels['error_type'] = type(e).__name__
                collector.record_counter(f"{metric_name}_error", 1, error_labels)
                
                # Record timing even for errors
                duration = time.perf_counter() - start_time
                collector.record_timing(f"{metric_name}_duration_seconds", duration, labels)
                
                raise
        
        return wrapper
    return decorator


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    import random
    
    setup_logging()
    set_correlation_id("METRICS_TEST")
    
    # Test the metrics collector
    config = {
        'metrics': {
            'export_enabled': True,
            'export_interval_seconds': 5,
            'export_file_path': 'logs/test_metrics.jsonl',
            'auto_start_background': True
        }
    }
    
    collector = MetricsCollector(config)
    
    print("Testing metrics collection...")
    
    # Test different metric types
    for i in range(50):
        # Counter metrics
        collector.record_counter('claims_processed_success', 1, {'facility': f'FAC{(i % 3) + 1}'})
        
        if random.random() < 0.1:  # 10% error rate
            collector.record_counter('claims_processed_error', 1, {'facility': f'FAC{(i % 3) + 1}', 'error': 'ValidationError'})
        
        # Gauge metrics
        collector.record_gauge('claims_pending_count', random.randint(10, 100))
        collector.record_gauge('active_connections', random.randint(5, 20))
        
        # Histogram metrics
        collector.record_histogram('claim_charge_amount', random.uniform(50, 5000))
        
        # Timer metrics
        collector.record_timing('claim_processing_duration_seconds', random.uniform(0.1, 2.0))
        collector.record_timing('edi_parsing_duration_seconds', random.uniform(0.01, 0.5))
        
        time.sleep(0.1)  # Simulate processing time
    
    # Test decorator
    @collect_metrics('test_function')
    def test_function(should_fail=False):
        time.sleep(random.uniform(0.01, 0.1))
        if should_fail:
            raise ValueError("Test error")
        return "success"
    
    # Test successful calls
    for _ in range(10):
        test_function()
    
    # Test failed calls
    for _ in range(3):
        try:
            test_function(should_fail=True)
        except ValueError:
            pass
    
    # Wait a bit for background processing
    time.sleep(8)
    
    # Generate report
    report = collector.generate_metrics_report()
    
    print("\n=== METRICS REPORT ===")
    print(f"Timestamp: {report['timestamp']}")
    
    print("\n--- Business Metrics ---")
    bm = report['business_metrics']
    print(f"Claims Processed: {bm['claims_processed_total']}")
    print(f"Claims Failed: {bm['claims_failed_total']}")
    print(f"Claims Pending: {bm['claims_pending']}")
    print(f"Throughput: {bm['throughput_claims_per_second']:.2f} claims/sec")
    print(f"Error Rate: {bm['error_rate_percentage']:.2f}%")
    print(f"Avg Processing Time: {bm['average_processing_time_seconds']:.3f}s")
    print(f"Health Score: {bm['system_health_score']}/100")
    
    print("\n--- System Metrics ---")
    sm = report['system_metrics']
    print(f"Total Metrics Collected: {sm['total_metrics_collected']}")
    print(f"Active Counters: {sm['active_counters']}")
    print(f"Active Gauges: {sm['active_gauges']}")
    print(f"Active Timers: {sm['active_timers']}")
    
    print("\n--- Performance Metrics ---")
    pm = report['performance_metrics']
    if pm['claim_processing']:
        cp = pm['claim_processing']
        print(f"Claim Processing: {cp.get('count', 0)} samples, avg: {cp.get('avg_ms', 0):.2f}ms")
    
    print("\n--- Throughput Metrics ---")
    tm = report['throughput_metrics']
    print(f"1 minute: {tm['claims_per_minute_1m']:.2f} claims/min")
    print(f"5 minute: {tm['claims_per_minute_5m']:.2f} claims/min")
    print(f"15 minute: {tm['claims_per_minute_15m']:.2f} claims/min")
    
    print("\n--- Error Metrics ---")
    em = report['error_metrics']
    print(f"1 minute error rate: {em['error_rate_1m']:.2f}%")
    print(f"5 minute error rate: {em['error_rate_5m']:.2f}%")
    
    # Test specific metric retrieval
    print("\n--- Specific Metrics ---")
    success_count = collector.get_counter_value('claims_processed_success')
    pending_count = collector.get_gauge_value('claims_pending_count')
    processing_stats = collector.get_timer_stats('claim_processing_duration_seconds')
    
    print(f"Total successful claims: {success_count}")
    print(f"Current pending claims: {pending_count}")
    if processing_stats:
        print(f"Processing time stats: {processing_stats}")
    
    # Check export file
    export_file = config['metrics']['export_file_path']
    if os.path.exists(export_file):
        with open(export_file, 'r') as f:
            lines = f.readlines()
        print(f"\nExported {len(lines)} metric snapshots to {export_file}")
    
    collector.stop_background_processing()
    print("\nMetrics collection test completed.")