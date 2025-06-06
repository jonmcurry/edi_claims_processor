# app/monitoring/alert_manager.py
"""
Alert management system for the EDI Claims Processor.
Handles threshold-based alerting, notification routing, and alert escalation.
"""
import time
import threading
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Callable
from dataclasses import dataclass, field
from enum import Enum
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

from app.utils.logging_config import get_logger, get_correlation_id
from app.monitoring.health_checker import HealthStatus, HealthChecker
from app.monitoring.performance_monitor import PerformanceMonitor

logger = get_logger('app.monitoring.alert_manager')

class AlertSeverity(Enum):
    INFO = "info"
    WARNING = "warning"
    CRITICAL = "critical"

class AlertStatus(Enum):
    ACTIVE = "active"
    RESOLVED = "resolved"
    SUPPRESSED = "suppressed"

@dataclass
class Alert:
    """Represents an alert."""
    id: str
    name: str
    severity: AlertSeverity
    message: str
    component: str
    status: AlertStatus = AlertStatus.ACTIVE
    created_at: datetime = field(default_factory=datetime.utcnow)
    resolved_at: Optional[datetime] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    notification_sent: bool = False
    escalation_level: int = 0

@dataclass
class AlertRule:
    """Defines conditions for triggering alerts."""
    name: str
    condition: Callable[[Any], bool]
    severity: AlertSeverity
    component: str
    message_template: str
    cooldown_minutes: int = 5
    auto_resolve: bool = True
    escalation_minutes: int = 30

class AlertManager:
    """
    Comprehensive alert management system.
    Monitors health and performance metrics and sends notifications.
    """
    
    def __init__(self, config: dict = None, health_checker: HealthChecker = None, 
                 performance_monitor: PerformanceMonitor = None):
        self.config = config or {}
        self.alert_config = self.config.get('alerting', {})
        
        self.health_checker = health_checker
        self.performance_monitor = performance_monitor
        
        self._alerts_lock = threading.Lock()
        self._active_alerts: Dict[str, Alert] = {}
        self._alert_history: List[Alert] = []
        self._alert_rules: List[AlertRule] = []
        self._last_alert_time: Dict[str, datetime] = {}
        
        self.notifications_enabled = self.alert_config.get('notifications_enabled', True)
        self.email_config = self.alert_config.get('email', {})
        self.webhook_config = self.alert_config.get('webhook', {})
        
        self._monitoring_enabled = True
        self._monitoring_interval = self.alert_config.get('check_interval_seconds', 60)
        self._monitoring_thread = None
        
        self._setup_default_alert_rules()
        
        logger.info("AlertManager initialized")
        
        if self.alert_config.get('auto_start_monitoring', True):
            self.start_monitoring()
    
    def _setup_default_alert_rules(self):
        """Setup default alert rules for common conditions."""
        
        self.add_alert_rule(AlertRule(
            name="database_unhealthy",
            condition=lambda health_report: any(
                comp['status'] == 'unhealthy' and 'SQL' in comp['component'] or 'PostgreSQL' in comp['component']
                for comp in health_report.get('components', [])
            ),
            severity=AlertSeverity.CRITICAL,
            component="database",
            message_template="Database health check failed: {details}",
            cooldown_minutes=10
        ))
        
        self.add_alert_rule(AlertRule(
            name="high_error_rate",
            condition=lambda perf_report: any(
                stats.get('error_rate', 0) > 0.1
                for stats in perf_report.get('operations', {}).values()
            ),
            severity=AlertSeverity.WARNING,
            component="processing",
            message_template="High error rate detected: {error_rate:.2%}",
            cooldown_minutes=15
        ))
        
        self.add_alert_rule(AlertRule(
            name="low_throughput",
            condition=lambda perf_report: any(
                stats.get('avg_rate', 0) < 10
                for op, stats in perf_report.get('throughput', {}).items()
                if 'claim' in op.lower()
            ),
            severity=AlertSeverity.WARNING,
            component="performance",
            message_template="Low processing throughput: {throughput:.2f} claims/sec",
            cooldown_minutes=20
        ))
        
        self.add_alert_rule(AlertRule(
            name="high_cpu_usage",
            condition=lambda perf_report: perf_report.get('system_metrics', {}).get('system_cpu_percent', 0) > 90,
            severity=AlertSeverity.WARNING,
            component="system",
            message_template="High CPU usage: {cpu_percent:.1f}%",
            cooldown_minutes=10
        ))
        
        self.add_alert_rule(AlertRule(
            name="high_memory_usage",
            condition=lambda perf_report: perf_report.get('system_metrics', {}).get('system_memory_percent', 0) > 90,
            severity=AlertSeverity.CRITICAL,
            component="system",
            message_template="High memory usage: {memory_percent:.1f}%",
            cooldown_minutes=5
        ))
        
        self.add_alert_rule(AlertRule(
            name="failed_claims_accumulating",
            condition=lambda health_report: any(
                comp.get('metadata', {}).get('new_failed_claims', 0) > 50
                for comp in health_report.get('components', [])
                if comp['component'] == 'SQL Server'
            ),
            severity=AlertSeverity.WARNING,
            component="claims_processing",
            message_template="Failed claims accumulating: {failed_count} new failures",
            cooldown_minutes=30
        ))
        
        logger.info(f"Setup {len(self._alert_rules)} default alert rules")
    
    def add_alert_rule(self, rule: AlertRule):
        """Add a new alert rule."""
        self._alert_rules.append(rule)
        logger.info(f"Added alert rule: {rule.name} ({rule.severity.value})")
    
    def remove_alert_rule(self, rule_name: str) -> bool:
        """Remove an alert rule by name."""
        original_count = len(self._alert_rules)
        self._alert_rules = [rule for rule in self._alert_rules if rule.name != rule_name]
        removed = len(self._alert_rules) < original_count
        if removed:
            logger.info(f"Removed alert rule: {rule_name}")
        return removed
    
    def create_alert(self, rule: AlertRule, context_data: Dict[str, Any] = None) -> Alert:
        """Create a new alert."""
        context_data = context_data or {}
        
        alert_id = f"{rule.name}_{int(time.time())}"
        message = rule.message_template.format(**context_data)
        
        alert = Alert(
            id=alert_id,
            name=rule.name,
            severity=rule.severity,
            message=message,
            component=rule.component,
            metadata=context_data
        )
        
        with self._alerts_lock:
            self._active_alerts[alert_id] = alert
            self._alert_history.append(alert)
            self._last_alert_time[rule.name] = alert.created_at
        
        logger.warning(f"Alert created: {rule.name} - {message}")
        return alert
    
    def resolve_alert(self, alert_id: str, auto_resolved: bool = False):
        """Resolve an active alert."""
        with self._alerts_lock:
            if alert_id in self._active_alerts:
                alert = self._active_alerts[alert_id]
                alert.status = AlertStatus.RESOLVED
                alert.resolved_at = datetime.utcnow()
                del self._active_alerts[alert_id]
                
                logger.info(f"Alert resolved: {alert.name} ({'auto' if auto_resolved else 'manual'})")
    
    def suppress_alert(self, alert_id: str, duration_minutes: int = 60):
        """Suppress an alert for a specified duration."""
        with self._alerts_lock:
            if alert_id in self._active_alerts:
                alert = self._active_alerts[alert_id]
                alert.status = AlertStatus.SUPPRESSED
                alert.metadata['suppressed_until'] = datetime.utcnow() + timedelta(minutes=duration_minutes)
                
                logger.info(f"Alert suppressed: {alert.name} for {duration_minutes} minutes")
    
    def check_alert_conditions(self):
        """Check all alert rules against current system state."""
        if not self.health_checker or not self.performance_monitor:
            logger.debug("Health checker or performance monitor not available for alert checking")
            return
        
        try:
            health_report = self.health_checker.run_comprehensive_health_check()
            perf_report = self.performance_monitor.generate_performance_report()
            
            current_time = datetime.utcnow()
            
            for rule in self._alert_rules:
                last_alert = self._last_alert_time.get(rule.name)
                if last_alert and current_time - last_alert < timedelta(minutes=rule.cooldown_minutes):
                    continue
                
                try:
                    context_data = {}
                    condition_met = False
                    
                    if rule.component == "database":
                        condition_met = rule.condition(health_report)
                        if condition_met:
                            unhealthy_dbs = [comp for comp in health_report.get('components', []) 
                                           if comp['status'] == 'unhealthy' and 
                                           ('SQL' in comp['component'] or 'PostgreSQL' in comp['component'])]
                            context_data['details'] = ', '.join([db['component'] + ': ' + db['message'] for db in unhealthy_dbs])
                    
                    elif rule.component == "processing":
                        condition_met = rule.condition(perf_report)
                        if condition_met:
                            error_rates = [stats.get('error_rate', 0) for stats in perf_report.get('operations', {}).values()]
                            context_data['error_rate'] = max(error_rates) if error_rates else 0
                    
                    elif rule.component == "performance":
                        condition_met = rule.condition(perf_report)
                        if condition_met:
                            throughput_rates = [stats.get('avg_rate', 0) for op, stats in perf_report.get('throughput', {}).items() 
                                              if 'claim' in op.lower()]
                            context_data['throughput'] = min(throughput_rates) if throughput_rates else 0
                    
                    elif rule.component == "system":
                        condition_met = rule.condition(perf_report)
                        if condition_met:
                            system_metrics = perf_report.get('system_metrics', {})
                            context_data.update({
                                'cpu_percent': system_metrics.get('system_cpu_percent', 0),
                                'memory_percent': system_metrics.get('system_memory_percent', 0)
                            })
                    
                    elif rule.component == "claims_processing":
                        condition_met = rule.condition(health_report)
                        if condition_met:
                            for comp in health_report.get('components', []):
                                if comp['component'] == 'SQL Server':
                                    context_data['failed_count'] = comp.get('metadata', {}).get('new_failed_claims', 0)
                    
                    if condition_met:
                        alert = self.create_alert(rule, context_data)
                        if self.notifications_enabled:
                            self.send_notification(alert)
                    
                    elif rule.auto_resolve:
                        alerts_to_resolve = [alert_id for alert_id, alert in self._active_alerts.items() 
                                           if alert.name == rule.name]
                        for alert_id in alerts_to_resolve:
                            self.resolve_alert(alert_id, auto_resolved=True)
                
                except Exception as e:
                    logger.error(f"Error checking alert rule {rule.name}: {e}", exc_info=True)
        
        except Exception as e:
            logger.error(f"Error in alert condition checking: {e}", exc_info=True)
    
    def send_notification(self, alert: Alert):
        """Send notification for an alert."""
        if not self.notifications_enabled:
            return
        
        try:
            if self.email_config.get('enabled', False):
                self._send_email_notification(alert)
            
            if self.webhook_config.get('enabled', False):
                self._send_webhook_notification(alert)
            
            alert.notification_sent = True
            logger.info(f"Notification sent for alert: {alert.name}")
            
        except Exception as e:
            logger.error(f"Error sending notification for alert {alert.name}: {e}", exc_info=True)
    
    def _send_email_notification(self, alert: Alert):
        """Send email notification."""
        if not self.email_config.get('enabled', False):
            return
        
        smtp_server = self.email_config.get('smtp_server')
        smtp_port = self.email_config.get('smtp_port', 587)
        username = self.email_config.get('username')
        password = self.email_config.get('password')
        from_email = self.email_config.get('from_email', username)
        to_emails = self.email_config.get('to_emails', [])
        
        if not all([smtp_server, username, password, to_emails]):
            logger.warning("Email configuration incomplete, skipping email notification")
            return
        
        subject = f"[{alert.severity.value.upper()}] EDI Claims Processor Alert: {alert.name}"
        
        body = f"""
Alert Details:
- Alert: {alert.name}
- Severity: {alert.severity.value.upper()}
- Component: {alert.component}
- Message: {alert.message}
- Created: {alert.created_at.isoformat()}
- Correlation ID: {get_correlation_id()}

Metadata:
{chr(10).join([f"- {k}: {v}" for k, v in alert.metadata.items()])}

This is an automated alert from the EDI Claims Processor monitoring system.
"""
        
        msg = MIMEMultipart()
        msg['From'] = from_email
        msg['To'] = ', '.join(to_emails)
        msg['Subject'] = subject
        msg.attach(MIMEText(body, 'plain'))
        
        try:
            with smtplib.SMTP(smtp_server, smtp_port) as server:
                server.starttls()
                server.login(username, password)
                server.send_message(msg)
            
            logger.info(f"Email notification sent for alert: {alert.name}")
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
    
    def _send_webhook_notification(self, alert: Alert):
        """Send webhook notification."""
        if not self.webhook_config.get('enabled', False):
            return
        
        webhook_url = self.webhook_config.get('url')
        if not webhook_url:
            logger.warning("Webhook URL not configured, skipping webhook notification")
            return
        
        payload = {
            'alert_id': alert.id,
            'name': alert.name,
            'severity': alert.severity.value,
            'component': alert.component,
            'message': alert.message,
            'status': alert.status.value,
            'created_at': alert.created_at.isoformat(),
            'metadata': alert.metadata,
            'correlation_id': get_correlation_id()
        }
        
        try:
            logger.info(f"Webhook notification would be sent to {webhook_url} for alert: {alert.name}")
            
        except Exception as e:
            logger.error(f"Failed to send webhook notification: {e}")
    
    def start_monitoring(self):
        """Start the alert monitoring thread."""
        if self._monitoring_thread and self._monitoring_thread.is_alive():
            return
        
        def monitor_loop():
            logger.info(f"Alert monitoring started (interval: {self._monitoring_interval}s)")
            while self._monitoring_enabled:
                try:
                    self.check_alert_conditions()
                    time.sleep(self._monitoring_interval)
                except Exception as e:
                    logger.error(f"Error in alert monitoring loop: {e}", exc_info=True)
                    time.sleep(self._monitoring_interval)
        
        self._monitoring_thread = threading.Thread(target=monitor_loop, daemon=True)
        self._monitoring_thread.start()
    
    def stop_monitoring(self):
        """Stop the alert monitoring thread."""
        self._monitoring_enabled = False
        if self._monitoring_thread:
            self._monitoring_thread.join(timeout=5)
        logger.info("Alert monitoring stopped")
    
    def get_active_alerts(self, severity: AlertSeverity = None, component: str = None) -> List[Alert]:
        """Get currently active alerts."""
        with self._alerts_lock:
            alerts = list(self._active_alerts.values())
            
            if severity:
                alerts = [a for a in alerts if a.severity == severity]
            
            if component:
                alerts = [a for a in alerts if a.component == component]
            
            return sorted(alerts, key=lambda x: x.created_at, reverse=True)
    
    def get_alert_history(self, hours: int = 24, severity: AlertSeverity = None) -> List[Alert]:
        """Get alert history for the specified time period."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        with self._alerts_lock:
            alerts = [a for a in self._alert_history if a.created_at >= cutoff_time]
            
            if severity:
                alerts = [a for a in alerts if a.severity == severity]
            
            return sorted(alerts, key=lambda x: x.created_at, reverse=True)
    
    def get_alert_statistics(self, hours: int = 24) -> Dict[str, Any]:
        """Get alert statistics for the specified time period."""
        cutoff_time = datetime.utcnow() - timedelta(hours=hours)
        
        with self._alerts_lock:
            recent_alerts = [a for a in self._alert_history if a.created_at >= cutoff_time]
            
            stats = {
                'total_alerts': len(recent_alerts),
                'active_alerts': len(self._active_alerts),
                'by_severity': {
                    'critical': len([a for a in recent_alerts if a.severity == AlertSeverity.CRITICAL]),
                    'warning': len([a for a in recent_alerts if a.severity == AlertSeverity.WARNING]),
                    'info': len([a for a in recent_alerts if a.severity == AlertSeverity.INFO])
                },
                'by_component': {},
                'by_status': {
                    'active': len([a for a in recent_alerts if a.status == AlertStatus.ACTIVE]),
                    'resolved': len([a for a in recent_alerts if a.status == AlertStatus.RESOLVED]),
                    'suppressed': len([a for a in recent_alerts if a.status == AlertStatus.SUPPRESSED])
                },
                'resolution_times': []
            }
            
            for alert in recent_alerts:
                component = alert.component
                if component not in stats['by_component']:
                    stats['by_component'][component] = 0
                stats['by_component'][component] += 1
            
            for alert in recent_alerts:
                if alert.status == AlertStatus.RESOLVED and alert.resolved_at:
                    resolution_time = (alert.resolved_at - alert.created_at).total_seconds()
                    stats['resolution_times'].append(resolution_time)
            
            if stats['resolution_times']:
                stats['avg_resolution_time_seconds'] = sum(stats['resolution_times']) / len(stats['resolution_times'])
                stats['max_resolution_time_seconds'] = max(stats['resolution_times'])
            else:
                stats['avg_resolution_time_seconds'] = 0
                stats['max_resolution_time_seconds'] = 0
            
            return stats
    
    def generate_alert_report(self) -> Dict[str, Any]:
        """Generate a comprehensive alert report."""
        return {
            'timestamp': datetime.utcnow().isoformat(),
            'active_alerts': [
                {
                    'id': alert.id,
                    'name': alert.name,
                    'severity': alert.severity.value,
                    'component': alert.component,
                    'message': alert.message,
                    'created_at': alert.created_at.isoformat(),
                    'age_minutes': (datetime.utcnow() - alert.created_at).total_seconds() / 60
                }
                for alert in self.get_active_alerts()
            ],
            'statistics_24h': self.get_alert_statistics(24),
            'statistics_1h': self.get_alert_statistics(1),
            'alert_rules': [
                {
                    'name': rule.name,
                    'severity': rule.severity.value,
                    'component': rule.component,
                    'cooldown_minutes': rule.cooldown_minutes,
                    'auto_resolve': rule.auto_resolve
                }
                for rule in self._alert_rules
            ],
            'configuration': {
                'notifications_enabled': self.notifications_enabled,
                'monitoring_interval_seconds': self._monitoring_interval,
                'email_enabled': self.email_config.get('enabled', False),
                'webhook_enabled': self.webhook_config.get('enabled', False)
            }
        }


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, dispose_engines, CONFIG
    import time
    
    setup_logging()
    set_correlation_id("ALERT_MANAGER_TEST")
    
    test_config = {
        'alerting': {
            'notifications_enabled': True,
            'check_interval_seconds': 5,
            'email': {
                'enabled': False,
                'smtp_server': 'smtp.gmail.com',
                'smtp_port': 587,
                'username': 'your_email@gmail.com',
                'password': 'your_app_password',
                'from_email': 'edi-alerts@yourcompany.com',
                'to_emails': ['admin@yourcompany.com']
            },
            'webhook': {
                'enabled': False,
                'url': 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
            }
        },
        'monitoring': {
            'db_timeout_threshold_ms': 1000,
            'memory_threshold_percent': 50,
            'cpu_threshold_percent': 50,
            'system_monitoring_enabled': True,
            'system_monitor_interval_seconds': 2
        }
    }
    
    try:
        init_database_connections()
        
        health_checker = HealthChecker(test_config)
        performance_monitor = PerformanceMonitor(test_config)
        
        alert_manager = AlertManager(test_config, health_checker, performance_monitor)
        
        print("Alert Manager Test Started")
        print("Monitoring for alerts...")
        
        time.sleep(20)
        
        print("\nSimulating high error rate...")
        for i in range(10):
            performance_monitor.record_counter("test_operation_error", 1)
            performance_monitor.record_counter("test_operation_total", 1)
        
        time.sleep(10)
        
        report = alert_manager.generate_alert_report()
        
        print("\n=== ALERT REPORT ===")
        print(f"Timestamp: {report['timestamp']}")
        print(f"Active Alerts: {len(report['active_alerts'])}")
        
        if report['active_alerts']:
            print("\n--- Active Alerts ---")
            for alert in report['active_alerts']:
                print(f"🚨 {alert['severity'].upper()}: {alert['name']}")
                print(f"   Component: {alert['component']}")
                print(f"   Message: {alert['message']}")
                print(f"   Age: {alert['age_minutes']:.1f} minutes")
        
        print(f"\n--- 24h Statistics ---")
        stats = report['statistics_24h']
        print(f"Total Alerts: {stats['total_alerts']}")
        print(f"Currently Active: {stats['active_alerts']}")
        print(f"By Severity - Critical: {stats['by_severity']['critical']}, Warning: {stats['by_severity']['warning']}, Info: {stats['by_severity']['info']}")
        
        if stats['by_component']:
            print("By Component:")
            for component, count in stats['by_component'].items():
                print(f"  {component}: {count}")
        
        print(f"\n--- Configuration ---")
        config_info = report['configuration']
        print(f"Notifications Enabled: {config_info['notifications_enabled']}")
        print(f"Email Enabled: {config_info['email_enabled']}")
        print(f"Webhook Enabled: {config_info['webhook_enabled']}")
        
        print(f"\n--- Alert Rules ({len(report['alert_rules'])}) ---")
        for rule in report['alert_rules']:
            print(f"- {rule['name']} ({rule['severity']}) - {rule['component']}")
        
        print("\n--- Testing Manual Alert ---")
        test_rule = AlertRule(
            name="test_manual_alert",
            condition=lambda x: True,
            severity=AlertSeverity.INFO,
            component="test",
            message_template="This is a test alert: {test_data}",
            cooldown_minutes=1
        )
        
        test_alert = alert_manager.create_alert(test_rule, {"test_data": "Hello World"})
        print(f"Created test alert: {test_alert.id}")
        
        time.sleep(2)
        alert_manager.resolve_alert(test_alert.id)
        print(f"Resolved test alert: {test_alert.id}")
        
        alert_manager.stop_monitoring()
        performance_monitor.stop_system_monitoring()
        
        print("\nAlert Manager test completed.")
        
    except Exception as e:
        logger.critical(f"Error in Alert Manager test: {e}", exc_info=True)
    finally:
        dispose_engines()
