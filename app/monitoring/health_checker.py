# app/monitoring/health_checker.py
"""
Health checking system for the EDI Claims Processor.
Monitors database connections, file system, cache, and external services.
"""
import time
import os
import psutil
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
from dataclasses import dataclass
from enum import Enum

from app.utils.logging_config import get_logger, get_correlation_id
from app.utils.error_handler import AppException
from app.database.connection_manager import (
    check_postgres_connection, check_sqlserver_connection,
    get_postgres_session, get_sqlserver_session
)
from app.utils.caching import rvu_cache_instance

logger = get_logger('app.monitoring.health_checker')

class HealthStatus(Enum):
    HEALTHY = "healthy"
    DEGRADED = "degraded"
    UNHEALTHY = "unhealthy"
    UNKNOWN = "unknown"

@dataclass
class HealthCheckResult:
    """Represents the result of a health check."""
    component: str
    status: HealthStatus
    message: str
    response_time_ms: Optional[float] = None
    metadata: Optional[Dict[str, Any]] = None
    timestamp: datetime = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.utcnow()

class HealthChecker:
    """
    Comprehensive health checking system for the EDI Claims Processor.
    """
    
    def __init__(self, config: dict = None):
        self.config = config or {}
        self.monitoring_config = self.config.get('monitoring', {})
        
        # Health check thresholds
        self.db_timeout_threshold = self.monitoring_config.get('db_timeout_threshold_ms', 5000)
        self.memory_threshold_percent = self.monitoring_config.get('memory_threshold_percent', 85)
        self.disk_threshold_percent = self.monitoring_config.get('disk_threshold_percent', 90)
        self.cpu_threshold_percent = self.monitoring_config.get('cpu_threshold_percent', 80)
        
        logger.info("HealthChecker initialized with thresholds: "
                   f"DB timeout: {self.db_timeout_threshold}ms, "
                   f"Memory: {self.memory_threshold_percent}%, "
                   f"Disk: {self.disk_threshold_percent}%, "
                   f"CPU: {self.cpu_threshold_percent}%")

    def check_database_health(self) -> List[HealthCheckResult]:
        """Check health of both PostgreSQL and SQL Server databases."""
        results = []
        cid = get_correlation_id()
        
        # PostgreSQL Health Check
        start_time = time.perf_counter()
        try:
            pg_session = None
            try:
                pg_session = get_postgres_session()
                success = check_postgres_connection(pg_session)
                response_time = (time.perf_counter() - start_time) * 1000
                
                if success and response_time < self.db_timeout_threshold:
                    status = HealthStatus.HEALTHY
                    message = f"PostgreSQL connection healthy (response: {response_time:.2f}ms)"
                elif success and response_time >= self.db_timeout_threshold:
                    status = HealthStatus.DEGRADED
                    message = f"PostgreSQL slow response ({response_time:.2f}ms)"
                else:
                    status = HealthStatus.UNHEALTHY
                    message = "PostgreSQL connection failed"
                
                # Additional PostgreSQL metrics
                try:
                    # Check staging claims count
                    staging_count = pg_session.execute("SELECT COUNT(*) FROM staging.claims WHERE processing_status = 'PENDING'").scalar()
                    pending_export_count = pg_session.execute("SELECT COUNT(*) FROM staging.claims WHERE exported_to_production = FALSE").scalar()
                    
                    metadata = {
                        "pending_claims": staging_count,
                        "pending_export": pending_export_count,
                        "response_time_ms": response_time
                    }
                except Exception as e:
                    metadata = {"error_getting_metrics": str(e)}
                
                results.append(HealthCheckResult(
                    component="PostgreSQL",
                    status=status,
                    message=message,
                    response_time_ms=response_time,
                    metadata=metadata
                ))
            finally:
                if pg_session:
                    pg_session.close()
                    
        except Exception as e:
            response_time = (time.perf_counter() - start_time) * 1000
            results.append(HealthCheckResult(
                component="PostgreSQL",
                status=HealthStatus.UNHEALTHY,
                message=f"PostgreSQL health check failed: {str(e)}",
                response_time_ms=response_time,
                metadata={"error": str(e)}
            ))
        
        # SQL Server Health Check
        start_time = time.perf_counter()
        try:
            sql_session = None
            try:
                sql_session = get_sqlserver_session()
                success = check_sqlserver_connection(sql_session)
                response_time = (time.perf_counter() - start_time) * 1000
                
                if success and response_time < self.db_timeout_threshold:
                    status = HealthStatus.HEALTHY
                    message = f"SQL Server connection healthy (response: {response_time:.2f}ms)"
                elif success and response_time >= self.db_timeout_threshold:
                    status = HealthStatus.DEGRADED
                    message = f"SQL Server slow response ({response_time:.2f}ms)"
                else:
                    status = HealthStatus.UNHEALTHY
                    message = "SQL Server connection failed"
                
                # Additional SQL Server metrics
                try:
                    # Check production claims and failed claims
                    total_claims = sql_session.execute("SELECT COUNT(*) FROM dbo.Claims").scalar()
                    failed_claims = sql_session.execute("SELECT COUNT(*) FROM dbo.FailedClaimDetails WHERE Status = 'New'").scalar()
                    
                    metadata = {
                        "total_production_claims": total_claims,
                        "new_failed_claims": failed_claims,
                        "response_time_ms": response_time
                    }
                except Exception as e:
                    metadata = {"error_getting_metrics": str(e)}
                
                results.append(HealthCheckResult(
                    component="SQL Server",
                    status=status,
                    message=message,
                    response_time_ms=response_time,
                    metadata=metadata
                ))
            finally:
                if sql_session:
                    sql_session.close()
                    
        except Exception as e:
            response_time = (time.perf_counter() - start_time) * 1000
            results.append(HealthCheckResult(
                component="SQL Server",
                status=HealthStatus.UNHEALTHY,
                message=f"SQL Server health check failed: {str(e)}",
                response_time_ms=response_time,
                metadata={"error": str(e)}
            ))
        
        return results

    def check_system_resources(self) -> List[HealthCheckResult]:
        """Check system resource utilization (CPU, Memory, Disk)."""
        results = []
        
        try:
            # CPU Usage
            cpu_percent = psutil.cpu_percent(interval=1)
            if cpu_percent < self.cpu_threshold_percent:
                cpu_status = HealthStatus.HEALTHY
                cpu_message = f"CPU usage normal ({cpu_percent:.1f}%)"
            elif cpu_percent < self.cpu_threshold_percent + 10:
                cpu_status = HealthStatus.DEGRADED
                cpu_message = f"CPU usage elevated ({cpu_percent:.1f}%)"
            else:
                cpu_status = HealthStatus.UNHEALTHY
                cpu_message = f"CPU usage critical ({cpu_percent:.1f}%)"
            
            results.append(HealthCheckResult(
                component="CPU",
                status=cpu_status,
                message=cpu_message,
                metadata={"cpu_percent": cpu_percent, "threshold": self.cpu_threshold_percent}
            ))
            
            # Memory Usage
            memory = psutil.virtual_memory()
            memory_percent = memory.percent
            if memory_percent < self.memory_threshold_percent:
                memory_status = HealthStatus.HEALTHY
                memory_message = f"Memory usage normal ({memory_percent:.1f}%)"
            elif memory_percent < self.memory_threshold_percent + 5:
                memory_status = HealthStatus.DEGRADED
                memory_message = f"Memory usage elevated ({memory_percent:.1f}%)"
            else:
                memory_status = HealthStatus.UNHEALTHY
                memory_message = f"Memory usage critical ({memory_percent:.1f}%)"
            
            results.append(HealthCheckResult(
                component="Memory",
                status=memory_status,
                message=memory_message,
                metadata={
                    "memory_percent": memory_percent,
                    "available_gb": round(memory.available / (1024**3), 2),
                    "total_gb": round(memory.total / (1024**3), 2),
                    "threshold": self.memory_threshold_percent
                }
            ))
            
            # Disk Usage (for logs and data directories)
            disk_paths = [
                self.config.get('file_paths', {}).get('log_dir', 'logs/'),
                'data/',
                '.'  # Current directory
            ]
            
            for path in disk_paths:
                if os.path.exists(path):
                    disk_usage = psutil.disk_usage(path)
                    disk_percent = (disk_usage.used / disk_usage.total) * 100
                    
                    if disk_percent < self.disk_threshold_percent:
                        disk_status = HealthStatus.HEALTHY
                        disk_message = f"Disk space normal ({disk_percent:.1f}%)"
                    elif disk_percent < self.disk_threshold_percent + 5:
                        disk_status = HealthStatus.DEGRADED
                        disk_message = f"Disk space elevated ({disk_percent:.1f}%)"
                    else:
                        disk_status = HealthStatus.UNHEALTHY
                        disk_message = f"Disk space critical ({disk_percent:.1f}%)"
                    
                    results.append(HealthCheckResult(
                        component=f"Disk ({path})",
                        status=disk_status,
                        message=disk_message,
                        metadata={
                            "disk_percent": disk_percent,
                            "free_gb": round(disk_usage.free / (1024**3), 2),
                            "total_gb": round(disk_usage.total / (1024**3), 2),
                            "threshold": self.disk_threshold_percent
                        }
                    ))
                    break  # Check only the first valid path
                    
        except Exception as e:
            results.append(HealthCheckResult(
                component="System Resources",
                status=HealthStatus.UNHEALTHY,
                message=f"Failed to check system resources: {str(e)}",
                metadata={"error": str(e)}
            ))
        
        return results

    def check_cache_health(self) -> HealthCheckResult:
        """Check RVU cache health and accessibility."""
        try:
            start_time = time.perf_counter()
            
            if rvu_cache_instance is None:
                return HealthCheckResult(
                    component="RVU Cache",
                    status=HealthStatus.UNHEALTHY,
                    message="RVU cache instance is None"
                )
            
            # Test cache access with a common CPT code
            test_rvu = rvu_cache_instance.get_rvu_details('99213')
            response_time = (time.perf_counter() - start_time) * 1000
            
            if test_rvu is not None:
                status = HealthStatus.HEALTHY
                message = f"RVU cache accessible (response: {response_time:.2f}ms)"
                metadata = {
                    "test_lookup_success": True,
                    "response_time_ms": response_time,
                    "cache_type": rvu_cache_instance.cache_type
                }
            else:
                status = HealthStatus.DEGRADED
                message = "RVU cache accessible but test lookup failed"
                metadata = {
                    "test_lookup_success": False,
                    "response_time_ms": response_time,
                    "cache_type": rvu_cache_instance.cache_type
                }
            
            return HealthCheckResult(
                component="RVU Cache",
                status=status,
                message=message,
                response_time_ms=response_time,
                metadata=metadata
            )
            
        except Exception as e:
            return HealthCheckResult(
                component="RVU Cache",
                status=HealthStatus.UNHEALTHY,
                message=f"RVU cache check failed: {str(e)}",
                metadata={"error": str(e)}
            )

    def check_file_system_health(self) -> List[HealthCheckResult]:
        """Check critical directories and file permissions."""
        results = []
        
        critical_paths = [
            ('Log Directory', self.config.get('file_paths', {}).get('log_dir', 'logs/')),
            ('Data Directory', 'data/'),
            ('ML Model Directory', 'ml_model/'),
            ('Config Directory', 'config/')
        ]
        
        for name, path in critical_paths:
            try:
                if os.path.exists(path):
                    if os.path.isdir(path):
                        # Check if writable
                        if os.access(path, os.W_OK):
                            # Check file count if it's a directory like logs
                            if 'log' in path.lower():
                                file_count = len([f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))])
                                metadata = {"file_count": file_count, "writable": True}
                            else:
                                metadata = {"writable": True}
                            
                            results.append(HealthCheckResult(
                                component=name,
                                status=HealthStatus.HEALTHY,
                                message=f"{name} accessible and writable",
                                metadata=metadata
                            ))
                        else:
                            results.append(HealthCheckResult(
                                component=name,
                                status=HealthStatus.DEGRADED,
                                message=f"{name} exists but not writable",
                                metadata={"writable": False}
                            ))
                    else:
                        results.append(HealthCheckResult(
                            component=name,
                            status=HealthStatus.UNHEALTHY,
                            message=f"{name} exists but is not a directory",
                            metadata={"is_directory": False}
                        ))
                else:
                    results.append(HealthCheckResult(
                        component=name,
                        status=HealthStatus.UNHEALTHY,
                        message=f"{name} does not exist",
                        metadata={"exists": False}
                    ))
                    
            except Exception as e:
                results.append(HealthCheckResult(
                    component=name,
                    status=HealthStatus.UNHEALTHY,
                    message=f"Error checking {name}: {str(e)}",
                    metadata={"error": str(e)}
                ))
        
        return results

    def run_comprehensive_health_check(self) -> Dict[str, Any]:
        """Run all health checks and return a comprehensive report."""
        cid = get_correlation_id()
        logger.info(f"[{cid}] Starting comprehensive health check")
        
        start_time = time.perf_counter()
        all_results = []
        
        # Run all health checks
        try:
            all_results.extend(self.check_database_health())
        except Exception as e:
            logger.error(f"Error in database health check: {e}", exc_info=True)
        
        try:
            all_results.extend(self.check_system_resources())
        except Exception as e:
            logger.error(f"Error in system resource check: {e}", exc_info=True)
        
        try:
            all_results.append(self.check_cache_health())
        except Exception as e:
            logger.error(f"Error in cache health check: {e}", exc_info=True)
        
        try:
            all_results.extend(self.check_file_system_health())
        except Exception as e:
            logger.error(f"Error in file system health check: {e}", exc_info=True)
        
        # Aggregate results
        total_time = (time.perf_counter() - start_time) * 1000
        
        healthy_count = sum(1 for r in all_results if r.status == HealthStatus.HEALTHY)
        degraded_count = sum(1 for r in all_results if r.status == HealthStatus.DEGRADED)
        unhealthy_count = sum(1 for r in all_results if r.status == HealthStatus.UNHEALTHY)
        
        # Determine overall status
        if unhealthy_count > 0:
            overall_status = HealthStatus.UNHEALTHY
        elif degraded_count > 0:
            overall_status = HealthStatus.DEGRADED
        else:
            overall_status = HealthStatus.HEALTHY
        
        report = {
            "overall_status": overall_status.value,
            "timestamp": datetime.utcnow().isoformat(),
            "check_duration_ms": round(total_time, 2),
            "summary": {
                "total_checks": len(all_results),
                "healthy": healthy_count,
                "degraded": degraded_count,
                "unhealthy": unhealthy_count
            },
            "components": [
                {
                    "component": result.component,
                    "status": result.status.value,
                    "message": result.message,
                    "response_time_ms": result.response_time_ms,
                    "metadata": result.metadata,
                    "timestamp": result.timestamp.isoformat()
                }
                for result in all_results
            ]
        }
        
        logger.info(f"[{cid}] Health check completed: {overall_status.value} "
                   f"({healthy_count} healthy, {degraded_count} degraded, {unhealthy_count} unhealthy)")
        
        return report


if __name__ == '__main__':
    from app.utils.logging_config import setup_logging, set_correlation_id
    from app.database.connection_manager import init_database_connections, dispose_engines, CONFIG
    
    setup_logging()
    set_correlation_id("HEALTH_CHECK_TEST")
    
    try:
        init_database_connections()
        
        health_checker = HealthChecker(CONFIG)
        report = health_checker.run_comprehensive_health_check()
        
        print("\n=== HEALTH CHECK REPORT ===")
        print(f"Overall Status: {report['overall_status'].upper()}")
        print(f"Total Checks: {report['summary']['total_checks']}")
        print(f"Healthy: {report['summary']['healthy']}")
        print(f"Degraded: {report['summary']['degraded']}")
        print(f"Unhealthy: {report['summary']['unhealthy']}")
        print(f"Check Duration: {report['check_duration_ms']}ms")
        
        print("\n=== COMPONENT DETAILS ===")
        for component in report['components']:
            status_symbol = "✓" if component['status'] == 'healthy' else "⚠" if component['status'] == 'degraded' else "✗"
            print(f"{status_symbol} {component['component']}: {component['message']}")
            if component.get('response_time_ms'):
                print(f"   Response Time: {component['response_time_ms']:.2f}ms")
        
    except Exception as e:
        logger.critical(f"Error running health check test: {e}", exc_info=True)
    finally:
        dispose_engines()
        logger.info("Health check test completed")