Windows Development Setup Guide for EDI Claims ProcessorPlatform-Specific Considerations1. Event Loop DifferencesIssue: uvloop is not supported on WindowsSolution: The requirements.txt now conditionally installs platform-specific event loops:# Linux/macOS gets uvloop (high performance)
uvloop>=0.19.0; sys_platform != "win32"

# Windows gets winloop (compatibility alternative)
winloop>=0.1.0; sys_platform == "win32"
2. Database Driver ConsiderationsSQL Server ODBC Driver on Windows# Download and install Microsoft ODBC Driver 17 for SQL Server
# [https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server](https://docs.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

# Verify installation
sqlcmd -?
PostgreSQL on Windows# Install PostgreSQL
winget install PostgreSQL.PostgreSQL

# Or download from: [https://www.postgresql.org/download/windows/](https://www.postgresql.org/download/windows/)
3. FastAPI Configuration for WindowsUpdate your app/api/main.py to handle event loop properly:import sys
import asyncio

# Configure event loop policy for Windows
if sys.platform == "win32":
    # Use ProactorEventLoop on Windows for better IOCP support
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    try:
        import winloop
        asyncio.set_event_loop_policy(winloop.EventLoopPolicy())
    except ImportError:
        pass  # Fall back to default Windows event loop
else:
    # Use uvloop on Unix systems
    try:
        import uvloop
        asyncio.set_event_loop_policy(uvloop.EventLoopPolicy())
    except ImportError:
        pass  # Fall back to default event loop
4. Running the Application on WindowsDevelopment Mode# Install dependencies
pip install -r requirements.txt

# Run with uvicorn (development)
uvicorn app.api.main:app --host 0.0.0.0 --port 8000 --reload

# Run main application (batch processing from database)
python -m app.main --run_all_processing
Production Mode on Windows# Use hypercorn instead of uvicorn with uvloop
hypercorn app.api.main:app --bind 0.0.0.0:8000 --workers 4

# Or use waitress (Windows-friendly WSGI server)
pip install waitress
waitress-serve --host=0.0.0.0 --port=8000 app.api.main:app
5. Windows Service ConfigurationFor running as a Windows Service, create windows_service.py:import sys
import servicemanager
import win32event
import win32service
import win32serviceutil
from app.main import main

class EDIClaimsProcessorService(win32serviceutil.ServiceFramework):
    _svc_name_ = "EDIClaimsProcessor"
    _svc_display_name_ = "EDI Claims Processor Service"
    _svc_description_ = "Processes EDI CMS 1500 claims with ML prediction"

    def __init__(self, args):
        win32serviceutil.ServiceFramework.__init__(self, args)
        self.hWaitStop = win32event.CreateEvent(None, 0, 0, None)

    def SvcStop(self):
        self.ReportServiceStatus(win32service.SERVICE_STOP_PENDING)
        win32event.SetEvent(self.hWaitStop)

    def SvcDoRun(self):
        servicemanager.LogMsg(
            servicemanager.EVENTLOG_INFORMATION_TYPE,
            servicemanager.PYS_SERVICE_STARTED,
            (self._svc_name_, '')
        )
        # Run your main application logic here
        main()

if __name__ == '__main__':
    win32serviceutil.HandleCommandLine(EDIClaimsProcessorService)
6. Windows-Specific File PathsUpdate config/config.yaml for Windows paths:file_paths:
  rvu_data_csv: "data\\rvu_data\\rvu_table.csv"  # Windows backslashes
  log_dir: "logs\\"

# Or use forward slashes (works on Windows too)
file_paths:
  rvu_data_csv: "data/rvu_data/rvu_table.csv"
  log_dir: "logs/"
7. Performance Optimizations for WindowsMemory Management# In your batch_handler.py, use ProcessPoolExecutor cautiously on Windows
import multiprocessing

if __name__ == '__main__':
    multiprocessing.freeze_support()  # Required for Windows
    
# Consider ThreadPoolExecutor for I/O bound operations on Windows
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=min(32, (os.cpu_count() or 1) + 4))
Database Connection String Adjustments# SQL Server connection for Windows Authentication
connection_string = (
    "mssql+pyodbc:///?odbc_connect="
    "DRIVER={ODBC Driver 17 for SQL Server};"
    "SERVER=localhost\\SQLEXPRESS;"
    "DATABASE=edi_production;"
    "Trusted_Connection=yes;"
    "TrustServerCertificate=yes;"  # For local development
)
8. Development Tools for Windows# Install Windows Subsystem for Linux (WSL) for better Docker support
wsl --install

# Install Docker Desktop for Windows
winget install Docker.DockerDesktop

# Install Git for Windows
winget install Git.Git

# Install Python build tools
winget install Microsoft.VisualStudio.2022.BuildTools
9. Troubleshooting Common Windows IssuesIssue: asyncio.run() RuntimeError# Solution: Use proper event loop handling
import asyncio
import sys

def run_async_main():
    if sys.platform == "win32":
        asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(your_async_function())
    finally:
        loop.close()
Issue: File locking problems# Use context managers and explicit file closing
import os
import tempfile

# For log file rotation on Windows
from logging.handlers import RotatingFileHandler
handler = RotatingFileHandler(
    filename='logs/app.log',
    maxBytes=10485760,
    backupCount=5,
    delay=True  # Don't open file until first log message
)
Issue: Path length limitations# Enable long path support in Windows registry or use short paths
import os
os.environ["PYTHONIOENCODING"] = "utf-8"
10. Alternative Requirements for Pure Windows EnvironmentIf you need a Windows-optimized requirements file:# Create windows-requirements.txt
pip freeze > windows-requirements.txt

# Key Windows alternatives:
# Instead of uvloop: use default asyncio or winloop
# Instead of gunicorn: use waitress or hypercorn
# Instead of supervisor: use Windows Service or Task Scheduler
11. Running Tests on Windows# Install test dependencies
pip install pytest pytest-asyncio pytest-cov

# Run tests with Windows-specific settings
$env:PYTHONPATH = "."
pytest tests/ -v --cov=app --cov-report=html

# Or use unittest for compatibility
python -m unittest discover tests/
This setup ensures your EDI Claims Processor runs smoothly on Windows while maintaining compatibility with Linux/macOS for production deployment.