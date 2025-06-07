import subprocess
import time
import sys
import os

# --- Script Configuration ---

# Ensure the app's root directory is in the Python path
# This allows for correct module imports like 'from app.api...'
sys.path.insert(0, os.path.abspath(os.path.dirname(__file__)))

# --- Commands to Execute ---

# 1. Command for the FastAPI/Uvicorn API server
uvicorn_command = [
    sys.executable,  # Use the current Python interpreter
    "-m", "uvicorn",
    "app.api.main:app",
    "--host", "0.0.0.0",
    "--port", "8000",
    "--reload"
]

# 2. Command for the background Batch Processing loop
#    This now correctly points to app.main and uses the right argument.
batch_command = [
    sys.executable,
    "-m", "app.main", # Use the module execution flag for app.main
    "--run-all-processing"  # Correct argument with a hyphen
]


def run_services():
    """
    Starts the API server and the batch processing application in parallel.
    Manages the lifecycle of these subprocesses.
    """
    print("--- Starting EDI Claims Processor Services ---")
    api_process = None
    batch_process = None

    try:
        # Start the FastAPI server
        print("Starting FastAPI server...")
        # On Windows, create a new console window for each process for cleaner output
        creation_flags = subprocess.CREATE_NEW_CONSOLE if sys.platform == "win32" else 0
        api_process = subprocess.Popen(uvicorn_command, creationflags=creation_flags)
        print(f"FastAPI server process started with PID: {api_process.pid}")

        # A brief pause to let the API server initialize before starting the next process
        time.sleep(5)

        # Start the Batch Processor
        print("\nStarting Batch Processing loop...")
        batch_process = subprocess.Popen(batch_command, creationflags=creation_flags)
        print(f"Batch processor process started with PID: {batch_process.pid}")

        print("-" * 40)
        print("✅ Both services are running in parallel.")
        print("   (Each may have its own console window on Windows)")
        print("\n   Press Ctrl+C in this main window to terminate both services.")
        print("-" * 40)

        # Wait for the processes to complete (they are long-running)
        api_process.wait()
        batch_process.wait()

    except KeyboardInterrupt:
        print("\n🛑 Shutdown signal received. Terminating services...")
    except Exception as e:
        print(f"\n❌ An unexpected error occurred: {e}")
    finally:
        if api_process and api_process.poll() is None:
            api_process.terminate()
            print("FastAPI server terminated.")
        if batch_process and batch_process.poll() is None:
            batch_process.terminate()
            print("Batch processor terminated.")
        print("--- Services have been shut down. ---")


if __name__ == "__main__":
    run_services()
