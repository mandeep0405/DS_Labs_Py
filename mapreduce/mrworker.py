#!/usr/bin/env python3
"""
mrworker.py - Main program for MapReduce Worker

Usage: python mrworker.py plugin.py
Example: python mrworker.py wc.py

This starts a worker process that connects to the coordinator
and executes map/reduce tasks.
"""

import sys
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    """Main function for worker"""
    if len(sys.argv) != 2:
        print("Usage: python mrworker.py plugin.py")
        print("Example: python mrworker.py wc.py")
        print("\nAvailable plugins:")
        print("  wc.py      - Word count")
        print("  indexer.py - Text indexer")
        sys.exit(1)
    
    plugin_path = sys.argv[1]
    coordinator_address = "localhost:8000"
    
    # Import here to avoid circular imports
    from mapreduce_worker import worker_main as run_worker
    
    print(f"Worker starting with plugin: {plugin_path}")
    print(f"Connecting to coordinator at: {coordinator_address}")
    
    try:
        run_worker(coordinator_address, plugin_path)
        print("Worker finished successfully")
    except KeyboardInterrupt:
        print("\nWorker interrupted")
        sys.exit(1)
    except Exception as e:
        print(f"Worker failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()