#!/usr/bin/env python3
"""
mrcoordinator.py - Main program for MapReduce Coordinator

Usage: python mrcoordinator.py inputfile1 [inputfile2 ...]
Example: python mrcoordinator.py pg-*.txt

This starts the coordinator process that manages the MapReduce job.
"""

import sys
import time
import threading
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)


def main():
    """Main function for coordinator"""
    if len(sys.argv) < 2:
        print("Usage: python mrcoordinator.py inputfile1 [inputfile2 ...]")
        print("Example: python mrcoordinator.py pg-*.txt")
        sys.exit(1)
    
    input_files = sys.argv[1:]
    n_reduce = 10  # Default number of reduce tasks
    
    # Import here to avoid circular imports
    import mapreduce_coordinator as mc 
    #from mapreduce_coordinator import make_coordinator
    
    coordinator = mc.make_coordinator(input_files, n_reduce)
    
    # Start server in a separate thread
    server_thread = threading.Thread(target=coordinator.server, daemon=True)
    server_thread.start()
    
    print(f"Coordinator started with {len(input_files)} input files and {n_reduce} reduce tasks")
    print("Waiting for workers to connect...")
    print("Workers should run: python mrworker.py <plugin.py>")
    
    # Wait for completion
    try:
        while not coordinator.done():
            time.sleep(1)
        
        print("MapReduce job completed successfully!")
        # Give a moment for any final RPC calls
        time.sleep(2)
        
    except KeyboardInterrupt:
        print("\nCoordinator interrupted")
        sys.exit(1)


if __name__ == "__main__":
    main()