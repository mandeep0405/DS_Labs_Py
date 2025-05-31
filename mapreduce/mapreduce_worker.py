"""
MapReduce Worker Implementation

The worker is responsible for:
1. Requesting tasks from the coordinator
2. Executing map and reduce functions
3. Managing intermediate files
4. Reporting task completion
"""

import json
import os
import time
import tempfile
import logging
from typing import List, Dict, Callable, Any, Iterator
from collections import defaultdict
import xmlrpc.client
import importlib.util

from mapreduce_rpc import (
    TaskType, TaskRequest, TaskCompleteRequest, Task, KeyValue, 
    ihash, intermediate_filename, temp_filename
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Worker:
    """
    MapReduce Worker implementation
    
    The worker requests tasks from the coordinator and executes them.
    It handles both map and reduce phases, managing intermediate files
    and ensuring atomic writes.
    """
    
    def __init__(self, coordinator_address: str = "localhost:8000"):
        self.coordinator_address = coordinator_address
        self.coordinator = xmlrpc.client.ServerProxy(f"http://{coordinator_address}")
        self.map_func: Callable = None
        self.reduce_func: Callable = None
        
    def load_plugin(self, plugin_path: str):
        """Load map and reduce functions from a plugin file"""
        try:
            spec = importlib.util.spec_from_file_location("plugin", plugin_path)
            plugin = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(plugin)
            
            self.map_func = plugin.Map
            self.reduce_func = plugin.Reduce
            logger.info(f"Loaded plugin from {plugin_path}")
            
        except Exception as e:
            logger.error(f"Failed to load plugin {plugin_path}: {e}")
            raise
    
    def worker_loop(self):
        """Main worker loop - request and execute tasks until done"""
        logger.info("Worker starting")
        
        while True:
            try:
                # Request task from coordinator
                task_dict = self.coordinator.request_task({})
                task = Task.from_dict(task_dict)
                
                if task.type == TaskType.EXIT:
                    logger.info("Received exit signal, worker shutting down")
                    break
                elif task.type == TaskType.WAIT:
                    logger.info("No tasks available, waiting...")
                    time.sleep(1)
                    continue
                elif task.type == TaskType.MAP:
                    success = self._execute_map_task(task)
                elif task.type == TaskType.REDUCE:
                    success = self._execute_reduce_task(task)
                else:
                    logger.error(f"Unknown task type: {task.type}")
                    continue
                
                # Report completion
                complete_req = TaskCompleteRequest(task.type, task.task_id, success)
                self.coordinator.task_complete(complete_req.to_dict())
                
            except Exception as e:
                logger.error(f"Error in worker loop: {e}")
                time.sleep(1)
        
        logger.info("Worker finished")
    
    def _execute_map_task(self, task: Task) -> bool:
        """Execute a map task"""
        try:
            logger.info(f"Executing map task {task.task_id}")
            
            # Read input file
            input_file = task.input_files[0]
            with open(input_file, 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Execute map function
            key_values = self.map_func(input_file, content)
            
            # Partition intermediate key-value pairs
            intermediate_kvs = defaultdict(list)
            for kv in key_values:
                reduce_task = ihash(kv.key) % task.n_reduce
                intermediate_kvs[reduce_task].append(kv)
            
            # Write intermediate files
            for reduce_task, kvs in intermediate_kvs.items():
                filename = intermediate_filename(task.task_id, reduce_task)
                temp_file = temp_filename(f"mr-{task.task_id}-{reduce_task}-")
                
                try:
                    with open(temp_file, 'w', encoding='utf-8') as f:
                        for kv in kvs:
                            json.dump(kv.to_dict(), f)
                            f.write('\n')
                    
                    # Atomic rename
                    os.rename(temp_file, filename)
                    
                except Exception as e:
                    # Clean up temp file on error
                    if os.path.exists(temp_file):
                        os.remove(temp_file)
                    raise e
            
            logger.info(f"Map task {task.task_id} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Map task {task.task_id} failed: {e}")
            return False
    
    def _execute_reduce_task(self, task: Task) -> bool:
        """Execute a reduce task"""
        try:
            logger.info(f"Executing reduce task {task.task_id}")
            
            # Read all intermediate files
            intermediate_kvs = []
            for input_file in task.input_files:
                if os.path.exists(input_file):
                    with open(input_file, 'r', encoding='utf-8') as f:
                        for line in f:
                            line = line.strip()
                            if line:
                                kv_dict = json.loads(line)
                                intermediate_kvs.append(KeyValue.from_dict(kv_dict))
            
            # Group by key
            grouped_kvs = defaultdict(list)
            for kv in intermediate_kvs:
                grouped_kvs[kv.key].append(kv.value)
            
            # Sort by key (as required by MapReduce specification)
            sorted_keys = sorted(grouped_kvs.keys())
            
            # Apply reduce function and write output
            temp_file = temp_filename(f"mr-out-{task.task_id}-")
            
            try:
                with open(temp_file, 'w', encoding='utf-8') as f:
                    for key in sorted_keys:
                        values = grouped_kvs[key]
                        result = self.reduce_func(key, values)
                        # Write in the format expected by the test (key value)
                        f.write(f"{key} {result}\n")
                
                # Atomic rename
                os.rename(temp_file, task.output_file)
                
            except Exception as e:
                # Clean up temp file on error
                if os.path.exists(temp_file):
                    os.remove(temp_file)
                raise e
            
            logger.info(f"Reduce task {task.task_id} completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Reduce task {task.task_id} failed: {e}")
            return False


def worker_main(coordinator_address: str, plugin_path: str):
    """Main function for worker process"""
    worker = Worker(coordinator_address)
    worker.load_plugin(plugin_path)
    worker.worker_loop()

