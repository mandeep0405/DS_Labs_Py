"""
MapReduce Coordinator Implementation

The coordinator is responsible for:
1. Distributing map and reduce tasks to workers
2. Tracking task progress and handling failures
3. Managing the overall MapReduce job lifecycle
"""

import threading
import time
import socket
import json
from xmlrpc.server import SimpleXMLRPCServer
from typing import List, Dict, Optional
import os
import logging

from mapreduce_rpc import (
    TaskType, TaskStatus, TaskInfo, Task, TaskRequest, 
    TaskCompleteRequest, TaskCompleteResponse, intermediate_filename, output_filename
)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class Coordinator:
    """
    Coordinator manages the MapReduce job execution.
    
    Architecture:
    - Maintains queues of map and reduce tasks
    - Assigns tasks to workers via RPC
    - Monitors task completion and handles timeouts
    - Ensures fault tolerance by reassigning failed tasks
    """
    
    def __init__(self, input_files: List[str], n_reduce: int):
        self.input_files = input_files
        self.n_reduce = n_reduce
        self.n_map = len(input_files)
        
        # Task management
        self.map_tasks: Dict[int, TaskInfo] = {}
        self.reduce_tasks: Dict[int, TaskInfo] = {}
        
        # State tracking
        self.phase = "map"  # "map", "reduce", "done"
        self.lock = threading.RLock()
        
        # Timeout handling
        self.task_timeout = 10  # 10 seconds as specified in lab
        
        # Initialize tasks
        self._init_map_tasks()
        self._init_reduce_tasks()
        
        logger.info(f"Coordinator initialized with {self.n_map} map tasks and {self.n_reduce} reduce tasks")
    
    def _init_map_tasks(self):
        """Initialize map tasks from input files"""
        for i, input_file in enumerate(self.input_files):
            self.map_tasks[i] = TaskInfo(
                task_type=TaskType.MAP,
                task_id=i,
                input_files=[input_file],
                n_reduce=self.n_reduce,
                n_map=self.n_map
            )
    
    def _init_reduce_tasks(self):
        """Initialize reduce tasks"""
        for i in range(self.n_reduce):
            # Reduce task input files are intermediate files from all map tasks
            input_files = [intermediate_filename(map_id, i) for map_id in range(self.n_map)]
            
            self.reduce_tasks[i] = TaskInfo(
                task_type=TaskType.REDUCE,
                task_id=i,
                input_files=input_files,
                output_file=output_filename(i),
                n_reduce=self.n_reduce,
                n_map=self.n_map
            )
    
    def request_task(self, request: dict) -> dict:
        """
        RPC handler for workers requesting tasks
        
        Returns:
        - Map task if map phase is active and tasks available
        - Reduce task if reduce phase is active and tasks available  
        - Wait task if all tasks assigned but not complete
        - Exit task if job is done
        """
        with self.lock:
            # Check for timeout tasks first
            self._check_timeouts()
            
            if self.phase == "map":
                task = self._assign_map_task()
                if task:
                    return task.to_dict()
                elif self._all_map_tasks_complete():
                    self.phase = "reduce"
                    logger.info("Map phase complete, starting reduce phase")
                    return self._get_reduce_task_or_wait()
                else:
                    return Task(TaskType.WAIT, -1, []).to_dict()
                    
            elif self.phase == "reduce":
                return self._get_reduce_task_or_wait()
                
            else:  # done
                return Task(TaskType.EXIT, -1, []).to_dict()
    
    def _get_reduce_task_or_wait(self) -> dict:
        """Get reduce task or wait/exit task"""
        task = self._assign_reduce_task()
        if task:
            return task.to_dict()
        elif self._all_reduce_tasks_complete():
            self.phase = "done"
            logger.info("Reduce phase complete, job finished")
            return Task(TaskType.EXIT, -1, []).to_dict()
        else:
            return Task(TaskType.WAIT, -1, []).to_dict()
    
    def _assign_map_task(self) -> Optional[Task]:
        """Assign an idle map task"""
        for task_info in self.map_tasks.values():
            if task_info.status == TaskStatus.IDLE:
                task_info.status = TaskStatus.IN_PROGRESS
                task_info.assigned_time = time.time()
                logger.info(f"Assigned map task {task_info.task_id}")
                return task_info.to_task()
        return None
    
    def _assign_reduce_task(self) -> Optional[Task]:
        """Assign an idle reduce task"""
        for task_info in self.reduce_tasks.values():
            if task_info.status == TaskStatus.IDLE:
                task_info.status = TaskStatus.IN_PROGRESS
                task_info.assigned_time = time.time()
                logger.info(f"Assigned reduce task {task_info.task_id}")
                return task_info.to_task()
        return None
    
    def _check_timeouts(self):
        """Check for timed out tasks and reset them"""
        current_time = time.time()
        
        # Check map tasks
        for task_info in self.map_tasks.values():
            if (task_info.status == TaskStatus.IN_PROGRESS and 
                current_time - task_info.assigned_time > self.task_timeout):
                logger.warning(f"Map task {task_info.task_id} timed out, reassigning")
                task_info.status = TaskStatus.IDLE
                task_info.assigned_time = 0
        
        # Check reduce tasks  
        for task_info in self.reduce_tasks.values():
            if (task_info.status == TaskStatus.IN_PROGRESS and
                current_time - task_info.assigned_time > self.task_timeout):
                logger.warning(f"Reduce task {task_info.task_id} timed out, reassigning")
                task_info.status = TaskStatus.IDLE
                task_info.assigned_time = 0
    
    def task_complete(self, request: dict) -> dict:
        """
        RPC handler for workers reporting task completion
        """
        req = TaskCompleteRequest.from_dict(request)
        
        with self.lock:
            if req.task_type == TaskType.MAP:
                if req.task_id in self.map_tasks:
                    task_info = self.map_tasks[req.task_id]
                    if req.success:
                        task_info.status = TaskStatus.COMPLETED
                        logger.info(f"Map task {req.task_id} completed successfully")
                    else:
                        task_info.status = TaskStatus.IDLE
                        task_info.assigned_time = 0
                        logger.warning(f"Map task {req.task_id} failed, will reassign")
                        
            elif req.task_type == TaskType.REDUCE:
                if req.task_id in self.reduce_tasks:
                    task_info = self.reduce_tasks[req.task_id]
                    if req.success:
                        task_info.status = TaskStatus.COMPLETED
                        logger.info(f"Reduce task {req.task_id} completed successfully")
                    else:
                        task_info.status = TaskStatus.IDLE
                        task_info.assigned_time = 0
                        logger.warning(f"Reduce task {req.task_id} failed, will reassign")
        
        return TaskCompleteResponse().to_dict()
    
    def _all_map_tasks_complete(self) -> bool:
        """Check if all map tasks are completed"""
        return all(task.status == TaskStatus.COMPLETED for task in self.map_tasks.values())
    
    def _all_reduce_tasks_complete(self) -> bool:
        """Check if all reduce tasks are completed"""
        return all(task.status == TaskStatus.COMPLETED for task in self.reduce_tasks.values())
    
    def done(self) -> bool:
        """Check if the entire MapReduce job is complete"""
        with self.lock:
            return self.phase == "done"
    
    def server(self, port: int = 8000):
        """Start the RPC server"""
        server = SimpleXMLRPCServer(("localhost", port), allow_none=True, logRequests=False)
        server.register_function(self.request_task, "request_task")
        server.register_function(self.task_complete, "task_complete")
        
        logger.info(f"Coordinator server starting on port {port}")
        
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            logger.info("Coordinator server shutting down")
            server.shutdown()


def make_coordinator(input_files: List[str], n_reduce: int) -> Coordinator:
    """Factory function to create a coordinator"""
    return Coordinator(input_files, n_reduce)


