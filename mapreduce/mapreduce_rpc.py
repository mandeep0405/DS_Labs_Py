"""
MapReduce Implementation in Python
Based on MIT 6.5840 Lab 1 and the original MapReduce paper by Dean & Ghemawat

This module implements the RPC structures and core data types for the MapReduce system.
"""
import tempfile
import os
import json
import time
from enum import Enum
from dataclasses import dataclass
from typing import List, Dict, Any, Optional, Tuple
import threading
import hashlib


class TaskType(Enum):
    MAP = "map"
    REDUCE = "reduce"
    WAIT = "wait"
    EXIT = "exit"


class TaskStatus(Enum):
    IDLE = "idle"
    IN_PROGRESS = "in_progress" 
    COMPLETED = "completed"


@dataclass
class KeyValue:
    """Key-value pair used throughout MapReduce"""
    key: str
    value: str
    
    def to_dict(self):
        return {"Key": self.key, "Value": self.value}
    
    @classmethod
    def from_dict(cls, d):
        return cls(d["Key"], d["Value"])


@dataclass
class TaskRequest:
    """RPC request for asking coordinator for a task"""
    pass


@dataclass
class Task:
    """Task assigned to worker by coordinator"""
    type: TaskType
    task_id: int
    input_files: List[str]
    output_file: str = ""
    n_reduce: int = 0
    n_map: int = 0
    
    def to_dict(self):
        return {
            "Type": self.type.value,
            "TaskId": self.task_id,
            "InputFiles": self.input_files,
            "OutputFile": self.output_file,
            "NReduce": self.n_reduce,
            "NMap": self.n_map
        }
    
    @classmethod
    def from_dict(cls, d):
        return cls(
            type=TaskType(d["Type"]),
            task_id=d["TaskId"],
            input_files=d["InputFiles"],
            output_file=d.get("OutputFile", ""),
            n_reduce=d.get("NReduce", 0),
            n_map=d.get("NMap", 0)
        )


@dataclass 
class TaskCompleteRequest:
    """RPC request for notifying coordinator of task completion"""
    task_type: TaskType
    task_id: int
    success: bool
    
    def to_dict(self):
        return {
            "TaskType": self.task_type.value,
            "TaskId": self.task_id,
            "Success": self.success
        }
    
    @classmethod
    def from_dict(cls, d):
        return cls(
            task_type=TaskType(d["TaskType"]),
            task_id=d["TaskId"],
            success=d["Success"]
        )


@dataclass
class TaskCompleteResponse:
    """RPC response for task completion notification"""
    success: bool = True
    
    def to_dict(self):
        return {"Success": self.success}
    
    @classmethod 
    def from_dict(cls, d):
        return cls(success=d.get("Success", True))


class TaskInfo:
    """Internal task tracking information for coordinator"""
    def __init__(
            self, 
            task_type: TaskType, 
            task_id: int, 
            input_files: List[str], 
            output_file: str = "", 
            n_reduce: int = 0,
            n_map: int = 0
        ):
        self.task_type = task_type
        self.task_id = task_id
        self.input_files = input_files
        self.output_file = output_file
        self.n_reduce = n_reduce
        self.n_map = n_map
        self.status = TaskStatus.IDLE
        self.assigned_time = 0
        self.worker_id = ""
        
    def to_task(self) -> Task:
        """Convert to Task for RPC"""
        return Task(
            type=self.task_type,
            task_id=self.task_id,
            input_files=self.input_files,
            output_file=self.output_file,
            n_reduce=self.n_reduce,
            n_map=self.n_map
        )


def ihash(key: str) -> int:
    """Hash function for determining reduce task assignment"""
    return int(hashlib.md5(key.encode()).hexdigest(), 16) & 0x7fffffff


def intermediate_filename(map_task: int, reduce_task: int) -> str:
    """Generate intermediate filename following mr-X-Y convention"""
    return f"mr-{map_task}-{reduce_task}"


def output_filename(reduce_task: int) -> str:
    """Generate output filename"""
    return f"mr-out-{reduce_task}"


def temp_filename(prefix: str) -> str:
    """Generate temporary filename"""
    fd, path = tempfile.mkstemp(prefix=prefix)
    os.close(fd)
    return path