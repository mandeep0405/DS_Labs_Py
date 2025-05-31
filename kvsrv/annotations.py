#!/usr/bin/env python3
"""
Test Annotations System
Matching MIT 6.5840 Lab 2 tester1 annotations functionality

This module provides test annotations for visualization and debugging,
similar to the Go lab's annotation system.
"""

import threading
import time
from typing import List, Dict, Any, Optional
from dataclasses import dataclass
from enum import Enum

class AnnotationType(Enum):
    """Types of annotations"""
    INFO = "info"
    SUCCESS = "success" 
    FAILURE = "failure"
    WARNING = "warning"
    PARTITION = "partition"
    CHECKER_BEGIN = "checker_begin"
    CHECKER_SUCCESS = "checker_success"
    CHECKER_FAILURE = "checker_failure"

@dataclass
class Annotation:
    """Single annotation entry"""
    type: AnnotationType
    timestamp: float
    description: str
    details: str = ""
    client_id: Optional[int] = None
    thread_id: Optional[int] = None

class AnnotationSystem:
    """
    Global annotation system for test visualization
    Thread-safe collection of test events and annotations
    """
    
    def __init__(self):
        """Initialize annotation system"""
        self.annotations: List[Annotation] = []
        self.mu = threading.Lock()
        self.finalized = False
        
    def add_annotation(self, ann_type: AnnotationType, description: str, 
                      details: str = "", client_id: Optional[int] = None):
        """Add annotation to the system"""
        with self.mu:
            if self.finalized:
                return
                
            annotation = Annotation(
                type=ann_type,
                timestamp=time.time(),
                description=description,
                details=details,
                client_id=client_id,
                thread_id=threading.get_ident()
            )
            self.annotations.append(annotation)
            
    def get_annotations(self) -> List[Annotation]:
        """Get all annotations (returns copy)"""
        with self.mu:
            return self.annotations.copy()
            
    def finalize(self, final_message: str) -> List[Annotation]:
        """Finalize annotations and return final list"""
        with self.mu:
            if not self.finalized:
                self.add_annotation(AnnotationType.INFO, final_message)
                self.finalized = True
            return self.annotations.copy()
            
    def clear(self):
        """Clear all annotations"""
        with self.mu:
            self.annotations.clear()
            self.finalized = False

# Global annotation system instance
_annotation_system = AnnotationSystem()

def AnnotateInfo(description: str, details: str = ""):
    """Add info annotation - matches Go lab AnnotateInfo"""
    _annotation_system.add_annotation(AnnotationType.INFO, description, details)

def AnnotateSuccess(description: str, details: str = ""):
    """Add success annotation"""
    _annotation_system.add_annotation(AnnotationType.SUCCESS, description, details)

def AnnotateFailure(description: str, details: str = ""):
    """Add failure annotation"""
    _annotation_system.add_annotation(AnnotationType.FAILURE, description, details)

def AnnotateWarning(description: str, details: str = ""):
    """Add warning annotation"""
    _annotation_system.add_annotation(AnnotationType.WARNING, description, details)

def AnnotateTwoPartitions(partition_a: List[int], partition_b: List[int]):
    """Annotate network partition - matches Go lab AnnotateTwoPartitions"""
    description = f"Network partition created"
    details = f"Partition A: {partition_a}, Partition B: {partition_b}"
    _annotation_system.add_annotation(AnnotationType.PARTITION, description, details)

def AnnotateCheckerBegin(description: str):
    """Begin checker annotation - matches Go lab AnnotateCheckerBegin"""
    _annotation_system.add_annotation(AnnotationType.CHECKER_BEGIN, description)

def AnnotateCheckerSuccess(description: str, details: str = ""):
    """Checker success annotation - matches Go lab AnnotateCheckerSuccess"""
    _annotation_system.add_annotation(AnnotationType.CHECKER_SUCCESS, description, details)

def AnnotateCheckerFailure(description: str, details: str = ""):
    """Checker failure annotation - matches Go lab AnnotateCheckerFailure"""
    _annotation_system.add_annotation(AnnotationType.CHECKER_FAILURE, description, details)

def FinalizeAnnotations(final_message: str) -> List[Annotation]:
    """Finalize and retrieve all annotations - matches Go lab FinalizeAnnotations"""
    return _annotation_system.finalize(final_message)

def RetrieveAnnotations() -> List[Annotation]:
    """Retrieve current annotations - matches Go lab RetrieveAnnotations"""
    return _annotation_system.get_annotations()

def GetAnnotationFinalized() -> bool:
    """Check if annotations have been finalized"""
    return _annotation_system.finalized

def ClearAnnotations():
    """Clear all annotations (for testing)"""
    _annotation_system.clear()

def AnnotationsToHTML(annotations: List[Annotation]) -> str:
    """Convert annotations to HTML for visualization"""
    html = """
    <html>
    <head>
        <title>Test Annotations</title>
        <style>
            body { font-family: Arial, sans-serif; margin: 20px; }
            .annotation { margin: 10px 0; padding: 10px; border-left: 4px solid #ccc; }
            .info { border-color: #2196F3; background-color: #E3F2FD; }
            .success { border-color: #4CAF50; background-color: #E8F5E8; }
            .failure { border-color: #F44336; background-color: #FFEBEE; }
            .warning { border-color: #FF9800; background-color: #FFF3E0; }
            .partition { border-color: #9C27B0; background-color: #F3E5F5; }
            .checker_begin { border-color: #607D8B; background-color: #ECEFF1; }
            .checker_success { border-color: #4CAF50; background-color: #E8F5E8; }
            .checker_failure { border-color: #F44336; background-color: #FFEBEE; }
            .timestamp { color: #666; font-size: 0.9em; }
            .details { margin-top: 5px; font-style: italic; color: #555; }
        </style>
    </head>
    <body>
        <h1>Test Execution Annotations</h1>
    """
    
    for ann in annotations:
        css_class = ann.type.value
        html += f"""
        <div class="annotation {css_class}">
            <div class="timestamp">{time.strftime('%H:%M:%S', time.localtime(ann.timestamp))}</div>
            <div class="description"><strong>{ann.description}</strong></div>
        """
        
        if ann.details:
            html += f'<div class="details">{ann.details}</div>'
            
        if ann.client_id is not None:
            html += f'<div class="details">Client ID: {ann.client_id}</div>'
            
        html += "</div>"
        
    html += """
    </body>
    </html>
    """
    
    return html

def SaveAnnotationsToFile(annotations: List[Annotation], filename: str):
    """Save annotations to HTML file"""
    try:
        html = AnnotationsToHTML(annotations)
        with open(filename, 'w') as f:
            f.write(html)
        return True
    except Exception as e:
        print(f"Failed to save annotations to {filename}: {e}")
        return False

# Test the annotation system
def test_annotations():
    """Test the annotation system"""
    print("Testing Annotation System...")
    
    # Clear any existing annotations
    ClearAnnotations()
    
    # Add various types of annotations
    AnnotateInfo("Starting test", "Initial setup")
    AnnotateCheckerBegin("Checking linearizability")
    AnnotateSuccess("Operation completed", "Put operation successful")
    AnnotateWarning("Potential issue", "High latency detected")
    AnnotateTwoPartitions([0, 1], [2, 3])
    AnnotateCheckerSuccess("Linearizability verified", "All operations valid")
    
    # Get annotations
    annotations = RetrieveAnnotations()
    assert len(annotations) == 6
    print(f"✓ Added {len(annotations)} annotations")
    
    # Test types
    types_found = {ann.type for ann in annotations}
    expected_types = {
        AnnotationType.INFO, AnnotationType.CHECKER_BEGIN, AnnotationType.SUCCESS,
        AnnotationType.WARNING, AnnotationType.PARTITION, AnnotationType.CHECKER_SUCCESS
    }
    assert types_found == expected_types
    print("✓ All annotation types work")
    
    # Test finalization
    final_annotations = FinalizeAnnotations("Test completed")
    assert len(final_annotations) == 7  # 6 + 1 final
    assert GetAnnotationFinalized()
    print("✓ Finalization works")
    
    # Test HTML generation
    html = AnnotationsToHTML(final_annotations)
    assert "<html>" in html
    assert "Test Execution Annotations" in html
    assert "Starting test" in html
    print("✓ HTML generation works")
    
    # Test file saving
    import tempfile
    with tempfile.NamedTemporaryFile(mode='w', suffix='.html', delete=False) as f:
        success = SaveAnnotationsToFile(final_annotations, f.name)
        assert success
        print(f"✓ File saving works: {f.name}")
    
    print("Annotation system test completed successfully!")

if __name__ == "__main__":
    test_annotations()