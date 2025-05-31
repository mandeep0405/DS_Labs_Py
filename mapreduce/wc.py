"""
Word Count MapReduce Application

This implements the classic word count example from the MapReduce paper.
It demonstrates how to write Map and Reduce functions for a MapReduce application.

The map function splits text into words and emits (word, 1) pairs.
The reduce function sums up the counts for each word.
"""

import re
import unicodedata
from typing import List, Iterator
from mapreduce_rpc import KeyValue


def Map(filename: str, contents: str) -> List[KeyValue]:
    """
    Map function for word count
    
    Args:
        filename: Name of the input file
        contents: Contents of the input file
    
    Returns:
        List of KeyValue pairs where key is a word and value is "1"
    """
    # Split contents into words
    # Use regex to find sequences of letters (similar to Go's unicode.IsLetter)
    words = re.findall(r'[a-zA-Z]+', contents.lower())
    
    # Emit (word, "1") for each word
    result = []
    for word in words:
        if word:  # Skip empty strings
            result.append(KeyValue(word, "1"))
    
    return result


def Reduce(key: str, values: List[str]) -> str:
    """
    Reduce function for word count
    
    Args:
        key: The word
        values: List of counts (all "1" in this case)
    
    Returns:
        Total count as a string
    """
    # Sum up all the counts
    total = sum(int(v) for v in values)
    return str(total)


# Test the functions
if __name__ == "__main__":
    # Test with sample data
    test_content = "Hello world! This is a test. Hello again world."
    
    # Test Map function
    map_result = Map("test.txt", test_content)
    print("Map output:")
    for kv in map_result:
        print(f"  {kv.key}: {kv.value}")
    
    # Group by key (simulate what the MapReduce framework does)
    from collections import defaultdict
    grouped = defaultdict(list)
    for kv in map_result:
        grouped[kv.key].append(kv.value)
    
    # Test Reduce function
    print("\nReduce output:")
    for key, values in sorted(grouped.items()):
        result = Reduce(key, values)
        print(f"  {key}: {result}")