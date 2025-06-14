#!/usr/bin/env python3
"""
Consistent Hashing Implementation 
"""
import time
import hashlib
import threading
from typing import Dict, List, Set, Optional, Tuple, Any


class Node:
    """
    Individual node that stores its own data locally.
    
    This represents a physical server/node in the distributed system.
    Each node manages its own storage independently.
    """
    
    def __init__(self, node_id: str):
        """
        Initialize a node.
        
        Args:
            node_id: Unique identifier for this node
        """
        self.node_id = node_id
        self.local_storage: Dict[str, Any] = {}
        self.lock = threading.RLock()
        self.stats = {
            'keys_stored': 0,
            'get_requests': 0,
            'put_requests': 0,
            'delete_requests': 0,
            'last_access_time': None
        }
        
    def put(self, key: str, value: Any) -> bool:
        """
        Store data locally on this node.
        
        Args:
            key: The key to store  
            value: The value to associate with the key
            
        Returns:
            True if successful
        """
        with self.lock:
            is_new_key = key not in self.local_storage
            self.local_storage[key] = value
            
            self.stats['put_requests'] += 1
            if is_new_key:
                self.stats['keys_stored'] += 1
            self.stats['last_access_time'] = time.time()
            
            return True
    
    def get(self, key: str) -> Optional[Any]:
        """
        Retrieve data from this node.
        
        Args:
            key: The key to look up
            
        Returns:
            The value if found, None otherwise
        """
        with self.lock:
            self.stats['get_requests'] += 1
            self.stats['last_access_time'] = time.time()
            
            return self.local_storage.get(key)
    
    def delete(self, key: str) -> bool:
        """
        Delete data from this node.
        
        Args:
            key: The key to delete
            
        Returns:
            True if key existed and was deleted, False otherwise
        """
        with self.lock:
            self.stats['delete_requests'] += 1
            self.stats['last_access_time'] = time.time()
            
            if key in self.local_storage:
                del self.local_storage[key]
                self.stats['keys_stored'] -= 1
                return True
            return False
    
    def get_keys(self) -> List[str]:
        """
        Get all keys stored on this node.
        
        Returns:
            List of all keys
        """
        with self.lock:
            return list(self.local_storage.keys())
    
    def get_key_count(self) -> int:
        """Get the number of keys stored on this node."""
        with self.lock:
            return len(self.local_storage)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get node statistics."""
        with self.lock:
            return {
                'node_id': self.node_id,
                'keys_stored': len(self.local_storage),
                'get_requests': self.stats['get_requests'],
                'put_requests': self.stats['put_requests'],
                'delete_requests': self.stats['delete_requests'],
                'last_access_time': self.stats['last_access_time']
            }
    
    def clear(self):
        """Clear all data from this node."""
        with self.lock:
            self.local_storage.clear()
            self.stats['keys_stored'] = 0
    
    def __str__(self) -> str:
        """String representation of the node."""
        return f"Node({self.node_id}, keys={self.get_key_count()})"


class ConsistentHashRing:
    """
    Consistent hash ring that routes requests to individual nodes.
    
    This class manages the ring topology and routes requests to the appropriate
    nodes, supports functionality to add/delete nodes.
    """
    
    def __init__(self, virtual_nodes: int = 150, hash_function: str = 'md5'):
        """
        Initialize the consistent hash ring.
        
        Args:
            virtual_nodes: Number of virtual nodes per physical node
            hash_function: Hash function to use ('md5', 'sha1', 'sha256')
        """
        self.virtual_nodes = virtual_nodes
        self.hash_function = hash_function
        
        # Core data structures
        self.ring: Dict[int, str] = {}  # hash_position -> node_id
        self.nodes: Dict[str, Node] = {}  # node_id -> Node object
        
        # Statistics
        self.stats = {
            'total_keys': 0,
            'data_movements': 0,
            'node_additions': 0,
            'node_removals': 0,
            'last_operation_time': None
        }
        
        # Thread safety
        self.lock = threading.RLock()
        
        # Sorted ring positions for faster lookups
        self._sorted_positions: List[int] = []
    
    def _hash(self, key: str) -> int:
        """Generate hash value for a key."""
        hash_obj = hashlib.new(self.hash_function)
        hash_obj.update(key.encode('utf-8'))
        return int(hash_obj.hexdigest(), 16)
    
    def _update_sorted_positions(self):
        """Update the sorted list of ring positions for efficient lookups."""
        self._sorted_positions = sorted(self.ring.keys())
    
    def _find_node_for_key(self, key: str) -> Optional[str]:
        """
        Find the node responsible for a given key.
        
        Args:
            key: The key to find a node for
            
        Returns:
            Node ID that should handle this key, or None if no nodes exist
        """
        if not self.ring:
            return None
        
        key_hash = self._hash(key)
        
        # Find the first node clockwise from the key's position
        for position in self._sorted_positions:
            if position >= key_hash:
                return self.ring[position]
        
        # Wrap around to the first node
        return self.ring[self._sorted_positions[0]]
    
    def add_node(self, node_id: str) -> Dict[str, Any]:
        """
        Add a new node to the hash ring.
        
        Args:
            node_id: Unique identifier for the node
            
        Returns:
            Dictionary with operation results and statistics
        """
        with self.lock:
            if node_id in self.nodes:
                return {
                    'success': False,
                    'message': f'Node {node_id} already exists',
                    'data_moved': 0
                }
            
            # Create the actual node object
            node = Node(node_id)
            self.nodes[node_id] = node
            
            # Add virtual nodes to the ring
            new_positions = []
            for i in range(self.virtual_nodes):
                virtual_key = f"{node_id}:{i}"
                position = self._hash(virtual_key)
                self.ring[position] = node_id
                new_positions.append(position)
            
            self._update_sorted_positions()
            
            # Redistribute existing data
            moved_keys = self._redistribute_data()
            
            # Update statistics
            self.stats['node_additions'] += 1
            self.stats['data_movements'] += len(moved_keys)
            self.stats['last_operation_time'] = time.time()
            
            return {
                'success': True,
                'message': f'Node {node_id} added successfully',
                'virtual_nodes_added': self.virtual_nodes,
                'new_positions': sorted(new_positions),
                'data_moved': len(moved_keys),
                'moved_keys': moved_keys
            }
    
    def remove_node(self, node_id: str) -> Dict[str, Any]:
        """
        Remove a node from the hash ring.
        
        Args:
            node_id: ID of the node to remove
            
        Returns:
            Dictionary with operation results and statistics
        """
        with self.lock:
            if node_id not in self.nodes:
                return {
                    'success': False,
                    'message': f'Node {node_id} does not exist',
                    'data_moved': 0
                }
            
            # Remove all virtual nodes for this physical node
            positions_to_remove = [pos for pos, node in self.ring.items() if node == node_id]
            for position in positions_to_remove:
                del self.ring[position]
            
            # Get the node to remove
            node_to_remove = self.nodes[node_id]
            
            # Move all data from the removed node to other nodes
            moved_keys = []
            for key in node_to_remove.get_keys():
                value = node_to_remove.get(key)
                moved_keys.append(key)
            
            # Remove the node from our tracking
            del self.nodes[node_id]
            self._update_sorted_positions()
            
            # Redistribute the data from the removed node
            for key in moved_keys:
                # Find new node for this key
                new_node_id = self._find_node_for_key(key)
                if new_node_id:
                    new_node = self.nodes[new_node_id]
                    # Get the value from the removed node and store it on the new node
                    value = node_to_remove.get(key)
                    new_node.put(key, value)
            
            # Update statistics
            self.stats['node_removals'] += 1
            self.stats['data_movements'] += len(moved_keys)
            self.stats['last_operation_time'] = time.time()
            
            return {
                'success': True,
                'message': f'Node {node_id} removed successfully',
                'virtual_nodes_removed': len(positions_to_remove),
                'data_moved': len(moved_keys),
                'moved_keys': moved_keys
            }
    
    def _redistribute_data(self) -> List[str]:
        """
        Redistribute all data according to current ring topology.
        
        Returns:
            List of keys that were moved to different nodes
        """
        moved_keys = []
        
        # Collect all data from all nodes
        all_data = {}
        for node in self.nodes.values():
            for key in node.get_keys():
                all_data[key] = node.get(key)
        
        # Clear all nodes
        for node in self.nodes.values():
            node.clear()
        
        # Redistribute all data to correct nodes
        for key, value in all_data.items():
            correct_node_id = self._find_node_for_key(key)
            if correct_node_id:
                correct_node = self.nodes[correct_node_id]
                correct_node.put(key, value)
                moved_keys.append(key)
        
        return moved_keys
    
    def put(self, key: str, value: Any) -> Dict[str, Any]:
        """
        Store a key-value pair on the appropriate node.
        
        Args:
            key: The key to store
            value: The value to associate with the key
            
        Returns:
            Dictionary with operation results
        """
        with self.lock:
            if not self.nodes:
                return {
                    'success': False,
                    'message': 'No nodes available in the ring',
                    'node': None
                }
            
            node_id = self._find_node_for_key(key)
            if not node_id:
                return {
                    'success': False,
                    'message': 'Unable to find node for key',
                    'node': None
                }
            
            node = self.nodes[node_id]
            is_new_key = node.get(key) is None
            success = node.put(key, value)
            
            if success and is_new_key:
                self.stats['total_keys'] += 1
            
            return {
                'success': success,
                'message': f'Key {key} stored successfully' if success else 'Storage failed',
                'node': node_id,
                'is_new_key': is_new_key
            }
    
    def get(self, key: str) -> Dict[str, Any]:
        """
        Retrieve a value by key from the appropriate node.
        
        Args:
            key: The key to look up
            
        Returns:
            Dictionary with the value and metadata
        """
        with self.lock:
            if not self.nodes:
                return {
                    'success': False,
                    'message': 'No nodes available in the ring',
                    'value': None,
                    'node': None
                }
            
            node_id = self._find_node_for_key(key)
            if not node_id:
                return {
                    'success': False,
                    'message': 'Unable to find node for key',
                    'value': None,
                    'node': None
                }
            
            node = self.nodes[node_id]
            value = node.get(key)
            
            if value is None:
                return {
                    'success': False,
                    'message': f'Key {key} not found',
                    'value': None,
                    'node': node_id
                }
            
            return {
                'success': True,
                'message': 'Key found',
                'value': value,
                'node': node_id
            }
    
    def delete(self, key: str) -> Dict[str, Any]:
        """
        Delete a key-value pair from the appropriate node.
        
        Args:
            key: The key to delete
            
        Returns:
            Dictionary with operation results
        """
        with self.lock:
            if not self.nodes:
                return {
                    'success': False,
                    'message': 'No nodes available in the ring'
                }
            
            node_id = self._find_node_for_key(key)
            if not node_id:
                return {
                    'success': False,
                    'message': 'Unable to find node for key'
                }
            
            node = self.nodes[node_id]
            success = node.delete(key)
            
            if success:
                self.stats['total_keys'] -= 1
            
            return {
                'success': success,
                'message': f'Key {key} deleted successfully' if success else f'Key {key} not found'
            }
    
    def get_node_distribution(self) -> Dict[str, Dict[str, Any]]:
        """
        Get the distribution of data across nodes.
        
        Returns:
            Dictionary with per-node statistics
        """
        with self.lock:
            distribution = {}
            
            for node_id, node in self.nodes.items():
                node_stats = node.get_stats()
                distribution[node_id] = {
                    'keys': node.get_keys(),
                    'key_count': node.get_key_count(),
                    'virtual_nodes': sum(1 for n in self.ring.values() if n == node_id),
                    'stats': node_stats
                }
            
            return distribution
    
    def get_ring_info(self) -> Dict[str, Any]:
        """
        Get detailed information about the hash ring.
        
        Returns:
            Dictionary with ring statistics and configuration
        """
        with self.lock:
            total_keys = sum(node.get_key_count() for node in self.nodes.values())
            
            return {
                'nodes': list(self.nodes.keys()),
                'total_nodes': len(self.nodes),
                'virtual_nodes_per_node': self.virtual_nodes,
                'total_virtual_nodes': len(self.ring),
                'total_keys': total_keys,
                'hash_function': self.hash_function,
                'ring_positions': len(self._sorted_positions),
                'stats': self.stats.copy()
            }
    
    def get_node_stats(self) -> Dict[str, Dict[str, Any]]:
        """Get detailed statistics for all nodes."""
        with self.lock:
            return {node_id: node.get_stats() for node_id, node in self.nodes.items()}
    
    def __str__(self) -> str:
        """String representation of the hash ring."""
        with self.lock:
            total_keys = sum(node.get_key_count() for node in self.nodes.values())
            return (f"ConsistentHashRing(nodes={len(self.nodes)}, "
                   f"keys={total_keys}, "
                   f"virtual_nodes={self.virtual_nodes}, ")
        

