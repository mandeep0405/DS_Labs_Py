#!/usr/bin/env python3
"""
Updated Main Test File for Consistent Hashing with Separate Node Storage
"""

from ring import ConsistentHashRing, Node

# Test data sets
test_data = {
    'user:1001': {'name': 'Alice', 'email': 'alice@example.com'},
    'user:1002': {'name': 'Bob', 'email': 'bob@example.com'},
    'user:1003': {'name': 'Charlie', 'email': 'charlie@example.com'},
    'product:501': {'name': 'Laptop', 'price': 999.99},
    'product:502': {'name': 'Mouse', 'price': 29.99},
    'order:2001': {'user_id': 1001, 'product_id': 501, 'quantity': 1}
}

# Generate larger test dataset
DUMMY_DATA = {f"key{i}": f"value{i}" for i in range(10000)}


def test_individual_node():
    """Test the Node class independently."""
    print("=== Testing Individual Node ===")
    
    # Create a node
    node = Node("TestNode")
    print(f"Created: {node}")
    
    # Test basic operations
    success = node.put("test_key", "test_value")
    print(f"Put operation: {'Success' if success else 'Failed'}")
    
    value = node.get("test_key")
    print(f"Get operation: {value}")
    
    # Test multiple keys
    for i in range(5):
        node.put(f"key_{i}", f"value_{i}")
    
    print(f"Node after adding 5 keys: {node}")
    print(f"All keys: {node.get_keys()}")
    print(f"Node stats: {node.get_stats()}")
    
    # Test deletion
    deleted = node.delete("key_2")
    print(f"Deleted key_2: {'Success' if deleted else 'Failed'}")
    print(f"Keys after deletion: {node.get_keys()}")
    print()


def demo_consistent_hashing():
    """Demonstrate the updated consistent hashing implementation."""
    print("=== Updated Consistent Hash Ring Demo ===\n")
    
    # Create hash ring
    ring = ConsistentHashRing(virtual_nodes=100)
    print(f"Created hash ring: {ring}\n")
    
    # Add initial nodes
    print("1. Adding initial nodes:")
    for node in ['NodeA', 'NodeB', 'NodeC']:
        result = ring.add_node(node)
        print(f"   Added {node}: {result['message']}")
    print()
    
    # Add test data
    print("2. Adding test data:")
    # Add small test data first
    for key, value in test_data.items():
        result = ring.put(key, value)
        print(f"   Stored {key} on {result['node']}")
    
    # Add larger dataset
    print(f"   Adding {len(DUMMY_DATA)} additional keys...")
    for key, value in DUMMY_DATA.items():
        ring.put(key, value)
    print()
    
    # Show distribution
    print("3. Current data distribution:")
    distribution = ring.get_node_distribution()
    for node_id, info in distribution.items():
        print(f"   {node_id}: {info['key_count']} keys, {info['virtual_nodes']} vnodes")
        # Show some sample keys for smaller dataset
        if info['key_count'] <= 10:
            print(f"      Keys: {info['keys']}")
    
    # Show node statistics
    print("4. Individual node statistics:")
    node_stats = ring.get_node_stats()
    for node_id, stats in node_stats.items():
        print(f"   {node_id}:")
        print(f"      Keys stored: {stats['keys_stored']}")
        print(f"      Get requests: {stats['get_requests']}")
        print(f"      Put requests: {stats['put_requests']}")
        print(f"      Delete requests: {stats['delete_requests']}")
    print()
    
    # Add a new node and show data movement
    print("5. Adding new node:")
    result = ring.add_node('NodeD')
    print(f"   {result['message']}")
    print(f"   Data moved: {result['data_moved']} keys")
    print()
    
    # Show new distribution
    print("6. Distribution after adding NodeD:")
    distribution = ring.get_node_distribution()
    for node_id, info in distribution.items():
        print(f"   {node_id}: {info['key_count']} keys")
    
    # Remove a node and show data movement
    print("7. Removing NodeB:")
    result = ring.remove_node('NodeB')
    print(f"   {result['message']}")
    print(f"   Data moved: {result['data_moved']} keys")
    print()
    
    # Final distribution
    print("8. Final distribution:")
    distribution = ring.get_node_distribution()
    for node_id, info in distribution.items():
        print(f"   {node_id}: {info['key_count']} keys")
    
    # Test data retrieval
    print("9. Testing data retrieval:")
    test_keys = ['user:1001', 'key100', 'nonexistent_key']
    for key in test_keys:
        result = ring.get(key)
        if result['success']:
            print(f"   {key}: Found on {result['node']}")
        else:
            print(f"   {key}: {result['message']}")
    print()
    
    # Test data modification
    print("10. Testing data modification:")
    # Update existing key
    result = ring.put('user:1001', {'name': 'Alice Updated', 'email': 'alice.new@example.com'})
    print(f"    Updated user:1001 on {result['node']}")
    
    # Verify update
    result = ring.get('user:1001')
    if result['success']:
        print(f"    Retrieved updated value: {result['value']}")
    
    # Test deletion
    result = ring.delete('product:502')
    print(f"    Deleted product:502: {'Success' if result['success'] else 'Failed'}")
    print()
    
    # Show final stats
    print("11. Final ring statistics:")
    info = ring.get_ring_info()
    print(f"    Total nodes: {info['total_nodes']}")
    print(f"    Total keys: {info['total_keys']}")
    print(f"    Virtual nodes: {info['total_virtual_nodes']}")
    print(f"    Node additions: {info['stats']['node_additions']}")
    print(f"    Node removals: {info['stats']['node_removals']}")
    print(f"    Data movements: {info['stats']['data_movements']}")


if __name__ == "__main__":
    # Run all tests
    test_individual_node()
    demo_consistent_hashing()
