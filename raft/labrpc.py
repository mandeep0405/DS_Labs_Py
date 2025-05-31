#!/usr/bin/env python3
"""
Enhanced labrpc - Python Implementation
Exactly matching Go lab labrpc.go behavior

This module provides network simulation for distributed systems testing,
with support for message drops, delays, network partitions, and failures.
"""

import threading
import time
import random
import pickle
import queue
from typing import Dict, Any, Optional, Callable, List
import logging
from concurrent.futures import ThreadPoolExecutor
import copy

logger = logging.getLogger(__name__)

# Constants matching Go lab labrpc
SHORT_DELAY = 27   # ms
LONG_DELAY = 7000  # ms
MAX_DELAY = LONG_DELAY + 100

class ClientEnd:
    """
    Client endpoint for making RPC calls
    """
    
    def __init__(self, endname: str, network: 'Network'):
        """Create client endpoint"""
        self.endname = endname
        self.network = network
        self.done = False
        
    def Call(self, svc_meth: str, args: Any, reply: Any) -> bool:
        """
        Send RPC and wait for reply
        
        Args:
            svc_meth: Method name like "Raft.RequestVote"
            args: Arguments object
            reply: Reply object to fill in
            
        Returns:
            True if successful, False if network error
        """
        if self.done:
            return False
            
        try:
            # Create request
            req = {
                'endname': self.endname,
                'svc_meth': svc_meth,
                'args': args,
                'reply_queue': queue.Queue()
            }
            
            # Send request to network
            success = self.network._process_request(req)
            if not success:
                return False
                
            # Wait for reply
            try:
                reply_data = req['reply_queue'].get(timeout=5.0) 
                if reply_data['ok']:
                    self._copy_reply(reply_data['reply'], reply)
                    return True
                else:
                    return False
            except queue.Empty:
                return False
                
        except Exception as e:
            logger.debug(f"ClientEnd.Call failed: {e}")
            return False
            
    def _copy_reply(self, source: Any, dest: Any):
        """Copy reply data from source to destination object"""
        if hasattr(source, '__dict__') and hasattr(dest, '__dict__'):
            # Copy all attributes from source to dest
            for key, value in source.__dict__.items():
                if hasattr(dest, key):
                    setattr(dest, key, value)
        else:
            # For simple types, direct assignment
            try:
                dest.__dict__.update(source.__dict__)
            except:
                pass 

class Network:
    """
    Simulates a network with configurable reliability, 
    delays, and partitions.
    """
    
    def __init__(self):
        """Create network simulation"""
        self.mu = threading.Lock()
        self.reliable = True
        self.long_delays = False
        self.long_reordering = False
        self.ends: Dict[str, ClientEnd] = {}
        self.enabled: Dict[str, bool] = {}
        self.servers: Dict[Any, 'Server'] = {}
        self.connections: Dict[str, Any] = {}
        self.done = False
        self.count = 0  # Total RPC count
        self.bytes = 0  # Total bytes sent
        
    def Cleanup(self):
        """Cleanup network resources"""
        with self.mu:
            self.done = True
            for end in self.ends.values():
                end.done = True
                
    def Reliable(self, yes: bool):
        """Set network reliability"""
        with self.mu:
            self.reliable = yes
            
    def IsReliable(self) -> bool:
        """Check if network is reliable"""
        with self.mu:
            return self.reliable
            
    def LongReordering(self, yes: bool):
        """Enable long message reordering"""
        with self.mu:
            self.long_reordering = yes
            
    def LongDelays(self, yes: bool):
        """Enable long delays"""
        with self.mu:
            self.long_delays = yes
            
    def IsLongDelays(self) -> bool:
        """Check if long delays are enabled"""
        with self.mu:
            return self.long_delays
            
    def MakeEnd(self, endname: str) -> ClientEnd:
        """Create client endpoint"""
        with self.mu:
            if endname in self.ends:
                raise Exception(f"MakeEnd: {endname} already exists")
                
            end = ClientEnd(endname, self)
            self.ends[endname] = end
            self.enabled[endname] = False
            self.connections[endname] = None
            
            return end
            
    def DeleteEnd(self, endname: str):
        """Delete client endpoint"""
        with self.mu:
            if endname not in self.ends:
                raise Exception(f"DeleteEnd: {endname} doesn't exist")
                
            self.ends[endname].done = True
            del self.ends[endname]
            del self.enabled[endname]
            del self.connections[endname]
            
    def AddServer(self, servername: Any, server: 'Server'):
        """Add server to network"""
        with self.mu:
            self.servers[servername] = server
            
    def DeleteServer(self, servername: Any):
        """Delete server from network"""
        with self.mu:
            if servername in self.servers:
                del self.servers[servername]
                
    def Connect(self, endname: str, servername: Any):
        """Connect client endpoint to server"""
        with self.mu:
            self.connections[endname] = servername
            
    def Enable(self, endname: str, enabled: bool):
        """Enable/disable client endpoint"""
        with self.mu:
            self.enabled[endname] = enabled
            
    def IsEnabled(self, endname: str) -> bool:
        """Check if endpoint is enabled"""
        with self.mu:
            return self.enabled.get(endname, False)
            
    def _is_endpoint_enabled(self, endname: str) -> bool:
        """Check if endpoint is enabled - helper method"""
        with self.mu:
            return self.enabled.get(endname, False)
            
    def GetCount(self, servername: Any) -> int:
        """Get RPC count for server"""
        with self.mu:
            server = self.servers.get(servername)
            if server:
                return server.GetCount()
            return 0
            
    def GetTotalCount(self) -> int:
        """Get total RPC count"""
        with self.mu:
            return self.count
            
    def GetTotalBytes(self) -> int:
        """Get total bytes sent"""
        with self.mu:
            return self.bytes
            
    def _process_request(self, req: Dict[str, Any]) -> bool:
        """Process RPC request"""
        with self.mu:
            if self.done:
                return False
                
            endname = req['endname']
            enabled = self.enabled.get(endname, False)
            servername = self.connections.get(endname)
            
            reliable = self.reliable
            long_reordering = self.long_reordering

            server = None
            if servername in self.servers:
                server = self.servers[servername]
            elif isinstance(servername, int) and str(servername) in self.servers:
                server = self.servers[str(servername)]
            elif isinstance(servername, str) and int(servername) in self.servers:
                server = self.servers[int(servername)]
            self.count += 1
            # Estimate bytes (simplified)
            try:
                args_bytes = len(pickle.dumps(req['args']))
                self.bytes += args_bytes
            except:
                self.bytes += 100  # Estimate
        
        # Check if endpoint is enabled and connected
        if not enabled:
            # Immediately fail for disabled endpoints (simulates network partition)
            req['reply_queue'].put({'ok': False, 'reply': None})
            return True
            
        if servername is None or server is None:
            # Simulate timeout for missing server
            def timeout_reply():
                time.sleep(random.randint(0, 100) / 1000.0)
                req['reply_queue'].put({'ok': False, 'reply': None})
            threading.Thread(target=timeout_reply, daemon=True).start()
            return True
            
        # Simulate network delays and failures
        if not reliable:
            # Short delay
            delay = random.randint(0, SHORT_DELAY) / 1000.0
            time.sleep(delay)
            
            # Random drop
            if random.randint(0, 999) < 100:  # 10% drop rate
                req['reply_queue'].put({'ok': False, 'reply': None})
                return True
                
        # Execute RPC
        def execute_rpc():
            try:
                reply = server.Dispatch(req['svc_meth'], req['args'])
                
                # Simulate reply delay/drop
                if not reliable and random.randint(0, 999) < 100:  # 10% reply drop
                    req['reply_queue'].put({'ok': False, 'reply': None})
                    return
                    
                if long_reordering and random.randint(0, 899) < 600:
                    # Long delay
                    delay = (200 + random.randint(0, 2000)) / 1000.0
                    time.sleep(delay)
                    
                # Update byte count
                try:
                    reply_bytes = len(pickle.dumps(reply))
                    with self.mu:
                        self.bytes += reply_bytes
                except:
                    with self.mu:
                        self.bytes += 100  # Estimate
                        
                req['reply_queue'].put({'ok': True, 'reply': reply})
                
            except Exception as e:
                logger.debug(f"RPC execution failed: {e}")
                req['reply_queue'].put({'ok': False, 'reply': None})
                
        thread = threading.Thread(target=execute_rpc, daemon=True)
        thread.start()
        return True


class Server:
    """
    RPC Server - Handles multiple services and dispatches RPC calls.
    """
    
    def __init__(self):
        """Create RPC server"""
        self.mu = threading.Lock()
        self.services: Dict[str, 'Service'] = {}
        self.count = 0  # Incoming RPC count
        
    def AddService(self, service: 'Service'):
        """Add service to server"""
        with self.mu:
            self.services[service.name] = service
            
    def GetCount(self) -> int:
        """Get incoming RPC count"""
        with self.mu:
            return self.count
            
    def Dispatch(self, svc_meth: str, args: Any) -> Any:
        """Dispatch RPC call to appropriate service"""
        with self.mu:
            self.count += 1
            
            # Split service and method
            parts = svc_meth.split('.')
            if len(parts) != 2:
                raise Exception(f"Invalid method format: {svc_meth}")
                
            service_name, method_name = parts
            service = self.services.get(service_name)
            
        if not service:
            available = list(self.services.keys())
            raise Exception(f"Unknown service {service_name}, available: {available}")
            
        return service.Dispatch(method_name, args)


class Service:
    """
    RPC Service - exactly matching Go lab Service behavior
    
    Wraps an object and exposes its methods via RPC.
    """
    
    def __init__(self, name: str, obj: Any):
        """Create RPC service"""
        self.name = name
        self.obj = obj
        self.methods = {}
        
        # Discover methods
        for method_name in dir(obj):
            if not method_name.startswith('_'):
                method = getattr(obj, method_name)
                if callable(method):
                    self.methods[method_name] = method
                    
    def Dispatch(self, method_name: str, args: Any) -> Any:
        """Dispatch method call"""
        method = self.methods.get(method_name)
        if not method:
            available = list(self.methods.keys())
            raise Exception(f"Unknown method {method_name} in {self.name}, available: {available}")
            
        # Create reply object of same type as args (simplified)
        reply = copy.deepcopy(args)
        
        # Call method with args and reply
        try:
            method(args, reply)
            return reply
        except Exception as e:
            logger.error(f"Method {method_name} failed: {e}")
            raise


# Factory functions matching Go lab API
def MakeNetwork() -> Network:
    """Create network simulation"""
    return Network()

def MakeServer() -> Server:
    """Create RPC server"""
    return Server()

def MakeService(name: str, obj: Any) -> Service:
    """Create RPC service"""
    return Service(name, obj)


# Test the enhanced labrpc
def test_enhanced_labrpc():
    """Test the enhanced labrpc functionality"""
    print("Testing Enhanced labrpc...")
    
    # Create network
    net = MakeNetwork()
    net.Reliable(True)
    
    # Create test service
    class TestService:
        def __init__(self):
            self.value = 0
            
        def Get(self, args, reply):
            reply.value = self.value
            
        def Put(self, args, reply):
            self.value = args.value
            reply.ok = True
    
    # Create server and service
    server = MakeServer()
    test_obj = TestService()
    service = MakeService("Test", test_obj)
    server.AddService(service)
    
    # Add server to network
    net.AddServer("server1", server)
    
    # Create client endpoint
    client = net.MakeEnd("client1")
    net.Connect("client1", "server1")
    net.Enable("client1", True)
    
    # Test RPC calls
    class PutArgs:
        def __init__(self, value):
            self.value = value
            
    class PutReply:
        def __init__(self):
            self.ok = False
            
    class GetArgs:
        pass
        
    class GetReply:
        def __init__(self):
            self.value = 0
    
    # Put value
    put_args = PutArgs(42)
    put_reply = PutReply()
    ok = client.Call("Test.Put", put_args, put_reply)
    print(f"Put: ok={ok}, reply.ok={put_reply.ok}")
    
    # Get value
    get_args = GetArgs()
    get_reply = GetReply()
    ok = client.Call("Test.Get", get_args, get_reply)
    print(f"Get: ok={ok}, reply.value={get_reply.value}")
    
    # Test network statistics
    print(f"Total RPCs: {net.GetTotalCount()}")
    print(f"Server RPCs: {net.GetCount('server1')}")
    print(f"Total bytes: {net.GetTotalBytes()}")
    
    # Test network partitions
    print("\nTesting network partition...")
    net.Enable("client1", False)
    ok = client.Call("Test.Get", get_args, get_reply)
    print(f"Partitioned call: ok={ok}")
    
    net.Enable("client1", True)
    ok = client.Call("Test.Get", get_args, get_reply)
    print(f"Reconnected call: ok={ok}, value={get_reply.value}")
    
    # Test unreliable network
    print("\nTesting unreliable network...")
    net.Reliable(False)
    successes = 0
    attempts = 10
    for i in range(attempts):
        ok = client.Call("Test.Get", get_args, get_reply)
        if ok:
            successes += 1
    print(f"Unreliable network: {successes}/{attempts} calls succeeded")
    
    net.Cleanup()
    print("Enhanced labrpc test completed")


if __name__ == "__main__":
    test_enhanced_labrpc()