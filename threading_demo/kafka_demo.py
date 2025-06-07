#!/usr/bin/env python3
"""
Simple Kafka-like Producer/Consumer Threading Example
Multiple producers sending messages, multiple consumers processing them
"""

import threading
import time
import queue
import random
import json
from dataclasses import dataclass
from typing import List, Dict, Any
import logging

# Setup logging to see what's happening
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(threadName)-12s] %(message)s',
    datefmt='%H:%M:%S'
)
logger = logging.getLogger(__name__)

# =============================================================================
# MESSAGE AND TOPIC CLASSES
# =============================================================================

@dataclass
class Message:
    """A message similar to Kafka message"""
    topic: str
    key: str
    value: str
    timestamp: float
    producer_id: str
    
    def to_dict(self):
        return {
            'topic': self.topic,
            'key': self.key, 
            'value': self.value,
            'timestamp': self.timestamp,
            'producer_id': self.producer_id
        }
    
class Topic:
    """A topic that holds messages (like Kafka topic)"""
    
    def __init__(self, name: str, max_size: int = 100):
        self.name = name
        self.messages = queue.Queue(maxsize=max_size)
        self.total_produced = 0
        self.total_consumed = 0
        self.lock = threading.Lock()
    
    def send(self, message: Message) -> bool:
        """Send message to topic (non-blocking)"""
        try:
            self.messages.put_nowait(message)
            with self.lock:
                self.total_produced += 1
            return True
        except queue.Full:
            return False  # Topic is full
    
    def receive(self, timeout: float = 1.0) -> Message:
        """Receive message from topic"""
        try:
            message = self.messages.get(timeout=timeout)
            with self.lock:
                self.total_consumed += 1
            return message
        except queue.Empty:
            return None
    
    def get_stats(self):
        """Get topic statistics"""
        with self.lock:
            return {
                'produced': self.total_produced,
                'consumed': self.total_consumed,
                'pending': self.total_produced - self.total_consumed,
                'queue_size': self.messages.qsize()
            }
        
# =============================================================================
# SIMPLE MESSAGE BROKER (LIKE KAFKA BROKER)
# =============================================================================

class MessageBroker:
    """Simple message broker that manages topics"""
    
    def __init__(self):
        self.topics: Dict[str, Topic] = {}
        self.lock = threading.Lock()
    
    def create_topic(self, topic_name: str, max_size: int = 100):
        """Create a new topic"""
        with self.lock:
            if topic_name not in self.topics:
                self.topics[topic_name] = Topic(topic_name, max_size)
                logger.info(f"Created topic: {topic_name}")
    
    def get_topic(self, topic_name: str) -> Topic:
        """Get topic by name"""
        with self.lock:
            return self.topics.get(topic_name)
    
    def get_all_stats(self):
        """Get statistics for all topics"""
        with self.lock:
            stats = {}
            for topic_name, topic in self.topics.items():
                stats[topic_name] = topic.get_stats()
            return stats


# =============================================================================
# PRODUCER CLASS
# =============================================================================

class Producer:
    """Message producer (like Kafka producer)"""
    
    def __init__(self, producer_id: str, broker: MessageBroker):
        self.producer_id = producer_id
        self.broker = broker
        self.messages_sent = 0
        self.running = True
    
    def send_message(self, topic_name: str, key: str, value: str) -> bool:
        """Send a message to a topic"""
        topic = self.broker.get_topic(topic_name)
        if not topic:
            logger.error(f"Topic {topic_name} not found!")
            return False
        
        message = Message(
            topic=topic_name,
            key=key,
            value=value,
            timestamp=time.time(),
            producer_id=self.producer_id
        )
        
        success = topic.send(message)
        if success:
            self.messages_sent += 1
            logger.info(f"{self.producer_id}: Sent '{value}' to {topic_name}")
        else:
            logger.warning(f"{self.producer_id}: Topic {topic_name} is full!")
        
        return success
    
    def produce_continuously(self, topic_name: str, message_count: int):
        """Continuously produce messages"""
        logger.info(f"{self.producer_id}: Starting to produce {message_count} messages")
        
        for i in range(message_count):
            if not self.running:
                break
                
            # Generate sample data
            user_id = f"user_{random.randint(1, 100)}"
            action = random.choice(["click", "view", "purchase", "logout"])
            value = f"{action}_event_{i}"
            
            self.send_message(topic_name, user_id, value)
            
            # Random delay between messages (0.1 to 0.5 seconds)
            time.sleep(random.uniform(0.1, 0.5))
        
        logger.info(f"{self.producer_id}: Finished producing ({self.messages_sent} sent)")
    
    def stop(self):
        """Stop the producer"""
        self.running = False
        
        

# =============================================================================
# CONSUMER CLASS
# =============================================================================

class Consumer:
    """Message consumer (like Kafka consumer)"""
    
    def __init__(self, consumer_id: str, broker: MessageBroker):
        self.consumer_id = consumer_id
        self.broker = broker
        self.messages_processed = 0
        self.running = True
    
    def process_message(self, message: Message):
        """Process a single message"""
        logger.info(f"{self.consumer_id}: Processing '{message.value}' (from {message.producer_id})")
        
        # Simulate message processing time
        processing_time = random.uniform(0.2, 0.8)
        time.sleep(processing_time)
        
        self.messages_processed += 1
        logger.info(f"✨ {self.consumer_id}: Finished processing '{message.value}' ({processing_time:.2f}s)")
    
    def consume_continuously(self, topic_name: str, max_messages: int = None):
        """Continuously consume messages from a topic"""
        logger.info(f"🎯 {self.consumer_id}: Starting to consume from {topic_name}")
        
        topic = self.broker.get_topic(topic_name)
        if not topic:
            logger.error(f"Topic {topic_name} not found!")
            return
        
        processed_count = 0
        consecutive_timeouts = 0
        max_consecutive_timeouts = 5  # Stop after 5 seconds of no messages
        
        while self.running:
            if max_messages and processed_count >= max_messages:
                break
                
            # Try to receive a message
            message = topic.receive(timeout=1.0)
            
            if message:
                self.process_message(message)
                processed_count += 1
                consecutive_timeouts = 0  # Reset timeout counter when we get a message
            else:
                # No message available
                consecutive_timeouts += 1
                logger.info(f"{self.consumer_id}: No messages available, waiting... ({consecutive_timeouts}/{max_consecutive_timeouts})")
                
                # Stop if we've been waiting too long
                if consecutive_timeouts >= max_consecutive_timeouts:
                    logger.info(f"{self.consumer_id}: No messages for {max_consecutive_timeouts} seconds, stopping")
                    break
        
        logger.info(f"{self.consumer_id}: Stopped consuming ({self.messages_processed} processed)")
    
    def stop(self):
        """Stop the consumer"""
        self.running = False


def stats_monitor(broker: MessageBroker, duration: int):
    """Monitor and print broker statistics"""
    logger.info("Stats monitor starting...")
    
    for i in range(duration):
        time.sleep(1)
        stats = broker.get_all_stats()
        
        print(f"\n BROKER STATS (t={i+1}s):")
        for topic_name, topic_stats in stats.items():
            print(f"  {topic_name}: "
                  f"Produced={topic_stats['produced']}, "
                  f"Consumed={topic_stats['consumed']}, "
                  f"Pending={topic_stats['pending']}")
    
    logger.info("📊 Stats monitor finished")

# =============================================================================
# MAIN DEMO FUNCTION
# =============================================================================

def run_kafka_demo():
    """Run the complete Kafka-like demo"""
    
    print("🎬 STARTING KAFKA-LIKE DEMO")
    print("=" * 50)

    # Global broker instance
    broker = MessageBroker()

    
    # Create topics
    broker.create_topic("user_events", max_size=50)
    broker.create_topic("system_logs", max_size=30)
    
    # Create producers
    producers = [
        Producer("producer_1", broker),
        Producer("producer_2", broker),
        Producer("producer_3", broker)
    ]
    
    # Create consumers
    consumers = [
        Consumer("consumer_A", broker),
        Consumer("consumer_B", broker),
        Consumer("consumer_C", broker),
        Consumer("consumer_D", broker)
    ]
    
    # Start all threads
    threads = []
    
    # Start producer threads
    for i, producer in enumerate(producers):
        if i < 2:  # First 2 producers send to user_events
            t = threading.Thread(
                target=producer.produce_continuously,
                args=("user_events", 10),
                name=f"Producer-{i+1}"
            )
        else:  # Last producer sends to system_logs
            t = threading.Thread(
                target=producer.produce_continuously,
                args=("system_logs", 8),
                name=f"Producer-{i+1}"
            )
        threads.append(t)
        t.start()
    
    # Start consumer threads
    for i, consumer in enumerate(consumers):
        if i < 3:  # First 3 consumers read user_events
            t = threading.Thread(
                target=consumer.consume_continuously,
                args=("user_events", 15),
                name=f"Consumer-{chr(65+i)}"
            )
        else:  # Last consumer reads system_logs
            t = threading.Thread(
                target=consumer.consume_continuously,
                args=("system_logs", 10),
                name=f"Consumer-{chr(65+i)}"
            )
        threads.append(t)
        t.start()
    
    # Start stats monitor
    stats_thread = threading.Thread(
        target=stats_monitor,
        args=(broker, 15),
        name="StatsMonitor"
    )
    threads.append(stats_thread)
    stats_thread.start()
    
    # Wait for all threads to complete
    for t in threads:
        t.join()
    
    # Final statistics
    print("\n" + "=" * 50)
    print("🏆 FINAL RESULTS:")
    final_stats = broker.get_all_stats()
    
    for topic_name, stats in final_stats.items():
        print(f"\n Topic: {topic_name}")
        print(f"   Messages produced: {stats['produced']}")
        print(f"   Messages consumed: {stats['consumed']}")
        print(f"   Messages pending: {stats['pending']}")
    
    # Producer statistics
    print(f"\nProducer Statistics:")
    for producer in producers:
        print(f"   {producer.producer_id}: {producer.messages_sent} messages sent")
    
    # Consumer statistics
    print(f"\nConsumer Statistics:")
    for consumer in consumers:
        print(f"   {consumer.consumer_id}: {consumer.messages_processed} messages processed")


if __name__ == "__main__":
    run_kafka_demo()