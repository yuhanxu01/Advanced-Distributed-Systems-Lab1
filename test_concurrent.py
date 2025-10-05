# -*- coding:utf-8 -*-
"""
Test script to simulate concurrent task submissions
Tests the "what if two clients send at the same time?" scenario
"""

import threading
import time
from manager import LoadBalancer

def submit_tasks_batch(lb, start_id, num_tasks, duration=60):
    """
    Submit a batch of tasks from a simulated client
    Args:
        lb: LoadBalancer instance
        start_id: starting task ID
        num_tasks: how many tasks to submit
        duration: task duration in seconds
    """
    client_name = threading.current_thread().name
    print(f"[{client_name}] Starting to submit {num_tasks} tasks")
    
    for i in range(num_tasks):
        task_id = f"{client_name}_TASK_{start_id + i}"
        lb.assign_task(task_id, duration, algorithm='cpu_based')
        time.sleep(0.5)  # Small delay between submissions from same client
    
    print(f"[{client_name}] Finished submitting tasks")

def test_concurrent_clients():
    """
    Test scenario: Multiple clients submitting tasks simultaneously
    This tests the manager's ability to handle concurrent requests
    """
    print("="*60)
    print("CONCURRENT CLIENT TEST")
    print("="*60)
    print("Simulating multiple clients submitting tasks at the same time")
    print("="*60 + "\n")
    
    # Configure workers
    worker_configs = [
        ('worker1', '10.128.0.2'),
        ('worker2', '10.128.0.3'),
        ('worker3', '10.128.0.4'),
        ('worker4', '10.128.0.6'),
    ]
    
    # Initialize and connect
    lb = LoadBalancer(worker_configs)
    if not lb.connect_all_workers():
        print("[Test] Failed to connect to workers")
        return
    
    lb.start_monitoring()
    time.sleep(5)  # Let monitoring stabilize
    
    # Create multiple client threads
    # Each thread represents a different client submitting tasks
    clients = []
    
    # Client 1: Submit 5 tasks
    client1 = threading.Thread(
        target=submit_tasks_batch,
        args=(lb, 1, 5, 60),
        name="CLIENT_1"
    )
    clients.append(client1)
    
    # Client 2: Submit 5 tasks
    client2 = threading.Thread(
        target=submit_tasks_batch,
        args=(lb, 101, 5, 60),
        name="CLIENT_2"
    )
    clients.append(client2)
    
    # Client 3: Submit 5 tasks
    client3 = threading.Thread(
        target=submit_tasks_batch,
        args=(lb, 201, 5, 60),
        name="CLIENT_3"
    )
    clients.append(client3)
    
    # Start all clients simultaneously
    print("[Test] Starting all clients simultaneously...")
    start_time = time.time()
    
    for client in clients:
        client.start()
    
    # Wait for all clients to finish submitting
    for client in clients:
        client.join()
    
    submission_time = time.time() - start_time
    print(f"\n[Test] All clients finished submitting in {submission_time:.2f}s")
    
    # Wait for tasks to complete
    print("[Test] Waiting for tasks to complete...")
    time.sleep(70)  # Wait for 1-minute tasks + buffer
    
    # Show results
    print("\n[Test] Task Distribution:")
    distribution = {}
    for assignment in lb.task_assignments:
        wid = assignment['worker_id']
        distribution[wid] = distribution.get(wid, 0) + 1
    
    for wid, count in sorted(distribution.items()):
        print(f"  Worker {wid}: {count} tasks")
    
    lb.stop_monitoring()
    print("\n[Test] Concurrent test completed!")

if __name__ == '__main__':
    test_concurrent_clients()
