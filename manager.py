# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Fixed Concurrent Scheduler
"""

import pickle
import time
import threading
from multiprocessing.connection import Client
from datetime import datetime
import json
from collections import deque

class RPCProxy:
    """Proxy for making RPC calls to worker nodes"""
    def __init__(self, connection):
        self._connection = connection
        self._lock = threading.Lock()
        
    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            with self._lock:
                try:
                    self._connection.send(pickle.dumps((name, args, kwargs)))
                    result = pickle.loads(self._connection.recv())
                    if isinstance(result, Exception):
                        raise result
                    return result
                except (EOFError, ConnectionResetError, BrokenPipeError) as e:
                    raise ConnectionError(f"Connection lost: {str(e)}")
        return do_rpc

class WorkerProxy:
    """Represents a worker node with RPC connection and status tracking"""
    def __init__(self, worker_id, host, port=17000):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.proxy = None
        self.last_status = None
        self.is_connected = False
        self._connection = None
        self.current_task = None  # Manager-side tracking
        self.task_lock = threading.Lock()
        
    def connect(self):
        """Establish RPC connection to worker"""
        try:
            self._connection = Client((self.host, self.port), authkey=b'peekaboo')
            self.proxy = RPCProxy(self._connection)
            self.is_connected = True
            print(f"[Manager] Connected to worker {self.worker_id} at {self.host}")
            return True
        except Exception as e:
            print(f"[Manager] Failed to connect to worker {self.worker_id}: {str(e)}")
            self.is_connected = False
            return False
    
    def get_status(self):
        """Get current resource status from worker"""
        if not self.is_connected:
            return None
        try:
            status = self.proxy.get_resource_status()
            self.last_status = status
            return status
        except Exception as e:
            self.is_connected = False
            return None
    
    def is_busy(self):
        """Check if worker is currently executing a task (manager-side tracking)"""
        with self.task_lock:
            return self.current_task is not None
    
    def assign_task(self, task_params):
        """Assign a task to this worker (blocking call)"""
        if not self.is_connected:
            return {'status': 'error', 'error': 'not connected'}
        
        try:
            result = self.proxy.execute_task(task_params)
            return result
        except Exception as e:
            print(f"[Manager] Error assigning task to worker {self.worker_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}

class LoadBalancer:
    """Manages workers and implements concurrent load balancing"""
    def __init__(self, worker_configs):
        self.workers = []
        self.monitoring_active = False
        self.monitor_thread = None
        self.monitor_interval = 2
        self.completed_tasks = []
        self.results_lock = threading.Lock()
        self.active_threads = []
        self.threads_lock = threading.Lock()
        
        # Initialize workers
        for worker_id, host in worker_configs:
            worker = WorkerProxy(worker_id, host)
            self.workers.append(worker)
        
        print(f"[Manager] Initialized with {len(self.workers)} workers")
    
    def connect_all_workers(self):
        """Connect to all worker nodes"""
        print("[Manager] Connecting to all workers...")
        success_count = 0
        for worker in self.workers:
            if worker.connect():
                success_count += 1
            time.sleep(0.5)
        print(f"[Manager] Successfully connected to {success_count}/{len(self.workers)} workers")
        return success_count > 0
    
    def start_monitoring(self):
        """Start background monitoring of worker resources"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print("[Manager] Started resource monitoring")
    
    def stop_monitoring(self):
        """Stop background monitoring"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()
    
    def _monitor_loop(self):
        """Continuous monitoring loop"""
        while self.monitoring_active:
            print(f"\n[Monitor] {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 70)
            
            for worker in self.workers:
                status = worker.get_status()
                if status:
                    with worker.task_lock:
                        current = worker.current_task
                    
                    # Clear status based on manager-side tracking
                    if current:
                        status_str = f"BUSY [{current}]"
                    else:
                        status_str = "IDLE"
                    
                    print(f"Worker {worker.worker_id}: CPU={status['cpu_load']:.4f} | {status_str}")
                else:
                    print(f"Worker {worker.worker_id}: DISCONNECTED")
            
            with self.results_lock:
                completed = len(self.completed_tasks)
            
            with self.threads_lock:
                active = sum(1 for t in self.active_threads if t.is_alive())
            
            print(f"Tasks: {active} running | {completed} completed")
            print("-" * 70)
            time.sleep(self.monitor_interval)
    
    def _execute_task(self, worker, task_id, duration, algorithm):
        """Execute a single task on a worker"""
        # Mark worker as busy
        with worker.task_lock:
            worker.current_task = task_id
        
        try:
            print(f"[Scheduler] Assigning {task_id} to {worker.worker_id} ({algorithm})")
            
            start_time = time.time()
            task_params = {'task_id': task_id, 'duration': duration}
            result = worker.assign_task(task_params)
            end_time = time.time()
            
            # Record completion
            record = {
                'task_id': task_id,
                'worker_id': worker.worker_id,
                'worker_host': worker.host,
                'algorithm': algorithm,
                'start_time': start_time,
                'end_time': end_time,
                'duration': end_time - start_time,
                'result': result
            }
            
            with self.results_lock:
                self.completed_tasks.append(record)
            
            if result.get('status') != 'error':
                print(f"[Scheduler] {task_id} completed on {worker.worker_id} "
                      f"in {end_time - start_time:.1f}s")
            else:
                print(f"[Scheduler] {task_id} FAILED on {worker.worker_id}")
        
        finally:
            # Mark worker as idle
            with worker.task_lock:
                worker.current_task = None
    
    def run_cpu_based(self, num_tasks, task_duration, interval=10):
        """
        CPU-based load balancing: submit tasks with interval, assign to lowest CPU worker
        """
        print("\n### CPU-BASED LOAD BALANCING ###")
        print(f"Submitting {num_tasks} tasks with {interval}s interval")
        print("Strategy: Assign to worker with lowest CPU load at submission time\n")
        
        self.completed_tasks = []
        threads = []
        
        for i in range(num_tasks):
            task_id = f"CPU_TASK_{i+1}"
            
            # Find worker with lowest CPU load
            best_worker = None
            lowest_cpu = float('inf')
            
            for worker in self.workers:
                if not worker.is_busy():
                    status = worker.get_status()
                    if status and status['cpu_load'] < lowest_cpu:
                        lowest_cpu = status['cpu_load']
                        best_worker = worker
            
            # If all workers busy, select one with lowest CPU anyway
            if not best_worker:
                for worker in self.workers:
                    status = worker.get_status()
                    if status and status['cpu_load'] < lowest_cpu:
                        lowest_cpu = status['cpu_load']
                        best_worker = worker
            
            if best_worker:
                print(f"[Submit {i+1}/{num_tasks}] {task_id} -> {best_worker.worker_id} (CPU: {lowest_cpu:.4f})")
                
                # Start task in separate thread
                t = threading.Thread(
                    target=self._execute_task,
                    args=(best_worker, task_id, task_duration, 'cpu_based')
                )
                t.daemon = True
                t.start()
                threads.append(t)
                
                with self.threads_lock:
                    self.active_threads = threads
            
            # Wait interval before next submission (except last one)
            if i < num_tasks - 1:
                time.sleep(interval)
        
        # Wait for all tasks to complete
        print(f"\n[Manager] Waiting for all {num_tasks} CPU-based tasks to complete...")
        for t in threads:
            t.join()
        
        return self.completed_tasks.copy()
    
    def run_round_robin(self, num_tasks, task_duration, interval=10):
        """
        Round-robin load balancing: submit tasks with interval, assign in round-robin order
        """
        print("\n### ROUND-ROBIN LOAD BALANCING ###")
        print(f"Submitting {num_tasks} tasks with {interval}s interval")
        print("Strategy: Assign to workers in sequential order\n")
        
        self.completed_tasks = []
        threads = []
        
        for i in range(num_tasks):
            task_id = f"RR_TASK_{i+1}"
            
            # Round-robin: cycle through workers
            worker = self.workers[i % len(self.workers)]
            
            print(f"[Submit {i+1}/{num_tasks}] {task_id} -> {worker.worker_id} (round-robin)")
            
            # Start task in separate thread
            t = threading.Thread(
                target=self._execute_task,
                args=(worker, task_id, task_duration, 'round_robin')
            )
            t.daemon = True
            t.start()
            threads.append(t)
            
            with self.threads_lock:
                self.active_threads = threads
            
            # Wait interval before next submission (except last one)
            if i < num_tasks - 1:
                time.sleep(interval)
        
        # Wait for all tasks to complete
        print(f"\n[Manager] Waiting for all {num_tasks} round-robin tasks to complete...")
        for t in threads:
            t.join()
        
        return self.completed_tasks.copy()
    
    def run_experiment(self, num_tasks=20, task_duration=10, interval=10):
        """
        Run load balancing experiment
        Args:
            num_tasks: number of tasks (default 20)
            task_duration: duration of each task in seconds (default 10)
            interval: seconds between task submissions (default 10)
        """
        print("\n" + "="*70)
        print("LOAD BALANCING EXPERIMENT")
        print("="*70)
        print(f"Total Tasks: {num_tasks}")
        print(f"Task Duration: {task_duration}s each")
        print(f"Submission Interval: {interval}s")
        print(f"Workers: {len(self.workers)}")
        print("="*70 + "\n")
        
        # Test 1: CPU-based algorithm
        exp_start = time.time()
        cpu_results = self.run_cpu_based(num_tasks, task_duration, interval)
        cpu_duration = time.time() - exp_start
        
        print(f"\n[Result] CPU-based completed in {cpu_duration:.1f}s\n")
        
        # Wait between tests
        print("Waiting 10s before next test...\n")
        time.sleep(10)
        
        # Test 2: Round-robin algorithm
        exp_start = time.time()
        rr_results = self.run_round_robin(num_tasks, task_duration, interval)
        rr_duration = time.time() - exp_start
        
        print(f"\n[Result] Round-robin completed in {rr_duration:.1f}s\n")
        
        # Generate comparison report
        self._generate_report(cpu_results, rr_results, cpu_duration, rr_duration, 
                            num_tasks, task_duration, interval)
    
    def _generate_report(self, cpu_results, rr_results, cpu_duration, rr_duration,
                        num_tasks, task_duration, interval):
        """Generate detailed comparison report"""
        print("\n" + "="*70)
        print("EXPERIMENT RESULTS")
        print("="*70)
        
        # Analyze CPU-based distribution
        print("\n### CPU-Based Algorithm ###")
        print(f"Total Time: {cpu_duration:.1f}s")
        cpu_dist = {}
        for record in cpu_results:
            wid = record['worker_id']
            cpu_dist[wid] = cpu_dist.get(wid, 0) + 1
        
        for wid in sorted(cpu_dist.keys()):
            count = cpu_dist[wid]
            print(f"  {wid}: {count} tasks ({count/len(cpu_results)*100:.1f}%)")
        
        # Analyze Round-robin distribution
        print("\n### Round-Robin Algorithm ###")
        print(f"Total Time: {rr_duration:.1f}s")
        rr_dist = {}
        for record in rr_results:
            wid = record['worker_id']
            rr_dist[wid] = rr_dist.get(wid, 0) + 1
        
        for wid in sorted(rr_dist.keys()):
            count = rr_dist[wid]
            print(f"  {wid}: {count} tasks ({count/len(rr_results)*100:.1f}%)")
        
        # Performance comparison
        print("\n### Performance Comparison ###")
        print(f"CPU-based total time: {cpu_duration:.1f}s")
        print(f"Round-robin total time: {rr_duration:.1f}s")
        
        diff = cpu_duration - rr_duration
        if abs(diff) < 1:
            print(f"Performance is similar (difference: {abs(diff):.1f}s)")
        elif diff > 0:
            print(f"Round-robin is {diff:.1f}s faster ({diff/cpu_duration*100:.1f}% improvement)")
        else:
            print(f"CPU-based is {abs(diff):.1f}s faster ({abs(diff)/rr_duration*100:.1f}% improvement)")
        
        # Save detailed report
        report = {
            'experiment_info': {
                'total_tasks': num_tasks,
                'task_duration': task_duration,
                'submission_interval': interval,
                'workers': len(self.workers)
            },
            'cpu_based': {
                'duration': cpu_duration,
                'distribution': cpu_dist,
                'tasks': cpu_results
            },
            'round_robin': {
                'duration': rr_duration,
                'distribution': rr_dist,
                'tasks': rr_results
            }
        }
        
        with open('experiment_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print("\n[Manager] Detailed report saved to experiment_report.json")
        print("="*70 + "\n")

def main():
    """Main function to run the manager"""
    # Configure worker nodes
    worker_configs = [
        ('worker1', '10.128.0.2'),
        ('worker2', '10.128.0.3'),
        ('worker3', '10.128.0.4'),
        ('worker4', '10.128.0.6'),
    ]
    
    # Initialize load balancer
    lb = LoadBalancer(worker_configs)
    
    # Connect to all workers
    if not lb.connect_all_workers():
        print("[Manager] Failed to connect to workers. Exiting.")
        return
    
    # Start monitoring
    lb.start_monitoring()
    
    # Wait for initial status check
    time.sleep(5)
    
    # Run experiment with 10-second submission intervals
    # Each task runs for 10 seconds
    # 20 tasks submitted with 10s interval between submissions
    lb.run_experiment(num_tasks=20, task_duration=10, interval=10)
    
    # Stop monitoring
    lb.stop_monitoring()
    
    print("[Manager] Experiment completed!")

if __name__ == '__main__':
    main()
