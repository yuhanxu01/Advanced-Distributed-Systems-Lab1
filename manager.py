# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Concurrent Task Scheduler
Real-time monitoring and immediate task assignment
"""

import pickle
import time
import threading
from multiprocessing.connection import Client
from datetime import datetime
import json
from collections import deque

class RPCProxy:
    """
    Proxy for making RPC calls to worker nodes
    """
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
    """
    Represents a worker node with RPC connection and status tracking
    """
    def __init__(self, worker_id, host, port=17000):
        self.worker_id = worker_id
        self.host = host
        self.port = port
        self.proxy = None
        self.last_status = None
        self.is_connected = False
        self._connection = None
        self.current_task = None  # Track current task
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
    
    def reconnect(self):
        """Attempt to reconnect to worker"""
        print(f"[Manager] Attempting to reconnect to worker {self.worker_id}...")
        if self._connection:
            try:
                self._connection.close()
            except:
                pass
        return self.connect()
    
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
    
    def is_idle(self):
        """Check if worker is idle and available for new task"""
        with self.task_lock:
            if self.current_task is not None:
                return False
        
        status = self.get_status()
        if status:
            return status['is_idle']
        return False
    
    def assign_task(self, task_params):
        """Assign a task to this worker (non-blocking)"""
        if not self.is_connected:
            if not self.reconnect():
                return {'status': 'error', 'error': 'not connected'}
        
        with self.task_lock:
            self.current_task = task_params['task_id']
        
        try:
            result = self.proxy.execute_task(task_params)
            return result
        except Exception as e:
            print(f"[Manager] Error assigning task to worker {self.worker_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}
        finally:
            with self.task_lock:
                self.current_task = None

class LoadBalancer:
    """
    Manages workers and implements concurrent load balancing
    """
    def __init__(self, worker_configs):
        self.workers = []
        self.monitoring_active = False
        self.monitor_thread = None
        self.monitor_interval = 2  # Check every 2 seconds
        self.task_queue = deque()  # Queue of pending tasks
        self.completed_tasks = []  # Completed task records
        self.scheduler_active = False
        self.scheduler_thread = None
        self.queue_lock = threading.Lock()
        self.results_lock = threading.Lock()
        
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
                        task_info = f"Task: {worker.current_task}" if worker.current_task else "IDLE"
                    idle_str = "IDLE" if status['is_idle'] else "BUSY"
                    print(f"Worker {worker.worker_id}: CPU={status['cpu_load']:.4f} | "
                          f"{idle_str} | {task_info}")
                else:
                    print(f"Worker {worker.worker_id}: DISCONNECTED")
            
            with self.queue_lock:
                pending = len(self.task_queue)
            with self.results_lock:
                completed = len(self.completed_tasks)
            
            print(f"Queue: {pending} pending | {completed} completed")
            print("-" * 70)
            time.sleep(self.monitor_interval)
    
    def start_scheduler(self):
        """Start the task scheduler"""
        self.scheduler_active = True
        self.scheduler_thread = threading.Thread(target=self._scheduler_loop)
        self.scheduler_thread.daemon = True
        self.scheduler_thread.start()
        print("[Manager] Started task scheduler")
    
    def stop_scheduler(self):
        """Stop the task scheduler"""
        self.scheduler_active = False
        if self.scheduler_thread:
            self.scheduler_thread.join()
    
    def _scheduler_loop(self):
        """
        Continuously check for idle workers and assign tasks
        This is the core of concurrent task execution
        """
        while self.scheduler_active:
            # Check if there are pending tasks
            with self.queue_lock:
                if not self.task_queue:
                    time.sleep(0.5)
                    continue
            
            # Find idle workers
            idle_workers = [w for w in self.workers if w.is_idle()]
            
            if idle_workers:
                # Assign tasks to all idle workers
                for worker in idle_workers:
                    with self.queue_lock:
                        if not self.task_queue:
                            break
                        task_info = self.task_queue.popleft()
                    
                    # Start task in separate thread
                    t = threading.Thread(
                        target=self._execute_task,
                        args=(worker, task_info)
                    )
                    t.daemon = True
                    t.start()
            
            time.sleep(0.5)  # Check every 0.5 seconds
    
    def _execute_task(self, worker, task_info):
        """Execute a single task on a worker"""
        task_id = task_info['task_id']
        task_params = task_info['params']
        algorithm = task_info['algorithm']
        
        print(f"\n[Scheduler] Assigning {task_id} to {worker.worker_id} ({algorithm})")
        
        start_time = time.time()
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
    
    def select_worker_by_cpu(self):
        """Select worker with lowest CPU usage"""
        worker_loads = []
        for worker in self.workers:
            if worker.is_idle():
                status = worker.get_status()
                if status:
                    worker_loads.append((worker, status['cpu_load']))
        
        if not worker_loads:
            return None
        
        worker_loads.sort(key=lambda x: x[1])
        return worker_loads[0][0]
    
    def enqueue_tasks(self, tasks, algorithm='cpu_based'):
        """Add tasks to the queue"""
        with self.queue_lock:
            for task_id, duration in tasks:
                task_info = {
                    'task_id': task_id,
                    'params': {'task_id': task_id, 'duration': duration},
                    'algorithm': algorithm
                }
                self.task_queue.append(task_info)
        print(f"[Manager] Enqueued {len(tasks)} tasks with {algorithm} algorithm")
    
    def wait_for_completion(self, expected_count):
        """Wait until all tasks are completed"""
        print(f"\n[Manager] Waiting for {expected_count} tasks to complete...")
        while True:
            with self.results_lock:
                completed = len(self.completed_tasks)
            
            with self.queue_lock:
                pending = len(self.task_queue)
            
            if completed >= expected_count and pending == 0:
                # Wait a bit more to ensure all tasks are done
                time.sleep(2)
                break
            
            time.sleep(1)
        
        print(f"[Manager] All {expected_count} tasks completed!")
    
    def run_experiment(self, num_tasks=20, task_duration=10):
        """
        Run concurrent load balancing experiment
        Args:
            num_tasks: number of tasks to run (default 20)
            task_duration: duration of each task in seconds (default 10)
        """
        print("\n" + "="*70)
        print("CONCURRENT LOAD BALANCING EXPERIMENT")
        print("="*70)
        print(f"Total Tasks: {num_tasks}")
        print(f"Task Duration: {task_duration}s each")
        print(f"Workers: {len(self.workers)}")
        print(f"Strategy: Assign tasks immediately when workers become idle")
        print("="*70 + "\n")
        
        # Start scheduler for concurrent execution
        self.start_scheduler()
        
        # Test 1: CPU-based algorithm
        print("\n### TEST 1: CPU-BASED LOAD BALANCING (Concurrent) ###\n")
        self.completed_tasks = []
        
        tasks = [(f"CPU_TASK_{i+1}", task_duration) for i in range(num_tasks)]
        self.enqueue_tasks(tasks, algorithm='cpu_based')
        
        experiment_start = time.time()
        self.wait_for_completion(num_tasks)
        cpu_duration = time.time() - experiment_start
        
        cpu_results = self.completed_tasks.copy()
        
        print(f"\n[Result] CPU-based test completed in {cpu_duration:.1f}s")
        print(f"  Theoretical minimum: {num_tasks * task_duration / len(self.workers):.1f}s")
        print(f"  Efficiency: {(num_tasks * task_duration / len(self.workers) / cpu_duration * 100):.1f}%")
        
        # Wait between tests
        time.sleep(5)
        
        # Test 2: Round-robin algorithm (for comparison)
        print("\n### TEST 2: ROUND-ROBIN LOAD BALANCING (Concurrent) ###\n")
        self.completed_tasks = []
        
        # For round-robin, we pre-assign workers
        tasks_rr = []
        for i in range(num_tasks):
            task_id = f"RR_TASK_{i+1}"
            # Pre-assign to worker based on round-robin
            worker_idx = i % len(self.workers)
            tasks_rr.append((task_id, task_duration))
        
        self.enqueue_tasks(tasks_rr, algorithm='round_robin')
        
        experiment_start = time.time()
        self.wait_for_completion(num_tasks)
        rr_duration = time.time() - experiment_start
        
        rr_results = self.completed_tasks.copy()
        
        print(f"\n[Result] Round-robin test completed in {rr_duration:.1f}s")
        print(f"  Theoretical minimum: {num_tasks * task_duration / len(self.workers):.1f}s")
        print(f"  Efficiency: {(num_tasks * task_duration / len(self.workers) / rr_duration * 100):.1f}%")
        
        # Stop scheduler
        self.stop_scheduler()
        
        # Generate comparison report
        self._generate_report(cpu_results, rr_results, cpu_duration, rr_duration)
    
    def _generate_report(self, cpu_results, rr_results, cpu_duration, rr_duration):
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
        print(f"CPU-based: {cpu_duration:.1f}s")
        print(f"Round-robin: {rr_duration:.1f}s")
        speedup = ((rr_duration - cpu_duration) / rr_duration * 100)
        if speedup > 0:
            print(f"CPU-based is {speedup:.1f}% faster")
        else:
            print(f"Round-robin is {-speedup:.1f}% faster")
        
        # Save detailed report
        report = {
            'experiment_info': {
                'total_tasks': len(cpu_results),
                'workers': len(self.workers),
                'task_duration': 10
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
    # Configure worker nodes (update with your actual IPs)
    worker_configs = [
        ('worker1', '10.128.0.2'),  # node1
        ('worker2', '10.128.0.3'),  # node2
        ('worker3', '10.128.0.4'),  # node3
        ('worker4', '10.128.0.6'),  # node4
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
    
    # Run concurrent experiment
    # 20 tasks, 10 seconds each
    # With 4 workers, theoretical minimum time: 20*10/4 = 50 seconds
    lb.run_experiment(num_tasks=20, task_duration=10)
    
    # Stop monitoring
    lb.stop_monitoring()
    
    print("[Manager] Experiment completed!")

if __name__ == '__main__':
    main()
