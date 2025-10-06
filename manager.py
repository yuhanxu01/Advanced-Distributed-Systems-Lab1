# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Fixed Concurrent Scheduler
"""

import pickle
import time
import threading
import random
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
                    # Raise a standard ConnectionError for the caller to handle
                    raise ConnectionError(f"Connection to worker lost: {str(e)}")
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
        self.current_task = None  # Manager-side tracking of the current task
        self.task_lock = threading.Lock()

    def connect(self):
        """Establish RPC connection to the worker"""
        try:
            self._connection = Client((self.host, self.port), authkey=b'peekaboo')
            self.proxy = RPCProxy(self._connection)
            self.is_connected = True
            print(f"[Manager] Connected to worker {self.worker_id} at {self.host}:{self.port}")
            return True
        except Exception as e:
            print(f"[Manager] Failed to connect to worker {self.worker_id}: {str(e)}")
            self.is_connected = False
            return False

    def get_status(self):
        """Get current resource status (CPU load) from the worker"""
        if not self.is_connected:
            return None
        try:
            status = self.proxy.get_resource_status()
            self.last_status = status
            return status
        except ConnectionError as e:
            print(f"[Manager] Connection error with worker {self.worker_id}: {e}")
            self.is_connected = False
            return None
        except Exception as e:
            print(f"[Manager] Failed to get status from worker {self.worker_id}: {e}")
            self.is_connected = False
            return None

    def is_busy(self):
        """Check if the worker is currently executing a task (based on manager-side tracking)"""
        with self.task_lock:
            return self.current_task is not None

    def assign_task(self, task_params):
        """Assign a task to this worker (this is a blocking call)"""
        if not self.is_connected:
            return {'status': 'error', 'error': 'not connected'}

        try:
            result = self.proxy.execute_task(task_params)
            return result
        except ConnectionError as e:
            print(f"[Manager] Connection lost while assigning task to {self.worker_id}: {str(e)}")
            self.is_connected = False
            return {'status': 'error', 'error': f"Connection lost: {str(e)}"}
        except Exception as e:
            print(f"[Manager] Error assigning task to worker {self.worker_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}

class LoadBalancer:
    """Manages workers and implements load balancing strategies"""
    def __init__(self, worker_configs):
        self.workers = [WorkerProxy(w_id, host) for w_id, host in worker_configs]
        self.monitoring_active = False
        self.monitor_thread = None
        self.monitor_interval = 2  # seconds
        self.completed_tasks = []
        self.results_lock = threading.Lock()
        self.active_threads = []
        self.threads_lock = threading.Lock()
        print(f"[Manager] Initialized with {len(self.workers)} workers")

    def connect_all_workers(self):
        """Attempt to connect to all configured worker nodes"""
        print("[Manager] Connecting to all workers...")
        success_count = sum(1 for worker in self.workers if worker.connect())
        print(f"[Manager] Successfully connected to {success_count}/{len(self.workers)} workers")
        return success_count > 0

    def start_monitoring(self):
        """Start background monitoring of worker resources in a separate thread"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop)
        self.monitor_thread.daemon = True
        self.monitor_thread.start()
        print("[Manager] Started resource monitoring")

    def stop_monitoring(self):
        """Stop the background monitoring thread"""
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()

    def _monitor_loop(self):
        """The continuous loop for monitoring worker status"""
        while self.monitoring_active:
            print(f"\n[Monitor] {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 70)

            for worker in self.workers:
                status = worker.get_status()
                if status:
                    with worker.task_lock:
                        current_task_id = worker.current_task
                    
                    status_str = f"BUSY [{current_task_id}]" if current_task_id else "IDLE"
                    print(f"Worker {worker.worker_id}: CPU={status['cpu_load']:.4f} | {status_str}")
                else:
                    print(f"Worker {worker.worker_id}: DISCONNECTED")

            with self.results_lock:
                completed_count = len(self.completed_tasks)
            
            with self.threads_lock:
                active_count = sum(1 for t in self.active_threads if t.is_alive())
            
            print(f"Tasks: {active_count} running | {completed_count} completed")
            print("-" * 70)
            time.sleep(self.monitor_interval)

    def _execute_task(self, worker, task_id, terms, algorithm):
        """
        The target function for each task thread. It handles the full lifecycle of a single task.
        """
        # Mark worker as busy with the task ID
        with worker.task_lock:
            worker.current_task = task_id
        
        try:
            print(f"[Scheduler] Assigning {task_id} ({terms/1e6:.1f}M terms) to {worker.worker_id} ({algorithm})")
            
            start_time = time.time()
            # Define the task for the worker: calculate Pi using the Leibniz method
            task_params = {'task_id': task_id, 'algorithm': 'pi_leibniz', 'terms': terms}
            result = worker.assign_task(task_params)
            end_time = time.time()
            
            # Record the result
            record = {
                'task_id': task_id,
                'worker_id': worker.worker_id,
                'algorithm': algorithm,
                'start_time': start_time,
                'end_time': end_time,
                'duration': end_time - start_time,
                'result': result
            }
            
            with self.results_lock:
                self.completed_tasks.append(record)
            
            if result.get('status') != 'error':
                print(f"[Scheduler] {task_id} COMPLETED on {worker.worker_id} in {record['duration']:.1f}s")
            else:
                print(f"[Scheduler] {task_id} FAILED on {worker.worker_id}: {result.get('error')}")

        finally:
            # Mark worker as idle
            with worker.task_lock:
                worker.current_task = None
    
    def run_cpu_based(self, num_tasks, submission_interval):
        """
        CPU-based load balancing: At each interval, assign a new task to the worker
        with the currently lowest CPU load.
        """
        print("\n### CPU-BASED LOAD BALANCING ###")
        print(f"Submitting {num_tasks} Pi calculation tasks every {submission_interval}s")
        print("Strategy: Assign each new task to the worker with the lowest CPU load\n")
        
        self.completed_tasks = []
        threads = []

        # NOTE: Adjust these values based on your worker machine's performance
        # to get a 15-20 second execution time.
        MIN_TERMS = 90_000_000 
        MAX_TERMS = 120_000_000

        for i in range(num_tasks):
            task_id = f"CPU_TASK_{i+1}"
            
            # Find the best worker at the moment of submission
            best_worker = None
            lowest_cpu = float('inf')
            
            for worker in self.workers:
                status = worker.get_status()
                # Ensure worker is connected and has a valid status
                if worker.is_connected and status and status['cpu_load'] < lowest_cpu:
                    lowest_cpu = status['cpu_load']
                    best_worker = worker
            
            # Fallback if no worker status is available
            if not best_worker:
                best_worker = self.workers[i % len(self.workers)] # Fallback to Round-Robin
                print(f"[Assign] Warning: Could not get worker statuses. Defaulting to {best_worker.worker_id}")
            
            terms = random.randint(MIN_TERMS, MAX_TERMS)
            
            # Start task in a separate thread
            t = threading.Thread(
                target=self._execute_task,
                args=(best_worker, task_id, terms, 'cpu_based')
            )
            t.daemon = True
            t.start()
            threads.append(t)
            
            with self.threads_lock:
                self.active_threads = threads

            # Wait for the interval before submitting the next task
            if i < num_tasks - 1:
                print(f"[Scheduler] Waiting {submission_interval}s for next submission...")
                time.sleep(submission_interval)
        
        print(f"\n[Manager] All {num_tasks} tasks submitted, waiting for completion...")
        for t in threads:
            t.join()
        
        return self.completed_tasks.copy()

    def run_round_robin(self, num_tasks, submission_interval):
        """
        Round-robin load balancing: Assign tasks sequentially to workers.
        """
        print("\n### ROUND-ROBIN LOAD BALANCING ###")
        print(f"Submitting {num_tasks} Pi calculation tasks every {submission_interval}s")
        print("Strategy: Assign tasks to workers in sequential order\n")
        
        self.completed_tasks = []
        threads = []

        # NOTE: Adjust these values based on your worker machine's performance
        MIN_TERMS = 90_000_000
        MAX_TERMS = 120_000_000

        for i in range(num_tasks):
            task_id = f"RR_TASK_{i+1}"
            
            # Select worker in round-robin fashion
            worker = self.workers[i % len(self.workers)]
            terms = random.randint(MIN_TERMS, MAX_TERMS)

            # Start task in a separate thread
            t = threading.Thread(
                target=self._execute_task,
                args=(worker, task_id, terms, 'round_robin')
            )
            t.daemon = True
            t.start()
            threads.append(t)
            
            with self.threads_lock:
                self.active_threads = threads
                
            # Wait for the interval before submitting the next task
            if i < num_tasks - 1:
                print(f"[Scheduler] Waiting {submission_interval}s for next submission...")
                time.sleep(submission_interval)

        print(f"\n[Manager] All {num_tasks} tasks submitted, waiting for completion...")
        for t in threads:
            t.join()
            
        return self.completed_tasks.copy()

    def run_experiment(self, num_tasks=20, submission_interval=10):
        """
        Run the full load balancing experiment, comparing CPU-based and Round-Robin strategies.
        """
        # Average expected task duration for theoretical calculation
        AVG_TASK_DURATION = 17.5 # (15 + 20) / 2
        
        print("\n" + "="*70)
        print("LOAD BALANCING EXPERIMENT")
        print("="*70)
        print(f"Total Tasks: {num_tasks}")
        print(f"Task Type: Pi Calculation (approx. {AVG_TASK_DURATION}s each)")
        print(f"Submission Interval: {submission_interval}s")
        print(f"Workers: {len(self.workers)}")
        
        # Theoretical time is complex; a simple estimate:
        theoretical_time = (num_tasks * AVG_TASK_DURATION) / len(self.workers)
        print(f"Simple Theoretical Best Time: {theoretical_time:.0f}s (ignoring intervals)")
        print("="*70 + "\n")
        
        # Test 1: CPU-based algorithm
        exp_start_cpu = time.time()
        cpu_results = self.run_cpu_based(num_tasks, submission_interval)
        cpu_duration = time.time() - exp_start_cpu
        
        print(f"\n[Result] CPU-based experiment completed in {cpu_duration:.1f}s\n")
        
        # Wait between tests to let workers settle
        print("Waiting 10s before next test...\n")
        time.sleep(10)
        
        # Test 2: Round-robin algorithm
        exp_start_rr = time.time()
        rr_results = self.run_round_robin(num_tasks, submission_interval)
        rr_duration = time.time() - exp_start_rr
        
        print(f"\n[Result] Round-robin experiment completed in {rr_duration:.1f}s\n")
        
        # Generate comparison report
        self._generate_report(cpu_results, rr_results, cpu_duration, rr_duration, 
                              num_tasks, AVG_TASK_DURATION, submission_interval)

    def _generate_report(self, cpu_results, rr_results, cpu_duration, rr_duration,
                         num_tasks, avg_duration, interval):
        """Generate and save a detailed comparison report"""
        print("\n" + "="*70)
        print("EXPERIMENT RESULTS")
        print("="*70)
        
        theoretical_time = (num_tasks * avg_duration) / len(self.workers)
        
        # Helper to analyze distribution
        def analyze_distribution(results):
            dist = {}
            for record in results:
                wid = record['worker_id']
                dist[wid] = dist.get(wid, 0) + 1
            return dist

        cpu_dist = analyze_distribution(cpu_results)
        rr_dist = analyze_distribution(rr_results)
        
        print("\n### CPU-Based Algorithm ###")
        print(f"Total Time: {cpu_duration:.1f}s")
        for wid in sorted(cpu_dist.keys()):
            count = cpu_dist[wid]
            print(f"  {wid}: {count} tasks ({count/len(cpu_results)*100:.1f}%)")
            
        print("\n### Round-Robin Algorithm ###")
        print(f"Total Time: {rr_duration:.1f}s")
        for wid in sorted(rr_dist.keys()):
            count = rr_dist[wid]
            print(f"  {wid}: {count} tasks ({count/len(rr_results)*100:.1f}%)")
            
        print("\n### Performance Comparison ###")
        if abs(cpu_duration - rr_duration) < 1.0:
            print("Performance is very similar.")
        elif cpu_duration < rr_duration:
            diff = rr_duration - cpu_duration
            print(f"CPU-based was {diff:.1f}s faster ({diff/rr_duration*100:.1f}% improvement).")
        else:
            diff = cpu_duration - rr_duration
            print(f"Round-robin was {diff:.1f}s faster ({diff/cpu_duration*100:.1f}% improvement).")

        # Save detailed report to JSON file
        report = {
            'experiment_info': {
                'total_tasks': num_tasks,
                'submission_interval': interval,
                'avg_task_duration': avg_duration,
                'workers': len(self.workers),
                'theoretical_best_time': theoretical_time
            },
            'cpu_based': {
                'total_duration': cpu_duration,
                'distribution': cpu_dist,
                'tasks': cpu_results
            },
            'round_robin': {
                'total_duration': rr_duration,
                'distribution': rr_dist,
                'tasks': rr_results
            }
        }
        
        report_filename = 'experiment_report.json'
        with open(report_filename, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"\n[Manager] Detailed report saved to {report_filename}")
        print("="*70 + "\n")

def main():
    """Main function to configure and run the manager"""
    # Configure your worker nodes here
    # Use the actual IP addresses of your worker machines
    worker_configs = [
        ('worker1', '10.128.0.2'),
        ('worker2', '10.128.0.3'),
        ('worker3', '10.128.0.4'),
        ('worker4', '10.128.0.6'),
    ]
    
    # Initialize the load balancer
    lb = LoadBalancer(worker_configs)
    
    # Connect to all workers
    if not lb.connect_all_workers():
        print("[Manager] Could not connect to any workers. Exiting.")
        return
    
    # Start the monitoring service
    lb.start_monitoring()
    
    # Allow time for initial status check before starting
    time.sleep(3)
    
    # Run the experiment
    lb.run_experiment(num_tasks=20, submission_interval=10)
    
    # Stop monitoring
    lb.stop_monitoring()
    
    print("[Manager] Experiment completed!")

if __name__ == '__main__':
    main()
