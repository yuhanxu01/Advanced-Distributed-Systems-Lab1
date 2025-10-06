# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Fixed Version
Monitors workers and assigns tasks using intelligent load balancing
"""

import pickle
import time
import threading
from multiprocessing.connection import Client
from datetime import datetime
import json

class RPCProxy:
    """
    Proxy for making RPC calls to worker nodes
    """
    def __init__(self, connection):
        self._connection = connection
        self._lock = threading.Lock()  # Add lock for thread safety
        
    def __getattr__(self, name):
        def do_rpc(*args, **kwargs):
            with self._lock:  # Ensure thread-safe communication
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
        except ConnectionError as e:
            print(f"[Manager] Connection error with worker {self.worker_id}: {str(e)}")
            self.is_connected = False
            # Try to reconnect
            if self.reconnect():
                try:
                    status = self.proxy.get_resource_status()
                    self.last_status = status
                    return status
                except:
                    return None
            return None
        except Exception as e:
            print(f"[Manager] Error getting status from worker {self.worker_id}: {str(e)}")
            self.is_connected = False
            return None
    
    def assign_task(self, task_params):
        """Assign a task to this worker"""
        if not self.is_connected:
            # Try to reconnect before giving up
            if not self.reconnect():
                return {'status': 'error', 'error': 'not connected'}
        
        try:
            result = self.proxy.execute_task(task_params)
            return result
        except ConnectionError as e:
            print(f"[Manager] Connection lost during task assignment to worker {self.worker_id}")
            self.is_connected = False
            return {'status': 'error', 'error': 'connection lost'}
        except Exception as e:
            print(f"[Manager] Error assigning task to worker {self.worker_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}

class LoadBalancer:
    """
    Manages workers and implements load balancing algorithms
    """
    def __init__(self, worker_configs):
        """
        Initialize load balancer with worker configurations
        Args:
            worker_configs: list of (worker_id, host) tuples
        """
        self.workers = []
        self.monitoring_active = False
        self.monitor_thread = None
        self.monitor_interval = 5  # Monitor every 5 seconds
        self.round_robin_index = 0
        self.task_assignments = []  # Track all task assignments
        
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
            time.sleep(0.5)  # Small delay between connections
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
        print("[Manager] Stopped resource monitoring")
    
    def _monitor_loop(self):
        """Continuous monitoring loop (runs in background thread)"""
        while self.monitoring_active:
            print(f"\n[Monitor] {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            print("-" * 60)
            
            for worker in self.workers:
                status = worker.get_status()
                if status:
                    idle_str = "IDLE" if status['is_idle'] else "BUSY"
                    task_str = "YES" if status['has_task'] else "NO"
                    print(f"Worker {worker.worker_id} ({worker.host}): "
                          f"CPU={status['cpu_load']:.4f} | "
                          f"Status={idle_str} | "
                          f"Has Task={task_str}")
                else:
                    print(f"Worker {worker.worker_id}: DISCONNECTED")
            
            print("-" * 60)
            time.sleep(self.monitor_interval)
    
    def get_idle_workers(self):
        """
        Get list of idle workers (CPU < 5% and no current task)
        Returns: list of WorkerProxy objects
        """
        idle_workers = []
        for worker in self.workers:
            status = worker.get_status()
            if status and status['is_idle']:
                idle_workers.append(worker)
        return idle_workers
    
    def select_worker_by_cpu(self):
        """
        Intelligent load balancing: select worker with lowest CPU usage
        Returns: WorkerProxy object or None
        """
        # Get current status of all workers
        worker_loads = []
        for worker in self.workers:
            status = worker.get_status()
            if status and worker.is_connected:
                worker_loads.append((worker, status['cpu_load']))
        
        if not worker_loads:
            print("[Manager] No available workers!")
            return None
        
        # Sort by CPU load (ascending) and select the one with lowest load
        worker_loads.sort(key=lambda x: x[1])
        selected_worker = worker_loads[0][0]
        
        print(f"[Manager] Selected worker {selected_worker.worker_id} "
              f"(CPU load: {worker_loads[0][1]:.4f})")
        
        return selected_worker
    
    def select_worker_round_robin(self):
        """
        Round-robin load balancing: cycle through workers
        Returns: WorkerProxy object or None
        """
        if not self.workers:
            return None
        
        # Find next available worker
        attempts = 0
        while attempts < len(self.workers):
            worker = self.workers[self.round_robin_index]
            self.round_robin_index = (self.round_robin_index + 1) % len(self.workers)
            
            if worker.is_connected:
                print(f"[Manager] Round-robin selected worker {worker.worker_id}")
                return worker
            
            attempts += 1
        
        print("[Manager] No available workers in round-robin!")
        return None
    
    def assign_task(self, task_id, duration, algorithm='cpu_based'):
        """
        Assign a task to a worker using specified algorithm
        Args:
            task_id: unique task identifier
            duration: task duration in seconds
            algorithm: 'cpu_based' or 'round_robin'
        Returns:
            dict with assignment info
        """
        # Select worker based on algorithm
        if algorithm == 'cpu_based':
            worker = self.select_worker_by_cpu()
        elif algorithm == 'round_robin':
            worker = self.select_worker_round_robin()
        else:
            print(f"[Manager] Unknown algorithm: {algorithm}")
            return None
        
        if not worker:
            print(f"[Manager] Failed to assign task {task_id}: no worker available")
            return None
        
        # Record assignment
        assignment = {
            'task_id': task_id,
            'worker_id': worker.worker_id,
            'worker_host': worker.host,
            'algorithm': algorithm,
            'assigned_at': time.time(),
            'cpu_load_at_assignment': worker.last_status['cpu_load'] if worker.last_status else None
        }
        
        print(f"\n[Manager] Assigning task {task_id} to worker {worker.worker_id}")
        print(f"  Algorithm: {algorithm}")
        print(f"  Duration: {duration}s")
        
        # Assign task in separate thread to avoid blocking
        def async_assign():
            task_params = {'task_id': task_id, 'duration': duration}
            result = worker.assign_task(task_params)
            assignment['completed_at'] = time.time()
            assignment['result'] = result
            if result.get('status') != 'error':
                print(f"[Manager] Task {task_id} completed on worker {worker.worker_id}")
            else:
                print(f"[Manager] Task {task_id} failed on worker {worker.worker_id}: {result.get('error')}")
        
        t = threading.Thread(target=async_assign)
        t.daemon = True
        t.start()
        
        self.task_assignments.append(assignment)
        return assignment
    
    def run_experiment(self, num_tasks=20, interval=10, task_duration=300):
        """
        Run load balancing experiment
        Args:
            num_tasks: number of tasks to run (default 20)
            interval: seconds between task submissions (default 10)
            task_duration: duration of each task in seconds (default 300 = 5 min)
        """
        print("\n" + "="*60)
        print("LOAD BALANCING EXPERIMENT")
        print("="*60)
        print(f"Tasks: {num_tasks}")
        print(f"Interval: {interval}s")
        print(f"Task Duration: {task_duration}s")
        print("="*60 + "\n")
        
        # Test 1: CPU-based algorithm
        print("\n### TEST 1: CPU-BASED LOAD BALANCING ###\n")
        cpu_assignments = []
        for i in range(num_tasks):
            task_id = f"CPU_TASK_{i+1}"
            assignment = self.assign_task(task_id, task_duration, algorithm='cpu_based')
            if assignment:
                cpu_assignments.append(assignment)
            time.sleep(interval)
        
        # Wait for all tasks to complete
        print("\n[Manager] Waiting for all CPU-based tasks to complete...")
        time.sleep(task_duration + 30)  # Wait for tasks + buffer
        
        # Test 2: Round-robin algorithm
        print("\n### TEST 2: ROUND-ROBIN LOAD BALANCING ###\n")
        rr_assignments = []
        for i in range(num_tasks):
            task_id = f"RR_TASK_{i+1}"
            assignment = self.assign_task(task_id, task_duration, algorithm='round_robin')
            if assignment:
                rr_assignments.append(assignment)
            time.sleep(interval)
        
        # Wait for all tasks to complete
        print("\n[Manager] Waiting for all round-robin tasks to complete...")
        time.sleep(task_duration + 30)
        
        # Generate comparison report
        self._generate_report(cpu_assignments, rr_assignments)
    
    def _generate_report(self, cpu_assignments, rr_assignments):
        """Generate comparison report between algorithms"""
        print("\n" + "="*60)
        print("EXPERIMENT RESULTS")
        print("="*60)
        
        # Analyze CPU-based distribution
        print("\n### CPU-Based Algorithm ###")
        cpu_dist = {}
        for assignment in cpu_assignments:
            wid = assignment['worker_id']
            cpu_dist[wid] = cpu_dist.get(wid, 0) + 1
        
        for wid, count in sorted(cpu_dist.items()):
            print(f"  Worker {wid}: {count} tasks ({count/len(cpu_assignments)*100:.1f}%)")
        
        # Analyze Round-robin distribution
        print("\n### Round-Robin Algorithm ###")
        rr_dist = {}
        for assignment in rr_assignments:
            wid = assignment['worker_id']
            rr_dist[wid] = rr_dist.get(wid, 0) + 1
        
        for wid, count in sorted(rr_dist.items()):
            print(f"  Worker {wid}: {count} tasks ({count/len(rr_assignments)*100:.1f}%)")
        
        # Save detailed report to file
        report = {
            'cpu_based': cpu_assignments,
            'round_robin': rr_assignments,
            'summary': {
                'cpu_distribution': cpu_dist,
                'rr_distribution': rr_dist
            }
        }
        
        with open('experiment_report.json', 'w') as f:
            json.dump(report, f, indent=2)
        
        print("\n[Manager] Detailed report saved to experiment_report.json")
        print("="*60 + "\n")

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
    
    # Wait for monitoring to stabilize
    time.sleep(10)
    
    # Run experiment
    lb.run_experiment(num_tasks=20, interval=10, task_duration=300)
    
    # Stop monitoring
    lb.stop_monitoring()
    
    print("[Manager] Experiment completed!")

if __name__ == '__main__':
    main()
