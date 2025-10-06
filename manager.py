# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Fixed Interval Scheduler (Pi Computation)
"""

import pickle
import time
import random
import threading
from multiprocessing.connection import Client
from datetime import datetime
import json
from collections import deque


# --------------------- RPC Proxy ---------------------
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


# --------------------- Worker Proxy ---------------------
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
        self.current_task = None
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
        except Exception:
            self.is_connected = False
            return None

    def is_busy(self):
        """Check if worker is currently executing a task (manager-side tracking)"""
        with self.task_lock:
            return self.current_task is not None

    def assign_task(self, task_params):
        """Assign a task to this worker"""
        if not self.is_connected:
            return {'status': 'error', 'error': 'not connected'}
        try:
            result = self.proxy.execute_task(task_params)
            return result
        except Exception as e:
            print(f"[Manager] Error assigning task to worker {self.worker_id}: {str(e)}")
            return {'status': 'error', 'error': str(e)}


# --------------------- Load Balancer ---------------------
class LoadBalancer:
    """Manages workers and implements interval-based and round-robin scheduling"""
    def __init__(self, worker_configs):
        self.workers = [WorkerProxy(wid, host) for wid, host in worker_configs]
        self.monitor_thread = None
        self.monitoring_active = False
        self.completed_tasks = []
        self.results_lock = threading.Lock()
        self.active_threads = []
        self.threads_lock = threading.Lock()
        print(f"[Manager] Initialized with {len(self.workers)} workers")

    def connect_all_workers(self):
        """Connect to all worker nodes"""
        print("[Manager] Connecting to all workers...")
        success = 0
        for worker in self.workers:
            if worker.connect():
                success += 1
            time.sleep(0.5)
        print(f"[Manager] Connected to {success}/{len(self.workers)} workers")
        return success > 0

    def start_monitoring(self):
        """Start monitoring thread"""
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()

    def _monitor_loop(self):
        """Background resource monitoring"""
        while self.monitoring_active:
            print(f"\n[Monitor] {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 60)
            for w in self.workers:
                status = w.get_status()
                if status:
                    state = "BUSY" if w.is_busy() else "IDLE"
                    print(f"{w.worker_id}: CPU={status['cpu_load']:.3f} | {state}")
                else:
                    print(f"{w.worker_id}: DISCONNECTED")
            time.sleep(2)

    def _execute_task(self, worker, task_id, duration, algorithm):
        """Execute a single task on a worker"""
        with worker.task_lock:
            worker.current_task = task_id
        try:
            print(f"[Scheduler] Assigning {task_id} to {worker.worker_id} ({algorithm})")
            start = time.time()
            task_params = {'task_id': task_id, 'duration': duration, 'task_type': 'pi'}
            result = worker.assign_task(task_params)
            end = time.time()

            record = {
                'task_id': task_id,
                'worker_id': worker.worker_id,
                'duration': end - start,
                'start_time': start,
                'end_time': end,
                'result': result,
                'algorithm': algorithm
            }
            with self.results_lock:
                self.completed_tasks.append(record)
            print(f"[Scheduler] {task_id} finished on {worker.worker_id} ({end - start:.1f}s)")
        finally:
            with worker.task_lock:
                worker.current_task = None

    # -----------------------------------------------------------------
    # NEW: Fixed-Interval Task Launch (each 10s, random 15–20s runtime)
    # -----------------------------------------------------------------
    def run_fixed_interval(self, num_tasks=20, interval=10):
        """
        Launch tasks every `interval` seconds.
        Each task computes pi and runs for random 15–20 seconds.
        """
        print("\n### FIXED INTERVAL SCHEDULING (Pi Computation) ###")
        print(f"Launching {num_tasks} tasks at {interval}s interval.\n")

        threads = []
        self.completed_tasks = []

        for i in range(num_tasks):
            task_id = f"PI_TASK_{i+1}"
            duration = random.randint(15, 20)  # simulate compute time
            # pick least busy worker
            idle_workers = [w for w in self.workers if not w.is_busy()]
            if not idle_workers:
                idle_workers = self.workers
            best_worker = min(idle_workers, key=lambda w: (w.last_status or {'cpu_load': 1})['cpu_load'])

            t = threading.Thread(
                target=self._execute_task,
                args=(best_worker, task_id, duration, 'fixed_interval')
            )
            t.daemon = True
            t.start()
            threads.append(t)

            print(f"[Interval] Task {task_id} launched -> {best_worker.worker_id} | Duration {duration}s")
            time.sleep(interval)

        print("\n[Manager] Waiting for all interval-launched tasks to finish...")
        for t in threads:
            t.join()

        return self.completed_tasks.copy()

    def run_round_robin(self, num_tasks, duration):
        """Round-robin scheduler"""
        print("\n### ROUND ROBIN SCHEDULING ###")
        threads = []
        self.completed_tasks = []
        for i in range(num_tasks):
            worker = self.workers[i % len(self.workers)]
            task_id = f"RR_TASK_{i+1}"
            task_dur = random.randint(15, 20)
            t = threading.Thread(
                target=self._execute_task,
                args=(worker, task_id, task_dur, 'round_robin')
            )
            t.daemon = True
            t.start()
            threads.append(t)
            print(f"[Assign] {task_id} -> {worker.worker_id} ({task_dur}s)")
            print(f"[Assign] {task_id} -> {worker.worker_id} ({task_dur}s)")
            time.sleep(interval)

        for t in threads:
            t.join()
        return self.completed_tasks.copy()

    def run_experiment(self):
        """Compare fixed-interval vs round-robin scheduling"""
        print("\n" + "="*70)
        print("LOAD BALANCING EXPERIMENT - Pi Computation")
        print("="*70)

        exp_start = time.time()
        fixed_results = self.run_fixed_interval(num_tasks=20, interval=10)
        fixed_time = time.time() - exp_start
        print(f"\n[Result] Fixed-interval total time: {fixed_time:.1f}s\n")

        print("Waiting 10s before running Round-Robin...\n")
        time.sleep(10)

        exp_start = time.time()
        rr_results = self.run_round_robin(num_tasks=20, duration=10)
        rr_time = time.time() - exp_start
        print(f"\n[Result] Round-Robin total time: {rr_time:.1f}s\n")

        self._generate_report(fixed_results, rr_results, fixed_time, rr_time)

    def _generate_report(self, fixed_results, rr_results, fixed_time, rr_time):
        """Save JSON report"""
        report = {
            'fixed_interval': {'duration': fixed_time, 'tasks': fixed_results},
            'round_robin': {'duration': rr_time, 'tasks': rr_results},
        }
        with open('experiment_report.json', 'w') as f:
            json.dump(report, f, indent=2, default=str)
        print("[Manager] Report saved to experiment_report.json")


# --------------------- Main Entry ---------------------
def main():
    worker_configs = [
        ('worker1', '10.128.0.2'),
        ('worker2', '10.128.0.3'),
        ('worker3', '10.128.0.4'),
        ('worker4', '10.128.0.6'),
    ]

    lb = LoadBalancer(worker_configs)
    if not lb.connect_all_workers():
        print("[Manager] Failed to connect to workers.")
        return

    lb.start_monitoring()
    time.sleep(3)
    lb.run_experiment()
    lb.stop_monitoring()
    print("[Manager] Experiment completed!")


if __name__ == '__main__':
    main()
