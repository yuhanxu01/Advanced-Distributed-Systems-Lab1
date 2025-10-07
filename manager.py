# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Manager Node Implementation - Fixed Series Scheduler (Pi Computation)

Changes:
- Deterministic 5–40s task series (no randomness).
- Adversarial order for RR to show imbalance.
- CPU-based scheduling picks longest-first to least-busy worker.
- Fault tolerance: auto-retry and reassign on worker failure.
"""

import pickle
import time
import threading
from multiprocessing.connection import Client
from datetime import datetime
import json
from collections import deque


# --------------------- RPC Proxy ---------------------
class RPCProxy:
    """Thread-safe RPC proxy"""
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
    """Worker node wrapper with status and connection"""
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

    def disconnect(self):
        try:
            if self._connection:
                self._connection.close()
        except Exception:
            pass
        self.is_connected = False
        self.proxy = None
        print(f"[Manager] Marked {self.worker_id} as DISCONNECTED")

    def get_status(self):
        if not self.is_connected or not self.proxy:
            return None
        try:
            status = self.proxy.get_resource_status()
            self.last_status = status
            return status
        except Exception:
            self.disconnect()
            return None

    def is_busy(self):
        with self.task_lock:
            return self.current_task is not None

    def assign_task(self, task_params):
        if not self.is_connected or not self.proxy:
            return {'status': 'error', 'error': 'not connected'}
        try:
            result = self.proxy.execute_task(task_params)
            return result
        except Exception as e:
            self.disconnect()
            return {'status': 'error', 'error': f'{e}'}


# --------------------- Load Balancer ---------------------
class LoadBalancer:
    """Manages workers and implements RR vs CPU-based scheduling on a fixed series"""
    def __init__(self, worker_configs):
        self.workers = [WorkerProxy(wid, host) for wid, host in worker_configs]
        self.monitor_thread = None
        self.monitoring_active = False
        self.completed_tasks = []
        self.results_lock = threading.Lock()
        self.MAX_RETRIES = 3
        print(f"[Manager] Initialized with {len(self.workers)} workers")

    # ---------- connections and monitoring ----------
    def connect_all_workers(self):
        print("[Manager] Connecting to all workers...")
        success = 0
        for w in self.workers:
            if w.connect():
                success += 1
            time.sleep(0.2)
        print(f"[Manager] Connected to {success}/{len(self.workers)} workers")
        return success > 0

    def start_monitoring(self):
        self.monitoring_active = True
        self.monitor_thread = threading.Thread(target=self._monitor_loop, daemon=True)
        self.monitor_thread.start()

    def stop_monitoring(self):
        self.monitoring_active = False
        if self.monitor_thread:
            self.monitor_thread.join()

    def _monitor_loop(self):
        while self.monitoring_active:
            print(f"\n[Monitor] {datetime.now().strftime('%H:%M:%S')}")
            print("-" * 60)
            for w in self.workers:
                status = w.get_status()
                if status:
                    state = "BUSY" if w.is_busy() else "IDLE"
                    cpu = status.get('cpu_load', 1.0)
                    print(f"{w.worker_id}: CPU={cpu:.3f} | {state}")
                else:
                    print(f"{w.worker_id}: DISCONNECTED")
            time.sleep(2)

    # ---------- helpers ----------
    def _connected_workers(self):
        return [w for w in self.workers if w.is_connected]

    def _choose_best_worker(self, exclude=None):
        exclude = exclude or set()
        candidates = [w for w in self._connected_workers() if w not in exclude]
        if not candidates:
            return None
        idle = [w for w in candidates if not w.is_busy()]
        pool = idle if idle else candidates
        # lower cpu_load preferred; missing status treated as high load
        return min(pool, key=lambda w: (w.last_status or {'cpu_load': 1.0})['cpu_load'])

    # ---------- task execution with retry ----------
    def _execute_task(self, worker, task, algorithm):
        task_id = task['task_id']
        duration = task['duration']
        retries = task['retries']

        with worker.task_lock:
            worker.current_task = task_id
        try:
            print(f"[Scheduler] Assign {task_id}({duration}s) -> {worker.worker_id} [{algorithm}] try:{retries+1}")
            start = time.time()
            params = {'task_id': task_id, 'duration': duration, 'task_type': 'pi'}
            result = worker.assign_task(params)
            end = time.time()

            if isinstance(result, dict) and result.get('status') == 'error':
                raise RuntimeError(result.get('error', 'unknown error'))

            record = {
                'task_id': task_id,
                'worker_id': worker.worker_id,
                'duration_wall': end - start,
                'start_time': start,
                'end_time': end,
                'task_duration_param': duration,
                'result': result,
                'algorithm': algorithm,
                'retries': retries
            }
            with self.results_lock:
                self.completed_tasks.append(record)
            print(f"[Scheduler] {task_id} finished on {worker.worker_id} ({end - start:.1f}s)")
            return True
        except Exception as e:
            print(f"[Scheduler] {task_id} failed on {worker.worker_id}: {e}")
            # choose alternate and retry if possible
            task['retries'] += 1
            if task['retries'] >= self.MAX_RETRIES:
                with self.results_lock:
                    self.completed_tasks.append({
                        'task_id': task_id,
                        'worker_id': worker.worker_id,
                        'duration_wall': None,
                        'start_time': None,
                        'end_time': None,
                        'task_duration_param': duration,
                        'result': {'status': 'error', 'error': f'permanent failure after {self.MAX_RETRIES} retries'},
                        'algorithm': algorithm,
                        'retries': task['retries']
                    })
                print(f"[Scheduler] {task_id} giving up after {self.MAX_RETRIES} retries")
                return False

            alt = self._choose_best_worker(exclude={worker})
            if not alt:
                print("[Scheduler] No alternate worker available, will retry later")
                # simple backoff then try any connected worker
                time.sleep(1.0)
                alt = self._choose_best_worker()
                if not alt:
                    print("[Scheduler] Still no worker; push to background retry skipped (demo mode)")
                    return False
            # retry on alternate in the same thread
            return self._execute_task(alt, task, algorithm)
        finally:
            with worker.task_lock:
                worker.current_task = None

    # ---------- fixed series ----------
    @staticmethod
    def build_fixed_series_20():
        """
        Adversarial RR order across 4 workers (i % 4):
          W1 gets heavy: indices 0,4,8,12,16 -> 40,39,38,37,36
          W2 gets light: indices 1,5,9,13,17 -> 5,6,7,8,9
          W3 medium:     25,26,27,28,29
          W4 med-high:   30,31,32,33,34
        """
        series = [40, 5, 25, 30,
                  39, 6, 26, 31,
                  38, 7, 27, 32,
                  37, 8, 28, 33,
                  36, 9, 29, 34]
        assert len(series) == 20 and min(series) >= 5 and max(series) <= 40
        return series

    # ---------- CPU-based scheduling (longest-first to least-busy) ----------
    def run_cpu_based(self, durations, interval=5):
        print("\n### CPU-BASED SCHEDULING (fixed series) ###")
        self.completed_tasks = []
        threads = []

        # longest-first uses same multiset as RR
        pending = deque(sorted(durations, reverse=True))

        i = 0
        while pending:
            best = self._choose_best_worker()
            if not best:
                print("[CPU] No connected workers. Waiting...")
                time.sleep(1.0)
                continue

            dur = pending.popleft()
            task = {'task_id': f"CPU_TASK_{i+1}_{dur}s", 'duration': dur, 'retries': 0}
            t = threading.Thread(target=self._execute_task, args=(best, task, 'cpu_based'), daemon=True)
            t.start()
            threads.append(t)
            print(f"[CPU] Launch {task['task_id']} -> {best.worker_id}")
            i += 1
            time.sleep(interval)

        print("\n[Manager] Waiting CPU-based tasks to finish...")
        for t in threads:
            t.join()
        return self.completed_tasks.copy()

    # ---------- Round-robin scheduling (uses adversarial order) ----------
    def run_round_robin(self, durations, interval=5):
        print("\n### ROUND ROBIN SCHEDULING (fixed series adversarial order) ###")
        self.completed_tasks = []
        threads = []

        n_workers = len(self.workers)
        for i, dur in enumerate(durations):
            # nominal RR target
            target = self.workers[i % n_workers]
            # if target down, pick next connected to keep moving
            chosen = target if target.is_connected else self._choose_best_worker()
            if not chosen:
                print("[RR] No connected workers. Waiting...")
                time.sleep(1.0)
                chosen = self._choose_best_worker()
                if not chosen:
                    print("[RR] Skipping launch due to no workers")
                    continue

            task = {'task_id': f"RR_TASK_{i+1}_{dur}s", 'duration': dur, 'retries': 0}
            t = threading.Thread(target=self._execute_task, args=(chosen, task, 'round_robin'), daemon=True)
            t.start()
            threads.append(t)
            print(f"[RR] Launch {task['task_id']} -> {chosen.worker_id}")
            time.sleep(interval)

        print("\n[Manager] Waiting RR tasks to finish...")
        for t in threads:
            t.join()
        return self.completed_tasks.copy()

    # ---------- experiment ----------
    def run_experiment(self):
        print("\n" + "="*70)
        print("LOAD BALANCING EXPERIMENT - Fixed Task Series")
        print("="*70)

        series = self.build_fixed_series_20()

        exp_start = time.time()
        cpu_results = self.run_cpu_based(series, interval=5)
        cpu_time = time.time() - exp_start
        print(f"\n[Result] CPU-based total time: {cpu_time:.1f}s\n")

        print("Pause 5s before Round-Robin...\n")
        time.sleep(5)

        exp_start = time.time()
        rr_results = self.run_round_robin(series, interval=5)
        rr_time = time.time() - exp_start
        print(f"\n[Result] Round-Robin total time: {rr_time:.1f}s\n")

        self._generate_report(cpu_results, rr_results, cpu_time, rr_time)

    def _generate_report(self, cpu_results, rr_results, cpu_time, rr_time):
        report = {
            'cpu_based': {'duration': cpu_time, 'tasks': cpu_results},
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
    time.sleep(0.5)
    lb.run_experiment()
    lb.stop_monitoring()
    print("[Manager] Experiment completed!")


if __name__ == '__main__':
    main()
