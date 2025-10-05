# -*- coding:utf-8 -*-
"""
CISC 6935 Distributed Systems - Lab 1
Worker Node Implementation
Provides RPC services for task execution and resource monitoring
"""

import pickle
import time
import threading
import math
from multiprocessing.connection import Listener
from threading import Thread

# Import resource monitoring functions
def get_cpu_status(path='/proc/loadavg'):
    """
    Get CPU load average
    Returns: dict with 1min, 5min, 15min load averages
    """
    loadavg = {}
    with open(path, 'r') as f1:
        list_content = f1.read().split()
        loadavg['lavg_1'] = float(list_content[0])
        loadavg['lavg_5'] = float(list_content[1])
        loadavg['lavg_15'] = float(list_content[2])
    return loadavg

def get_memory_status(path='/proc/meminfo'):
    """
    Get memory usage information
    Returns: dict with memory metrics in KB
    """
    mem_dic = {}
    with open(path, 'r') as f2:
        lines = f2.readlines()
        for line in lines:
            name = line.strip().split(':')[0]
            data = line.split(":")[1].split()[0]
            mem_dic[name] = float(data)
    return mem_dic

# Global variables for task management
current_task = None
task_lock = threading.Lock()

def calculate_pi_ramanujan(duration_seconds=300):
    """
    Calculate PI using Ramanujan's formula
    Runs for specified duration (default 5 minutes)
    
    Ramanujan formula:
    1/π = (2√2/9801) * Σ[(4k)!(1103+26390k)] / [(k!)^4 * 396^(4k)]
    
    Args:
        duration_seconds: How long to run the calculation
    Returns: 
        dict with pi value, iterations, and duration
    """
    start_time = time.time()
    iterations = 0
    
    # Ramanujan's formula constants
    C = 2 * math.sqrt(2) / 9801
    
    while time.time() - start_time < duration_seconds:
        sum_val = 0
        # Calculate more terms each iteration to increase CPU load
        for k in range(100):  # Calculate 100 terms per iteration
            numerator = math.factorial(4*k) * (1103 + 26390*k)
            denominator = (math.factorial(k)**4) * (396**(4*k))
            sum_val += numerator / denominator
        
        pi_estimate = 1 / (C * sum_val)
        iterations += 100
        
        # Small sleep to prevent complete CPU saturation
        time.sleep(0.01)
    
    end_time = time.time()
    duration = end_time - start_time
    
    return {
        'pi_value': pi_estimate,
        'iterations': iterations,
        'duration': duration,
        'status': 'completed'
    }

def get_resource_status():
    """
    Get current resource usage and task status
    Returns: dict with CPU, memory, and idle status
    """
    cpu = get_cpu_status()
    memory = get_memory_status()
    
    # Calculate CPU usage percentage (simplified)
    # Load average < 0.05 indicates very low usage
    cpu_usage = cpu['lavg_1']  # 1-minute load average
    
    # Check if worker is idle
    # Idle: CPU < 5% (load < 0.05) AND no current task
    with task_lock:
        has_task = current_task is not None
    
    is_idle = (cpu_usage < 0.05) and (not has_task)
    
    return {
        'cpu_load': cpu_usage,
        'memory_total': memory.get('MemTotal', 0),
        'memory_available': memory.get('MemAvailable', 0),
        'is_idle': is_idle,
        'has_task': has_task,
        'timestamp': time.time()
    }

def execute_task(task_params):
    """
    Execute a computational task (calculate PI)
    Args:
        task_params: dict with 'task_id' and 'duration'
    Returns:
        dict with task results
    """
    global current_task
    
    task_id = task_params.get('task_id', 'unknown')
    duration = task_params.get('duration', 300)  # Default 5 minutes
    
    print(f"[Worker] Starting task {task_id} for {duration} seconds")
    
    # Mark task as running
    with task_lock:
        current_task = task_id
    
    try:
        # Execute PI calculation
        result = calculate_pi_ramanujan(duration)
        result['task_id'] = task_id
        
        print(f"[Worker] Completed task {task_id}")
        print(f"  PI estimate: {result['pi_value']}")
        print(f"  Iterations: {result['iterations']}")
        
        return result
        
    except Exception as e:
        print(f"[Worker] Error in task {task_id}: {str(e)}")
        return {
            'task_id': task_id,
            'status': 'error',
            'error': str(e)
        }
    finally:
        # Clear current task
        with task_lock:
            current_task = None

class RPCHandler:
    """
    Handles RPC requests from manager
    Manages function registry and connection handling
    """
    def __init__(self):
        self._functions = {}

    def register_function(self, func):
        """Register a function to be callable via RPC"""
        self._functions[func.__name__] = func
        print(f"[Worker] Registered function: {func.__name__}")

    def handle_connection(self, connection):
        """
        Handle incoming RPC connections
        Processes requests in a loop until connection closes
        """
        try:
            while True:
                # Receive RPC request
                func_name, args, kwargs = pickle.loads(connection.recv())
                print(f"[Worker] Received RPC call: {func_name}")
                
                # Execute requested function
                try:
                    r = self._functions[func_name](*args, **kwargs)
                    connection.send(pickle.dumps(r))
                except Exception as e:
                    print(f"[Worker] Error executing {func_name}: {str(e)}")
                    connection.send(pickle.dumps(e))
        except EOFError:
            print("[Worker] Connection closed")
            pass

def rpc_server(handler, address, authkey):
    """
    Start RPC server to accept connections from manager
    Uses multithreading to handle concurrent requests
    """
    sock = Listener(address, authkey=authkey)
    print(f"[Worker] RPC server started on {address}")
    
    while True:
        client = sock.accept()
        print(f"[Worker] Accepted connection from manager")
        
        # Handle each connection in a separate thread
        t = Thread(target=handler.handle_connection, args=(client,))
        t.daemon = True
        t.start()

if __name__ == '__main__':
    # Initialize RPC handler
    handler = RPCHandler()
    
    # Register available functions
    handler.register_function(get_resource_status)
    handler.register_function(execute_task)
    
    # Start RPC server
    # Listen on all interfaces (0.0.0.0) port 17000
    print("="*50)
    print("Worker Node Starting...")
    print("="*50)
    
    rpc_server(handler, ('0.0.0.0', 17000), authkey=b'peekaboo')
