# Advanced-Distributed-Systems-Lab1

## Resource Monitoring and Load Balancing System

A distributed system with one manager and multiple workers that monitors resources and intelligently assigns tasks using CPU-based load balancing.

---

## Quick Start

### 1. Clone Repository
```bash
git clone https://github.com/yuhanxu01/Advanced-Distributed-Systems-Lab1.git
cd Advanced-Distributed-Systems-Lab1
```

### 2. Start Workers (on each worker node)
```bash
python3 worker.py
```
**What it does:** Starts RPC server, provides resource monitoring APIs, executes PI calculation tasks.

### 3. Configure Manager
Edit `manager.py` and update worker IPs:
```python
worker_configs = [
    ('worker1', '10.128.0.2'),  # Update with your actual internal IPs
    ('worker2', '10.128.0.3'),
    ('worker3', '10.128.0.4'),
    ('worker4', '10.128.0.6'),
]
```

### 4. Run Manager (on manager node)
```bash
python3 manager.py
```
**What it does:** 
- Monitors worker resources every 5 seconds
- Assigns 20 tasks using CPU-based algorithm
- Assigns 20 tasks using Round-robin algorithm
- Generates comparison report: `experiment_report.json`

### 5. Test Concurrent Clients (optional)
```bash
python3 test_concurrent.py
```
**What it does:** Simulates 3 clients submitting tasks simultaneously.

---

## Configuration Parameters

In `manager.py` → `run_experiment()`:
- `num_tasks`: Number of tasks (default: 20)
- `interval`: Seconds between submissions (default: 10)
- `task_duration`: Task runtime in seconds (default: 300 = 5 minutes)

---

## System Architecture

```
Manager (node0)
  ├── Monitors workers (CPU, memory, idle status)
  ├── CPU-based load balancing (selects lowest CPU)
  └── Round-robin load balancing (equal distribution)

Workers (node1-4)
  ├── RPC server (multi-threaded)
  ├── Resource monitoring (CPU/memory)
  └── Task execution (Ramanujan PI calculation)
```

---

## Requirements
- Python 3.6+
- Linux environment (`/proc` filesystem)
- Network connectivity between nodes
- Port 17000 open

---

## Author
**Course:** CISC 6935 Distributed Systems  
**Lab:** Resource Monitoring and Load Balancing
