# Advanced Distributed Systems - Lab 1
## Resource Monitoring and Load Balancing System

Distributed system with intelligent load balancing: one manager monitors multiple workers and assigns tasks based on CPU usage.

---

## How to Run

### Start Workers
```bash
python3 worker.py
```

### Run Manager
```bash
python3 manager.py
```

### Test Concurrent Clients (optional)
```bash
python3 test_concurrent.py
```

---

## What Each Component Does

### **worker.py**
- Runs RPC server (multi-threaded for concurrent requests)
- Monitors CPU load and memory usage
- Executes computational tasks (Ramanujan PI calculation)
- Reports idle status when CPU < 5% and no active task

### **manager.py**
- Monitors all workers every 5 seconds
- Assigns tasks using two algorithms:
  - **CPU-based:** Selects worker with lowest CPU load
  - **Round-robin:** Distributes tasks evenly
- Runs experiment with 20 tasks per algorithm
- Generates comparison report (`experiment_report.json`)

### **test_concurrent.py**
- Simulates 3 clients submitting tasks simultaneously
- Tests concurrent request handling

---

## Key Features
✅ Real-time resource monitoring  
✅ RPC-based communication (not raw sockets)  
✅ Multi-threaded concurrent task handling  
✅ Intelligent CPU-based load balancing  
✅ Idle worker detection  
✅ Load balancing algorithm comparison

---

**Course:** CISC 6935 Distributed Systems
