# MIT6.824-labs

## This repository contains the code for each lab of MIT6.824 Distributed systems.
- Lab1 Map-Reduce system:  
  - Implemented Coordinator class who creates the tasks, gather and handle the response from workers
  - Implemented Worker class in charge of executing Map and Reduce task and return the result to Coordinator.
- Lab2 Raft Consensus Algorithms:
  - Realized the algorithm explained in [Raft](https://raft.github.io/raft.pdf)
  - Implemented a stable leader election to guarantee that there is exactly one leader per term.(1A)
  - Implemented the log replication mechanism then increase the efficiency of solving log confliction by updating the logs by term.
  - Optimized the performance of the system through writing the persistent data in batch.
