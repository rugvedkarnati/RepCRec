## Replicated Concurrency Control and Recovery

### RUGVED SAI KARNATI (rsk517) , TANMAY GATLE (tvg238)

This project is a course project of Advanced Database System in NYU taught by Prof. Dennis Shasha.

We simulated a tiny distributed database, complete with multi-version concurrency control, deadlock detection, replication, and failure recovery.

### ALGORITHMS:
1. Available Copies Algorithm for replication.
2. Strict Two-phase locking for Read/Write transactions.
3. Multiversion Read Consistency for Read-Only transactions.
4. DFS for deadlock cycle detection.

For more information refer to the design document.

### Run this project
```shell
	javac ./src/*.java
	java -cp ./src Main ./test/testfilename.txt
```

### Points to Note

We will ensure that when a transaction is waiting, it will not receive another operation.

Our application could prevent write starvation.

For example, if requests arrive in the order R(T1,x), R(T2,x), W(T3,x,73), R(T4,x) then, assuming no transaction aborts, first T1 and T2 will share read locks on x, then T3 will obtain a write lock on x and then T4 a read lock on x. Note that T4 does not skip in front of T3, as that might result in starvation for writes.

