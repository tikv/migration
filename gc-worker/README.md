*GC-Worker*

**GC-Worker** is a module for TiKV to advance the garbage collection progress. It's designed to be high available with help of etcd and work with TiKV cluster. It's designed for both TxnKV and RawKV modes. The work progress is:  
1. Start several `GC-Worker` process at different nodes with the given etcd server advertisement address. (Singular is recommended.)
2. All `GC-Worker` nodes campaign for leader, only one can succeed to be the leader.
3. The leader node get the `service safepoint` from `PD` and update calculate the final `gc safepoint`, then update it to `PD`.
4. If leader node crash, follower node will become the leader and do step 3.

*Building*

To build binary and run test:
```
$ make
```
Notice GC-Worker supports building with Go version Go >= 1.18  

When GC-Worker is built successfully, you can find binary in the bin directory.

*Quick Start*

```
# deploy the TiKV cluster
tiup playground --mode tikv-slim

# start GC-Worker process in three different nodes
bin/gc-worker --name gc-worker0 --pd pd0:2379 \
    --L info --log-file "/logs/gc-worker0.log"

```