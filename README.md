# MIT6.5840-golabs-2026
完成MIT6.5840-golabs-2026中的实验(课程网站：https://pdos.csail.mit.edu/6.824/index.html)     
原始代码fork来自：git://g.csail.mit.edu/6.5840-golabs-2026

## Lab1 MapReduce
执行100次测试均通过:
``` txt
=== 第 100 次执行 make mr ===
go build -race -o main/mrsequential main/mrsequential.go
go build -race -o main/mrcoordinator main/mrcoordinator.go
go build -race -o main/mrworker main/mrworker.go&
(cd mrapps && go build -race -buildmode=plugin wc.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin indexer.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin mtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin rtiming.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin jobcount.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin early_exit.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin crash.go) || exit 1
(cd mrapps && go build -race -buildmode=plugin nocrash.go) || exit 1
cd mr; go test -v -race 
=== RUN   TestWc
--- PASS: TestWc (13.43s)
=== RUN   TestIndexer
--- PASS: TestIndexer (7.26s)
=== RUN   TestMapParallel
--- PASS: TestMapParallel (8.05s)
=== RUN   TestReduceParallel
--- PASS: TestReduceParallel (11.07s)
=== RUN   TestJobCount
--- PASS: TestJobCount (12.05s)
=== RUN   TestEarlyExit
--- PASS: TestEarlyExit (10.21s)
=== RUN   TestCrashWorker
--- PASS: TestCrashWorker (37.21s)
PASS
ok      6.5840/mr       100.301s
```
## Lab2 Key/Value Server
测试结果：
```
=== 第 100 次执行 make kvsrv1 ===
go build -race -o main/kvsrv1d main/kvsrv1d.go
cd kvsrv1 && go test -v -race  
=== RUN   TestReliablePut
One client and reliable Put (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     5 #Ops    5
--- PASS: TestReliablePut (0.17s)
=== RUN   TestPutConcurrentReliable
Test: many clients racing to put values to the same key (reliable network)...
  ... Passed --  time  1.6s #peers 1 #RPCs  1845 #Ops 3690
--- PASS: TestPutConcurrentReliable (1.71s)
=== RUN   TestMemPutManyClientsReliable
Test: memory use many put clients (reliable network)...
  ... Passed --  time 38.3s #peers 1 #RPCs 20000 #Ops 20000
--- PASS: TestMemPutManyClientsReliable (73.55s)
=== RUN   TestUnreliableNet
One client (unreliable network)...
  ... Passed --  time  9.7s #peers 1 #RPCs   262 #Ops  424
--- PASS: TestUnreliableNet (9.86s)
PASS
ok      6.5840/kvsrv1   86.342s
```
```
=== 第 100 次执行 make lock1 ===
go build -race -o main/kvsrv1d main/kvsrv1d.go
cd kvsrv1/lock; go test -v -race 
=== RUN   TestReliableBasic
Test: a single Acquire and Release (reliable network)...
  ... Passed --  time  0.0s #peers 1 #RPCs     4 #Ops    4
--- PASS: TestReliableBasic (0.16s)
=== RUN   TestReliableNested
Test: one client, two locks (reliable network)...
  ... Passed --  time  0.1s #peers 1 #RPCs    20 #Ops   20
--- PASS: TestReliableNested (0.20s)
=== RUN   TestOneClientReliable
Test: 1 lock clients (reliable network)...
  ... Passed --  time  2.0s #peers 1 #RPCs   464 #Ops  464
--- PASS: TestOneClientReliable (2.15s)
=== RUN   TestManyClientsReliable
Test: 10 lock clients (reliable network)...
  ... Passed --  time 10.2s #peers 1 #RPCs   683 #Ops  683
--- PASS: TestManyClientsReliable (10.37s)
=== RUN   TestOneClientUnreliable
Test: 1 lock clients (unreliable network)...
  ... Passed --  time  2.3s #peers 1 #RPCs    57 #Ops   46
--- PASS: TestOneClientUnreliable (2.43s)
=== RUN   TestManyClientsUnreliable
Test: 10 lock clients (unreliable network)...
  ... Passed --  time  8.8s #peers 1 #RPCs   242 #Ops  193
--- PASS: TestManyClientsUnreliable (8.93s)
PASS
ok      6.5840/kvsrv1/lock      25.270s
```