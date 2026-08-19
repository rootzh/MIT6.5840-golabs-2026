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

## Lab3 Raft1
### 遇到的问题
```
问题：选举很不稳定，在选出leader后，appendentry没有发送，但是新一轮的选举又开始了。
解决方案：成为Leader之后马上发送appendEntry消息
```

```
问题：
info: wrote visualization to /tmp/porcupine-3121644360.html
    test.go:258: commit index=9 server=0 5951278337898093599 != server=0 6194479875776984785
--- FAIL: TestFigure83C (64.04s)
解决方案：更新commitIndex的时候需要校验log的term是否为当前term
```

```
问题：
===========TestBackup3B:566 cost:2.683840292s
===========TestBackup3B:571 cost:13.628225748s
写入50条数据需要10s，没条数据都是200ms完成commit。（100ms的心跳间隔，那么就是两个心跳完成一个commit）

方案：写入数据后立即同步数据
commitindex更新之后也要发送AppendEntry
需要平衡消息条数和同步commit的耗时，写入数据后立即发送AppendEntry会导致RPC过多（TestCount3B用例失败）
```
### 测试结果
```
=== 第 25 次执行 make raft1 ===
go build -race -o main/raft1d main/raft1d.go
cd raft1 && go test -v -race  
=== RUN   TestInitialElection3A
Test (3A): initial election (reliable network)...
  ... Passed --  time  3.2s #peers 3 #RPCs    58 #Ops    0
--- PASS: TestInitialElection3A (3.57s)
=== RUN   TestReElection3A
Test (3A): election after network failure (reliable network)...
  ... Passed --  time  5.2s #peers 3 #RPCs   140 #Ops    0
--- PASS: TestReElection3A (5.64s)
=== RUN   TestManyElections3A
Test (3A): multiple elections (reliable network)...
  ... Passed --  time  5.9s #peers 7 #RPCs   546 #Ops    0
--- PASS: TestManyElections3A (6.89s)
=== RUN   TestBasicAgree3B
Test (3B): basic agreement (reliable network)...
  ... Passed --  time  1.1s #peers 3 #RPCs    18 #Ops    3
--- PASS: TestBasicAgree3B (1.56s)
=== RUN   TestRPCBytes3B
Test (3B): RPC byte count (reliable network)...
  ... Passed --  time  2.7s #peers 3 #RPCs    52 #Ops   11
--- PASS: TestRPCBytes3B (3.11s)
=== RUN   TestFollowerFailure3B
Test (3B): test progressive failure of followers (reliable network)...
  ... Passed --  time  4.9s #peers 3 #RPCs   117 #Ops    3
--- PASS: TestFollowerFailure3B (5.33s)
=== RUN   TestLeaderFailure3B
Test (3B): test failure of leaders (reliable network)...
  ... Passed --  time  5.5s #peers 3 #RPCs   191 #Ops    3
--- PASS: TestLeaderFailure3B (5.94s)
=== RUN   TestFailAgree3B
Test (3B): agreement after follower reconnects (reliable network)...
  ... Passed --  time  6.0s #peers 3 #RPCs   129 #Ops    7
--- PASS: TestFailAgree3B (6.39s)
=== RUN   TestFailNoAgree3B
Test (3B): no agreement if too many followers disconnect (reliable network)...
  ... Passed --  time  3.5s #peers 5 #RPCs   192 #Ops    2
--- PASS: TestFailNoAgree3B (4.22s)
=== RUN   TestConcurrentStarts3B
Test (3B): concurrent Start()s (reliable network)...
  ... Passed --  time  1.1s #peers 3 #RPCs    26 #Ops    0
--- PASS: TestConcurrentStarts3B (1.43s)
=== RUN   TestRejoin3B
Test (3B): rejoin of partitioned leader (reliable network)...
  ... Passed --  time  6.5s #peers 3 #RPCs   182 #Ops    4
--- PASS: TestRejoin3B (6.88s)
=== RUN   TestBackup3B
Test (3B): leader backs up quickly over incorrect follower logs (reliable network)...
  ... Passed --  time 28.5s #peers 5 #RPCs  3038 #Ops  102
--- PASS: TestBackup3B (29.22s)
=== RUN   TestCount3B
Test (3B): RPC counts aren't too high (reliable network)...
  ... Passed --  time  2.2s #peers 3 #RPCs    64 #Ops    0
--- PASS: TestCount3B (2.60s)
=== RUN   TestPersist13C
Test (3C): basic persistence (reliable network)...
  ... Passed --  time  5.2s #peers 3 #RPCs    88 #Ops    6
--- PASS: TestPersist13C (5.60s)
=== RUN   TestPersist23C
Test (3C): more persistence (reliable network)...
  ... Passed --  time 16.9s #peers 5 #RPCs   484 #Ops   16
--- PASS: TestPersist23C (17.59s)
=== RUN   TestPersist33C
Test (3C): partitioned leader and one follower crash, leader restarts (reliable network)...
  ... Passed --  time  2.0s #peers 3 #RPCs    36 #Ops    4
--- PASS: TestPersist33C (2.36s)
=== RUN   TestFigure83C
Test (3C): Figure 8 (reliable network)...
2026/08/16 00:51:33 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt oHcA4nw5azag31OXiDut ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
2026/08/16 00:51:34 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt cci70-zlV8-hkDLNCKs4 ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
2026/08/16 00:51:46 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt DA05-C1Bj8jtbv21YJjF ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
2026/08/16 00:51:58 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt MDiW_h4CVcNOhnzGdul2 ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
2026/08/16 00:51:59 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt xt2wdIwqQyRUGTk7jeGq ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
2026/08/16 00:52:00 B5oIVoEikYlMaZgZ3Kss: dmxsrv.reader: clnt KLoPJBlbaNRYBzsg3Tgf ReadCall err read unix /tmp/6.5840-B5oIVoEikYlMaZgZ3Kss->@: read: connection reset by peer
  ... Passed --  time 58.7s #peers 5 #RPCs  1614 #Ops    2
--- PASS: TestFigure83C (59.34s)
=== RUN   TestUnreliableAgree3C
Test (3C): unreliable agreement (unreliable network)...
  ... Passed --  time  5.9s #peers 5 #RPCs  1363 #Ops  246
--- PASS: TestUnreliableAgree3C (6.55s)
=== RUN   TestFigure8Unreliable3C
Test (3C): Figure 8 (unreliable) (unreliable network)...
  ... Passed --  time 72.4s #peers 5 #RPCs 14393 #Ops    2
--- PASS: TestFigure8Unreliable3C (73.14s)
=== RUN   TestReliableChurn3C
Test (3C): churn (reliable network)...
  ... Passed --  time 17.3s #peers 5 #RPCs  2375 #Ops    1
--- PASS: TestReliableChurn3C (18.01s)
=== RUN   TestUnreliableChurn3C
Test (3C): unreliable churn (unreliable network)...
  ... Passed --  time 17.2s #peers 5 #RPCs   763 #Ops    1
--- PASS: TestUnreliableChurn3C (17.94s)
=== RUN   TestSnapshotBasic3D
Test (3D): snapshots basic (reliable network)...
  ... Passed --  time  7.0s #peers 3 #RPCs   689 #Ops   31
--- PASS: TestSnapshotBasic3D (7.38s)
=== RUN   TestSnapshotInstall3D
Test (3D): install snapshots (disconnect) (reliable network)...
  ... Passed --  time 47.7s #peers 3 #RPCs  2049 #Ops   91
--- PASS: TestSnapshotInstall3D (48.04s)
=== RUN   TestSnapshotInstallUnreliable3D
Test (3D): install snapshots (disconnect) (unreliable network)...
  ... Passed --  time 64.8s #peers 3 #RPCs  2243 #Ops   91
--- PASS: TestSnapshotInstallUnreliable3D (65.27s)
=== RUN   TestSnapshotInstallCrash3D
Test (3D): install snapshots (crash) (reliable network)...
  ... Passed --  time 65.3s #peers 3 #RPCs  2178 #Ops   91
--- PASS: TestSnapshotInstallCrash3D (65.68s)
=== RUN   TestSnapshotInstallUnCrash3D
Test (3D): install snapshots (crash) (unreliable network)...
2026/08/16 00:58:18 BljNCsiqJPfX1c9yUWHe: dmxsrv.reader: clnt 58QSXvsGxYhBoHLFPCe4 ReadCall err read unix /tmp/6.5840-BljNCsiqJPfX1c9yUWHe->@: read: connection reset by peer
  ... Passed --  time 84.4s #peers 3 #RPCs  2466 #Ops   91
--- PASS: TestSnapshotInstallUnCrash3D (84.80s)
=== RUN   TestSnapshotAllCrash3D
Test (3D): crash and restart all servers (unreliable network)...
  ... Passed --  time 16.9s #peers 3 #RPCs   326 #Ops   55
--- PASS: TestSnapshotAllCrash3D (17.27s)
=== RUN   TestSnapshotInit3D
Test (3D): snapshot initialization after crash (unreliable network)...
  ... Passed --  time  5.5s #peers 3 #RPCs    98 #Ops   14
--- PASS: TestSnapshotInit3D (5.83s)
PASS
ok      6.5840/raft1    578.623s
```

## Lab4 kvraft1
### 遇到得问题
```
问题：TestSpeed4B 失败
kvraft_test.go:162: Operations completed too slowly 37.002908ms/op > 33.333333ms/op
定位以及结论：
1000条日志persist要30ms。耗时主要在encode上
encode cost:25.389993ms, size:95710
关掉 `RACE=-race`，耗时最大只有2ms。。。
后面得用例关掉race测试
```

```
问题：用例有卡住没有返回得场景
方案：需要注意applych通知过来得commitindex可能比当前正在等待的commitindex要大，这些等待的都是已经失效的Leader上的写入。需要返回ErrWrongLeader让客户端重试。
```

### 测试结果
```
=== 第 30 次执行 make kvraft1 ===
go build  -o main/kvraft1d main/kvraft1d.go
cd kvraft1 && go test -v   
=== RUN   TestBasic4B
Test: one client (4B basic) (reliable network)...
  ... Passed --  time  3.1s #peers 5 #RPCs  3419 #Ops  714
--- PASS: TestBasic4B (3.70s)
=== RUN   TestSpeed4B
Test: one client (4B speed) (reliable network)...
2026/08/19 20:15:30 dur 7.724263ms 33.333333ms
2026/08/19 20:15:30 0: 137
2026/08/19 20:15:30 1: 656
2026/08/19 20:15:30 2: 196
2026/08/19 20:15:30 3: 9
2026/08/19 20:15:30 4: 1
2026/08/19 20:15:30 6: 1
  ... Passed --  time  8.2s #peers 3 #RPCs  5598 #Ops 1002
--- PASS: TestSpeed4B (8.58s)
=== RUN   TestConcurrent4B
Test: many clients (4B many clients) (reliable network)...
  ... Passed --  time  3.5s #peers 5 #RPCs  5318 #Ops  974
--- PASS: TestConcurrent4B (4.10s)
=== RUN   TestUnreliable4B
Test: many clients (4B many clients) (unreliable network)...
  ... Passed --  time  4.6s #peers 5 #RPCs  2584 #Ops  430
--- PASS: TestUnreliable4B (5.41s)
=== RUN   TestOnePartition4B
Test: one client (4B progress in majority) (unreliable network)...
k:1-v:13 version:1 agreed
=========begin put, p1:[1 2 3], p2:[4 0]
  ... Passed --  time  1.4s #peers 5 #RPCs   148 #Ops    4
Test: no progress in minority (4B) (unreliable network)...
  ... Passed --  time  1.3s #peers 5 #RPCs   135 #Ops    7
Test: completion after heal (4B) (unreliable network)...
  ... Passed --  time  1.1s #peers 5 #RPCs    80 #Ops    4
2026/08/19 20:15:44 QqKdLbq4swx7mXSek1wQ: dmxsrv.reader: clnt 2ZRoIrs87FZCvBRptybp ReadCall err read unix /tmp/6.5840-QqKdLbq4swx7mXSek1wQ->@: read: connection reset by peer
--- PASS: TestOnePartition4B (4.51s)
=== RUN   TestManyPartitionsOneClient4B
Test: partitions, one client (4B partitions, one client) (reliable network)...
  ... Passed --  time 11.5s #peers 5 #RPCs  3834 #Ops  706
--- PASS: TestManyPartitionsOneClient4B (12.27s)
=== RUN   TestManyPartitionsManyClients4B
Test: partitions, many clients (4B partitions, many clients (4B)) (reliable network)...
  ... Passed --  time 10.3s #peers 5 #RPCs  4469 #Ops 1006
--- PASS: TestManyPartitionsManyClients4B (10.88s)
=== RUN   TestPersistOneClient4B
Test: restarts, one client (4B restarts, one client 4B ) (reliable network)...
  ... Passed --  time  8.4s #peers 5 #RPCs  3487 #Ops  518
--- PASS: TestPersistOneClient4B (9.10s)
=== RUN   TestPersistConcurrent4B
Test: restarts, many clients (4B restarts, many clients) (reliable network)...
2026/08/19 20:16:21 a6d45y1qEmYhQZt1TrXP: dmxsrv.reader: clnt Gfcve6P3lcD8EPWa8A_Q ReadCall err read unix /tmp/6.5840-a6d45y1qEmYhQZt1TrXP->@: read: connection reset by peer
  ... Passed --  time  8.6s #peers 5 #RPCs  3886 #Ops  830
--- PASS: TestPersistConcurrent4B (9.38s)
=== RUN   TestPersistConcurrentUnreliable4B
Test: restarts, many clients (4B restarts, many clients ) (unreliable network)...
  ... Passed --  time  9.2s #peers 5 #RPCs  2312 #Ops  382
--- PASS: TestPersistConcurrentUnreliable4B (9.91s)
=== RUN   TestPersistPartition4B
Test: restarts, partitions, many clients (4B restarts, partitions, many clients) (reliable network)...
  ... Passed --  time 14.8s #peers 5 #RPCs  4979 #Ops  730
--- PASS: TestPersistPartition4B (15.49s)
=== RUN   TestPersistPartitionUnreliable4B
Test: restarts, partitions, many clients (4B restarts, partitions, many clients) (unreliable network)...
  ... Passed --  time 15.6s #peers 5 #RPCs  2634 #Ops  406
--- PASS: TestPersistPartitionUnreliable4B (16.18s)
=== RUN   TestPersistPartitionUnreliableLinearizable4B
Test: restarts, partitions, random keys, many clients (4B restarts, partitions, random keys, many clients) (unreliable network)...
  ... Passed --  time 17.4s #peers 7 #RPCs  5320 #Ops  560
--- PASS: TestPersistPartitionUnreliableLinearizable4B (18.66s)
=== RUN   TestSnapshotRPC4C
Test: snapshots, one client (4C SnapshotsRPC) (reliable network)...
Test: InstallSnapshot RPC (4C) (reliable network)...
  ... Passed --  time  2.6s #peers 3 #RPCs   957 #Ops   71
--- PASS: TestSnapshotRPC4C (2.93s)
=== RUN   TestSnapshotSize4C
Test: snapshots, one client (4C snapshot size is reasonable) (reliable network)...
  ... Passed --  time  3.8s #peers 3 #RPCs  4266 #Ops 1200
--- PASS: TestSnapshotSize4C (4.36s)
=== RUN   TestSpeed4C
Test: snapshots, one client (4C speed) (reliable network)...
2026/08/19 20:17:39 dur 5.265612ms 33.333333ms
2026/08/19 20:17:39 0: 526
2026/08/19 20:17:39 1: 461
2026/08/19 20:17:39 2: 13
  ... Passed --  time  5.7s #peers 3 #RPCs  5589 #Ops 1002
--- PASS: TestSpeed4C (6.01s)
=== RUN   TestSnapshotRecover4C
Test: restarts, snapshots, one client (4C restarts, snapshots, one client) (reliable network)...
2026/08/19 20:17:41 rcgre67q5VO82SVlKeZt: dmxsrv.reader: clnt Bc3hG8NnNeStF8byZDv4 ReadCall err read unix /tmp/6.5840-rcgre67q5VO82SVlKeZt->@: read: connection reset by peer
  ... Passed --  time  7.9s #peers 5 #RPCs  3802 #Ops  666
--- PASS: TestSnapshotRecover4C (8.68s)
=== RUN   TestSnapshotRecoverManyClients4C
Test: restarts, snapshots, many clients (4C restarts, snapshots, many clients ) (reliable network)...
  ... Passed --  time 15.3s #peers 5 #RPCs 23528 #Ops 2854
--- PASS: TestSnapshotRecoverManyClients4C (15.97s)
=== RUN   TestSnapshotUnreliable4C
Test: snapshots, many clients (4C unreliable net, snapshots, many clients) (unreliable network)...
  ... Passed --  time  4.5s #peers 5 #RPCs  2984 #Ops  474
--- PASS: TestSnapshotUnreliable4C (5.20s)
=== RUN   TestSnapshotUnreliableRecover4C
Test: restarts, snapshots, many clients (4C unreliable net, restarts, snapshots, many clients) (unreliable network)...
2026/08/19 20:18:14 4Dq36LjqdNYPFl7Hikcl: dmxsrv.reader: clnt yllHNK5goj3lGs5JTByL ReadCall err read unix /tmp/6.5840-4Dq36LjqdNYPFl7Hikcl->@: read: connection reset by peer
  ... Passed --  time  9.1s #peers 5 #RPCs  2770 #Ops  406
--- PASS: TestSnapshotUnreliableRecover4C (9.82s)
=== RUN   TestSnapshotUnreliableRecoverConcurrentPartition4C
Test: restarts, partitions, snapshots, many clients (4C unreliable net, restarts, partitions, snapshots, many clients) (unreliable network)...
  ... Passed --  time 15.3s #peers 5 #RPCs  2897 #Ops  382
--- PASS: TestSnapshotUnreliableRecoverConcurrentPartition4C (16.15s)
=== RUN   TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C
Test: restarts, partitions, snapshots, random keys, many clients (4C unreliable net, restarts, partitions, snapshots, random keys, many clients) (unreliable network)...
  ... Passed --  time 15.4s #peers 7 #RPCs  6416 #Ops  620
--- PASS: TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable4C (16.53s)
PASS
ok      6.5840/kvraft1  213.838s
```