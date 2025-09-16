# 第6章：HDFS DataNode与数据存储机制

## 6.1 引言

如果说NameNode是HDFS的"大脑"，那么DataNode就是HDFS的"肌肉"——它们承担着实际的数据存储和传输工作。DataNode是HDFS集群中的工作节点，负责存储数据块、响应客户端的读写请求、执行NameNode的管理指令，以及维护数据的完整性和可靠性。

DataNode的设计体现了分布式存储系统的核心理念：将复杂的元数据管理与简单的数据存储分离，通过大量的廉价节点提供海量的存储容量和并行的数据处理能力。每个DataNode都是一个相对独立的存储单元，它们通过标准化的接口与NameNode协作，形成了一个高度可扩展的分布式存储系统。

本章将深入分析DataNode的架构设计和核心实现，包括数据块的存储管理、心跳和块汇报机制、数据完整性保证、以及数据复制和恢复等关键功能。通过对源码的详细分析，帮助读者理解HDFS数据存储层的设计精髓和实现细节。

## 6.2 DataNode架构与职责

### 6.2.1 DataNode的核心职责

DataNode在HDFS架构中承担着多重职责：


```java
/**********************************************************
 * DataNode is a class (and program) that stores a set of
 * blocks for a DFS deployment.  A single deployment can
 * have one or many DataNodes.  Each DataNode communicates
 * regularly with a single NameNode.  It also communicates
 * with client code and other DataNodes from time to time.
 *
 * DataNodes store a series of named blocks.  The DataNode
 * allows client code to read these blocks, or to write new
 * block data.  The DataNode may also, in response to instructions
 * from its NameNode, delete blocks or copy blocks to/from other
 * DataNodes.
 *
 * The DataNode maintains just one critical table:
 *   block{@literal ->} stream of bytes (of BLOCK_SIZE or less)
 */
```


从这段注释可以看出，DataNode的核心职责包括：

**数据块存储**：存储一系列命名的数据块，每个数据块对应一个字节流。

**客户端服务**：允许客户端代码读取这些数据块，或写入新的数据块。

**NameNode协作**：定期与NameNode通信，接收并执行NameNode的指令。

**节点间协作**：根据需要与其他DataNode通信，进行数据块的复制和传输。

**数据管理**：响应NameNode的指令，删除或复制数据块到其他DataNode。

### 6.2.2 DataNode的核心组件

DataNode包含多个重要的组件：


```java
volatile boolean shouldRun = true;
volatile boolean shutdownForUpgrade = false;
private boolean shutdownInProgress = false;
private BlockPoolManager blockPoolManager;
volatile FsDatasetSpi<? extends FsVolumeSpi> data = null;
private String clusterId = null;

final AtomicInteger xmitsInProgress = new AtomicInteger();
Daemon dataXceiverServer = null;
DataXceiverServer xserver = null;
Daemon localDataXceiverServer = null;
ShortCircuitRegistry shortCircuitRegistry = null;
ThreadGroup threadGroup = null;
private DNConf dnConf;
private volatile boolean heartbeatsDisabledForTests = false;
private volatile boolean ibrDisabledForTests = false;
private volatile boolean cacheReportsDisabledForTests = false;
private DataStorage storage = null;
```


**BlockPoolManager**：管理多个块池（Block Pool），支持HDFS联邦。

**FsDatasetSpi**：数据存储接口，抽象了底层的存储实现。

**DataXceiverServer**：处理客户端和其他DataNode的数据传输请求。

**ShortCircuitRegistry**：支持短路读取，提高本地读取性能。

**DataStorage**：管理DataNode的存储目录和元数据。

### 6.2.3 生命周期管理

DataNode的生命周期包括启动、运行和关闭三个阶段：

**启动阶段**：
- 加载配置和存储目录
- 初始化数据存储接口
- 启动各种服务线程
- 向NameNode注册

**运行阶段**：
- 定期发送心跳给NameNode
- 处理客户端的读写请求
- 执行NameNode的管理指令
- 进行数据完整性检查

**关闭阶段**：
- 停止接收新的请求
- 完成正在进行的操作
- 清理资源和临时文件
- 向NameNode报告关闭状态

## 6.3 数据块存储与管理

### 6.3.1 FsDatasetImpl：存储实现

FsDatasetImpl是DataNode数据存储的核心实现：


```java
@Override
public void addVolume(final StorageLocation location,
    final List<NamespaceInfo> nsInfos)
    throws IOException {
  // Prepare volume in DataStorage
  final DataStorage.VolumeBuilder builder;
  try {
    builder = dataStorage.prepareVolume(datanode, location, nsInfos);
  } catch (IOException e) {
    volumes.addVolumeFailureInfo(new VolumeFailureInfo(location, Time.now()));
    throw e;
  }

  final Storage.StorageDirectory sd = builder.getStorageDirectory();

  StorageType storageType = location.getStorageType();
  final FsVolumeImpl fsVolume =
      createFsVolume(sd.getStorageUuid(), sd, location);
  final ReplicaMap tempVolumeMap =
      new ReplicaMap(new ReentrantReadWriteLock());
  ArrayList<IOException> exceptions = Lists.newArrayList();
  // ... 添加卷的逻辑
}
```


**存储卷管理**：
- 支持动态添加和移除存储卷
- 支持多种存储类型（DISK、SSD、RAM_DISK等）
- 自动处理存储卷故障

**数据块分配**：
- 根据存储策略选择合适的存储卷
- 支持分层存储和缓存策略
- 实现负载均衡和容量管理

### 6.3.2 数据块创建流程

当客户端写入数据时，DataNode需要创建新的数据块：


```java
// Use ramdisk only if block size is a multiple of OS page size.
// This simplifies reservation for partially used replicas
// significantly.
if (allowLazyPersist &&
    lazyWriter != null &&
    b.getNumBytes() % cacheManager.getOsPageSize() == 0 &&
    reserveLockedMemory(b.getNumBytes())) {
  try {
    // First try to place the block on a transient volume.
    ref = volumes.getNextTransientVolume(b.getNumBytes());
    datanode.getMetrics().incrRamDiskBlocksWrite();
  } catch (DiskOutOfSpaceException de) {
    // Ignore the exception since we just fall back to persistent storage.
    LOG.warn("Insufficient space for placing the block on a transient "
        + "volume, fall back to persistent storage: "
        + de.getMessage());
  } finally {
    if (ref == null) {
      cacheManager.release(b.getNumBytes());
    }
  }
}

if (ref == null) {
  ref = volumes.getNextVolume(storageType, storageId, b.getNumBytes());
}

FsVolumeImpl v = (FsVolumeImpl) ref.getVolume();
// create an rbw file to hold block in the designated volume

if (allowLazyPersist && !v.isTransientStorage()) {
  datanode.getMetrics().incrRamDiskBlocksWriteFallback();
}

ReplicaInPipeline newReplicaInfo;
try {
  newReplicaInfo = v.createRbw(b);
```


**存储策略优化**：
- 优先使用RAM_DISK进行临时存储
- 根据块大小和页面大小进行优化
- 自动降级到持久化存储

**副本状态管理**：
- RBW（Replica Being Written）：正在写入的副本
- RWR（Replica Waiting to be Recovered）：等待恢复的副本
- RUR（Replica Under Recovery）：正在恢复的副本
- FINALIZED：已完成的副本

### 6.3.3 数据块完成处理

当数据块写入完成时，DataNode需要进行相应的处理：


```java
/**
 * After a block becomes finalized, a datanode increases metric counter,
 * notifies namenode, and adds it to the block scanner
 * @param block block to close
 * @param delHint hint on which excess block to delete
 * @param storageUuid UUID of the storage where block is stored
 */
void closeBlock(ExtendedBlock block, String delHint, String storageUuid,
    boolean isTransientStorage) {
  metrics.incrBlocksWritten();
  notifyNamenodeReceivedBlock(block, delHint, storageUuid,
      isTransientStorage);
}
```


**完成流程**：
1. 更新性能指标
2. 通知NameNode数据块已接收
3. 将数据块添加到扫描器进行完整性检查
4. 更新本地的副本映射

## 6.4 心跳机制与块汇报

### 6.4.1 心跳调度机制

DataNode通过BPServiceActor与NameNode保持心跳：


```java
//
// Every so often, send heartbeat or block-report
//
final boolean sendHeartbeat = scheduler.isHeartbeatDue(startTime);
LOG.debug("BP offer service run start time: {}, sendHeartbeat: {}", startTime,
    sendHeartbeat);
HeartbeatResponse resp = null;
if (sendHeartbeat) {
  //
  // All heartbeat messages include following info:
  // -- Datanode name
  // -- data transfer port
  // -- Total capacity
  // -- Bytes remaining
  //
  boolean requestBlockReportLease = (fullBlockReportLeaseId == 0) &&
          scheduler.isBlockReportDue(startTime);
  if (!dn.areHeartbeatsDisabledForTests()) {
    LOG.debug("Before sending heartbeat to namenode {}, the state of the namenode known"
        + " to datanode so far is {}", this.getNameNodeAddress(), state);
    resp = sendHeartBeat(requestBlockReportLease);
  }
}
```


**心跳内容**：
- DataNode名称和数据传输端口
- 总容量和剩余容量
- 存储报告和缓存使用情况
- 传输中的连接数和失败卷数

**心跳调度**：
- 默认每3秒发送一次心跳
- 根据NameNode状态调整心跳频率
- 支持生命线心跳机制

### 6.4.2 心跳响应处理

DataNode需要处理NameNode的心跳响应：


```java
// If the state of this NN has changed (eg STANDBY->ACTIVE)
// then let the BPOfferService update itself.
//
// Important that this happens before processCommand below,
// since the first heartbeat to a new active might have commands
// that we should actually process.
bpos.updateActorStatesFromHeartbeat(
    this, resp.getNameNodeHaState());
HAServiceState stateFromResp = resp.getNameNodeHaState().getState();
if (state != stateFromResp) {
  LOG.info("After receiving heartbeat response, updating state of namenode {} to {}",
      this.getNameNodeAddress(), stateFromResp);
}
state = stateFromResp;

if (state == HAServiceState.ACTIVE) {
  handleRollingUpgradeStatus(resp);
}
commandProcessingThread.enqueue(resp.getCommands());
```


**状态同步**：
- 跟踪NameNode的HA状态变化
- 处理STANDBY到ACTIVE的切换
- 更新本地的NameNode状态

**命令处理**：
- 将NameNode的命令加入处理队列
- 支持异步命令处理
- 确保命令的顺序执行

### 6.4.3 块汇报机制

DataNode定期向NameNode发送完整的块报告：


```java
/**
 * Report the list blocks to the Namenode
 * @return DatanodeCommands returned by the NN. May be null.
 * @throws IOException
 */
List<DatanodeCommand> blockReport(long fullBrLeaseId) throws IOException {
  final ArrayList<DatanodeCommand> cmds = new ArrayList<DatanodeCommand>();

  // Flush any block information that precedes the block report. Otherwise
  // we have a chance that we will miss the delHint information
  // or we will report an RBW replica after the BlockReport already reports
  // a FINALIZED one.
  synchronized (sendIBRLock) {
    ibrManager.sendIBRs(bpNamenode, bpRegistration,
        bpos.getBlockPoolId(), getRpcMetricSuffix());
  }
  // ... 块报告逻辑
}
```


**报告策略**：
- 默认每6小时发送一次完整块报告
- 支持增量块报告（IBR）
- 根据块数量分割大型报告

**报告内容**：
- 所有存储的数据块信息
- 块的状态和副本信息
- 存储位置和完整性信息

### 6.4.4 心跳调度器

BPServiceActor使用专门的调度器管理各种定时任务：


```java
long scheduleNextHeartbeat() {
  // Numerical overflow is possible here and is okay.
  nextHeartbeatTime = monotonicNow() + heartbeatIntervalMs;
  scheduleNextLifeline(nextHeartbeatTime);
  return nextHeartbeatTime;
}

void updateLastHeartbeatTime(long heartbeatTime) {
  lastHeartbeatTime = heartbeatTime;
}

void updateLastHeartbeatResponseTime(long heartbeatTime) {
  this.lastHeartbeatResponseTime = heartbeatTime;
}

void updateLastBlockReportTime(long blockReportTime) {
  lastBlockReportTime = blockReportTime;
}

void scheduleNextOutlierReport() {
  nextOutliersReportTime = monotonicNow() + outliersReportIntervalMs;
}
```


**调度功能**：
- 心跳调度：管理常规心跳的发送时间
- 生命线调度：管理生命线心跳的发送
- 块报告调度：管理块报告的发送时间
- 异常报告调度：管理慢节点和慢磁盘报告

## 6.5 数据完整性检查

### 6.5.1 校验和机制

HDFS使用校验和来保证数据完整性：

**CRC32校验**：
- 默认使用CRC32C算法
- 每512字节计算一个校验和
- 校验和与数据分别存储

**校验和文件**：
- 每个数据块对应一个.meta文件
- 存储块的校验和信息
- 包含校验和类型和块大小信息

**读取时验证**：
- 客户端读取时自动验证校验和
- 发现错误时自动切换到其他副本
- 向NameNode报告损坏的数据块

### 6.5.2 块扫描器

DataNode运行块扫描器定期检查数据完整性：

**扫描策略**：
- 周期性扫描所有数据块
- 优先扫描最近未扫描的块
- 根据磁盘负载调整扫描速度

**错误处理**：
- 发现损坏块时立即报告NameNode
- 尝试从其他副本恢复数据
- 记录错误统计信息

### 6.5.3 数据恢复机制

当发现数据损坏时，DataNode会启动恢复流程：


```java
/**
 * Update replica with the new generation stamp and length.  
 */
@Override // InterDatanodeProtocol
public String updateReplicaUnderRecovery(final ExtendedBlock oldBlock,
    final long recoveryId, final long newBlockId, final long newLength)
    throws IOException {
  Preconditions.checkNotNull(data, "Storage not yet initialized");
  final Replica r = data.updateReplicaUnderRecovery(oldBlock,
      recoveryId, newBlockId, newLength);
  // Notify the namenode of the updated block info. This is important
  // for HA, since otherwise the standby node may lose track of the
  // block locations until the next block report.
  ExtendedBlock newBlock = new ExtendedBlock(oldBlock);
  newBlock.setGenerationStamp(recoveryId);
  newBlock.setBlockId(newBlockId);
  newBlock.setNumBytes(newLength);
  final String storageID = r.getStorageUuid();
  notifyNamenodeReceivedBlock(newBlock, null, storageID,
      r.isOnTransientStorage());
  return storageID;
}
```


## 6.6 数据复制与恢复

### 6.6.1 块恢复工作器

BlockRecoveryWorker负责处理数据块的恢复任务：


```java
public Daemon recoverBlocks(final String who,
    final Collection<RecoveringBlock> blocks) {
  Daemon d = new Daemon(datanode.threadGroup, new Runnable() {
    @Override
    public void run() {
      datanode.metrics.incrDataNodeBlockRecoveryWorkerCount();
      try {
        for (RecoveringBlock b : blocks) {
          try {
            logRecoverBlock(who, b);
            if (b.isStriped()) {
              new RecoveryTaskStriped((RecoveringStripedBlock) b).recover();
            } else {
              new RecoveryTaskContiguous(b).recover();
            }
          } catch (IOException e) {
            LOG.warn("recover Block: {} FAILED: {}", b, e);
          }
        }
      } finally {
        datanode.metrics.decrDataNodeBlockRecoveryWorkerCount();
      }
    }
  });
  d.start();
  return d;
}
```


**恢复类型**：
- 连续块恢复：传统的副本恢复
- 条带块恢复：纠删码块的恢复
- 支持并行恢复多个块

**恢复流程**：
1. 协调所有相关的DataNode
2. 确定最新的数据版本
3. 从健康副本复制数据
4. 更新块的元数据信息
5. 通知NameNode恢复完成

### 6.6.2 数据传输管道

HDFS使用传输管道进行数据复制：

**管道建立**：
- 客户端选择第一个DataNode
- 第一个DataNode连接第二个DataNode
- 形成数据传输管道

**数据流控制**：
- 支持流水线传输
- 实现背压控制
- 处理节点故障和恢复

**确认机制**：
- 每个节点确认数据接收
- 支持部分确认和重传
- 保证数据的可靠传输

## 6.7 DataNode管理与监控

### 6.7.1 DatanodeManager

NameNode通过DatanodeManager管理所有的DataNode：


```java
/** Handle heartbeat from datanodes. */
public DatanodeCommand[] handleHeartbeat(DatanodeRegistration nodeReg,
    StorageReport[] reports, final String blockPoolId,
    long cacheCapacity, long cacheUsed, int xceiverCount, 
    int maxTransfers, int failedVolumes,
    VolumeFailureSummary volumeFailureSummary,
    @Nonnull SlowPeerReports slowPeers,
    @Nonnull SlowDiskReports slowDisks) throws IOException {
  final DatanodeDescriptor nodeinfo;
  try {
    nodeinfo = getDatanode(nodeReg);
  } catch (UnregisteredNodeException e) {
    return new DatanodeCommand[]{RegisterCommand.REGISTER};
  }

  // Check if this datanode should actually be shutdown instead.
  if (nodeinfo != null && nodeinfo.isDisallowed()) {
    setDatanodeDead(nodeinfo);
    throw new DisallowedDatanodeException(nodeinfo);
  }
  // ... 心跳处理逻辑
}
```


**节点管理**：
- 维护所有DataNode的状态信息
- 处理节点的注册和注销
- 监控节点的健康状况

**负载均衡**：
- 跟踪每个节点的存储使用情况
- 选择合适的节点进行数据放置
- 触发数据重新平衡

### 6.7.2 性能监控

DataNode提供了丰富的性能监控指标：

**存储指标**：
- 总容量和已使用容量
- 读写吞吐量和IOPS
- 存储卷的健康状况

**网络指标**：
- 数据传输速率
- 连接数和错误率
- 网络延迟统计

**系统指标**：
- CPU和内存使用率
- 磁盘I/O统计
- JVM性能指标

## 6.8 本章小结

本章深入分析了HDFS DataNode的架构设计和核心实现。通过对源码的详细分析，我们可以看到DataNode设计的几个重要特点：

**模块化架构**：DataNode采用了清晰的模块化设计，各个组件职责明确，便于维护和扩展。

**可靠性保证**：通过校验和、块扫描、数据恢复等机制，确保了数据的完整性和可靠性。

**高效通信**：通过心跳机制和块汇报，实现了与NameNode的高效协作。

**存储优化**：支持多种存储类型和分层存储，提供了灵活的存储策略。

**故障处理**：具备完善的故障检测和恢复机制，保证了系统的高可用性。

在下一章中，我们将分析HDFS的客户端实现，了解客户端如何与NameNode和DataNode交互，实现文件的读写操作。

---

**本章要点回顾**：
- DataNode负责实际的数据存储和传输，是HDFS的工作节点
- FsDatasetImpl实现了灵活的存储管理，支持多种存储类型和策略
- 心跳机制和块汇报保证了DataNode与NameNode的协作
- 校验和和块扫描机制保证了数据的完整性
- 块恢复机制保证了数据的可靠性和可用性
