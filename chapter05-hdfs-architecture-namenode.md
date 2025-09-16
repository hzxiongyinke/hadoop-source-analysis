# 第5章：HDFS架构与NameNode源码分析

## 5.1 引言

Hadoop分布式文件系统（HDFS）是Hadoop生态系统的存储基石，为整个大数据平台提供了可靠、可扩展的分布式存储服务。HDFS的设计灵感来源于Google File System（GFS），但在开源实现的过程中，Hadoop社区结合实际应用需求，发展出了独特的技术特色和架构优势。

NameNode作为HDFS的"大脑"，承担着整个文件系统的元数据管理职责。它不仅要维护文件系统的命名空间，还要协调数据块的分布和复制，处理客户端的文件操作请求，确保系统的一致性和可靠性。NameNode的设计和实现直接决定了HDFS的性能、可扩展性和可靠性。

本章将深入分析HDFS的整体架构设计，重点剖析NameNode的核心实现机制，包括元数据管理、文件系统操作、持久化机制以及高可用实现。通过对源码的详细分析，帮助读者理解HDFS这一分布式存储系统的设计精髓。

## 5.2 HDFS整体架构设计

### 5.2.1 设计目标与原则

HDFS的设计基于以下核心目标和原则：

**硬件故障容错**：
HDFS假设硬件故障是常态而非异常，通过软件层面的数据复制和故障检测机制来保证数据的可靠性。每个数据块默认复制3份，分布在不同的节点上，即使部分节点失效也不会影响数据的可用性。

**流式数据访问**：
HDFS针对大文件的顺序读取进行了优化，而不是随机访问。这种设计使得HDFS特别适合批处理应用，如MapReduce作业，能够提供很高的数据吞吐量。

**大数据集支持**：
HDFS设计用于存储和处理TB到PB级别的数据集。通过水平扩展的方式，可以通过增加节点来线性提升存储容量和处理能力。

**简单一致性模型**：
HDFS采用"一次写入，多次读取"的模型，简化了数据一致性的处理。文件一旦创建、写入并关闭后，除了追加和截断操作外，不能修改文件内容。

**移动计算而非数据**：
HDFS提供了将计算任务调度到数据所在节点的能力，减少网络传输开销，提高整体系统性能。

### 5.2.2 Master-Slave架构

HDFS采用典型的Master-Slave架构：

**NameNode（Master）**：
- 管理文件系统的命名空间
- 维护文件到数据块的映射关系
- 管理数据块的位置信息
- 处理客户端的文件系统操作请求
- 协调DataNode的工作

**DataNode（Slave）**：
- 存储实际的数据块
- 定期向NameNode发送心跳和块报告
- 执行NameNode的指令（复制、删除、移动数据块）
- 直接为客户端提供数据读写服务

**Secondary NameNode**：
- 辅助NameNode进行检查点操作
- 定期合并EditLog和FSImage
- 不是NameNode的热备份

### 5.2.3 数据块管理机制

HDFS将大文件分割成固定大小的数据块（默认128MB），这种设计带来了以下优势：

**简化存储管理**：
文件可以大于集群中任何一个磁盘的容量，数据块可以分布在集群的任何节点上。

**简化元数据管理**：
NameNode只需要管理数据块的元数据，而不需要管理文件内部的详细信息。

**提高容错能力**：
数据块可以独立复制，即使某些数据块损坏，也不会影响整个文件的其他部分。

**支持并行处理**：
不同的数据块可以并行处理，提高了数据处理的效率。

## 5.3 NameNode核心架构

### 5.3.1 NameNode的职责划分

NameNode作为HDFS的核心组件，承担着多重职责：


```java
/**********************************************************
 * NameNode serves as both directory namespace manager and
 * "inode table" for the Hadoop DFS.  There is a single NameNode
 * running in any DFS deployment.  (Well, except when there
 * is a second backup/failover NameNode, or when using federated NameNodes.)
 *
 * The NameNode controls two critical tables:
 *   1)  filename{@literal ->}blocksequence (namespace)
 *   2)  block{@literal ->}machinelist ("inodes")
 *
 * The first table is stored on disk and is very precious.
 * The second table is rebuilt every time the NameNode comes up.
 *
 * 'NameNode' refers to both this class as well as the 'NameNode server'.
 * The 'FSNamesystem' class actually performs most of the filesystem
 * management.
 */
```


从这段注释可以看出，NameNode管理着两个关键的映射表：
1. **文件名到数据块序列的映射**（命名空间）
2. **数据块到机器列表的映射**（数据块位置信息）

### 5.3.2 FSNamesystem：文件系统的核心

FSNamesystem是NameNode中最重要的组件，它实现了文件系统的核心逻辑：


```java
/**
 * FSNamesystem is a container of both transient
 * and persisted name-space state, and does all the book-keeping
 * work on a NameNode.
 *
 * Its roles are briefly described below:
 *
 * 1) Is the container for BlockManager, DatanodeManager,
 *    DelegationTokens, LeaseManager, etc. services.
 * 2) RPC calls that modify or inspect the name-space
 *    should get delegated here.
 * 3) Anything that touches only blocks (eg. block reports),
 *    it delegates to BlockManager.
 * 4) Anything that touches only file information (eg. permissions, mkdirs),
 *    it delegates to FSDirectory.
 * 5) Anything that crosses two of the above components should be
 *    coordinated here.
 * 6) Logs mutations to FSEditLog.
 */
@InterfaceAudience.Private
@Metrics(context="dfs")
public class FSNamesystem implements Namesystem, FSNamesystemMBean,
    NameNodeMXBean, ReplicatedBlocksMBean, ECBlockGroupsMBean {
```


FSNamesystem的核心职责包括：

**服务容器**：包含BlockManager、DatanodeManager、LeaseManager等核心服务。

**RPC请求处理**：处理所有修改或查询命名空间的RPC调用。

**组件协调**：协调不同组件之间的交互，如文件操作和块管理的协调。

**事务日志**：将所有的变更操作记录到FSEditLog中。

### 5.3.3 读写锁机制

FSNamesystem使用读写锁来保证并发安全：


```java
/**
 * The given node has reported in.  This method should:
 * 1) Record the heartbeat, so the datanode isn't timed out
 * 2) Adjust usage stats for future block allocation
 *
 * If a substantial amount of time passed since the last datanode
 * heartbeat then request an immediate block report.
 *
 * @return an array of datanode commands
 * @throws IOException
 */
HeartbeatResponse handleHeartbeat(DatanodeRegistration nodeReg,
    StorageReport[] reports, long cacheCapacity, long cacheUsed,
    int xceiverCount, int xmitsInProgress, int failedVolumes,
    VolumeFailureSummary volumeFailureSummary,
    boolean requestFullBlockReportLease,
    @Nonnull SlowPeerReports slowPeers,
    @Nonnull SlowDiskReports slowDisks)
        throws IOException {
  readLock();
  try {
    //get datanode commands
    // ... 处理心跳逻辑
  } finally {
    readUnlock();
  }
}
```


**读锁**：用于不修改状态的操作，如文件读取、状态查询等，允许并发执行。

**写锁**：用于修改状态的操作，如文件创建、删除等，确保操作的原子性。

## 5.4 元数据管理：INode体系

### 5.4.1 INode抽象基类

HDFS使用INode来表示文件系统中的文件和目录：


```java
/**
 * We keep an in-memory representation of the file/block hierarchy.
 * This is a base INode class containing common fields for file and 
 * directory inodes.
 */
@InterfaceAudience.Private
public abstract class INode implements INodeAttributes, Diff.Element<byte[]> {
  public static final Logger LOG = LoggerFactory.getLogger(INode.class);

  /** parent is either an {@link INodeDirectory} or an {@link INodeReference}.*/
  private INode parent = null;

  INode(INode parent) {
    this.parent = parent;
  }

  /** Get inode id */
  public abstract long getId();

  /**
   * Check whether this is the root inode.
   */
  final boolean isRoot() {
    return getLocalNameBytes().length == 0;
  }
}
```


INode的设计特点：

**层次结构**：通过parent指针维护文件系统的层次结构。

**唯一标识**：每个INode都有唯一的ID。

**属性管理**：包含文件/目录的基本属性，如权限、所有者、时间戳等。

**快照支持**：支持文件系统快照功能。

### 5.4.2 权限和配额管理

INode实现了完整的权限和配额管理机制：


```java
/**
 * @param snapshotId
 *          if it is not {@link Snapshot#CURRENT_STATE_ID}, get the result
 *          from the given snapshot; otherwise, get the result from the
 *          current inode.
 * @return permission.
 */
abstract FsPermission getFsPermission(int snapshotId);

/** The same as getFsPermission(Snapshot.CURRENT_STATE_ID). */
@Override
public final FsPermission getFsPermission() {
  return getFsPermission(Snapshot.CURRENT_STATE_ID);
}

/** Set the {@link FsPermission} of this {@link INode} */
abstract void setPermission(FsPermission permission);

/** Set the {@link FsPermission} of this {@link INode} */
INode setPermission(FsPermission permission, int latestSnapshotId) {
  recordModification(latestSnapshotId);
  setPermission(permission);
  return this;
}
```


**权限管理**：
- 支持POSIX风格的文件权限
- 支持ACL（访问控制列表）
- 支持扩展属性（XAttr）

**配额管理**：
- 命名空间配额：限制目录下的文件和目录数量
- 存储空间配额：限制目录下的存储空间使用
- 存储类型配额：限制不同存储类型的使用


```java
/**
 * Check and add namespace/storagespace/storagetype consumed to itself and the ancestors.
 */
public void addSpaceConsumed(QuotaCounts counts) {
  if (parent != null) {
    parent.addSpaceConsumed(counts);
  }
}

/**
 * Get the quota set for this inode
 * @return the quota counts.  The count is -1 if it is not set.
 */
public QuotaCounts getQuotaCounts() {
  return new QuotaCounts.Builder().
      nameSpace(HdfsConstants.QUOTA_RESET).
      storageSpace(HdfsConstants.QUOTA_RESET).
      typeSpaces(HdfsConstants.QUOTA_RESET).
      build();
}
```


## 5.5 持久化机制：EditLog与FSImage

### 5.5.1 EditLog：事务日志系统

EditLog记录了所有对文件系统的修改操作，确保数据的持久性和一致性：


```java
/**
 * Write an operation to the edit log.
 * <p/>
 * Additionally, this will sync the edit log if required by the underlying
 * edit stream's automatic sync policy (e.g. when the buffer is full, or
 * if a time interval has elapsed).
 */
void logEdit(final FSEditLogOp op) {
  boolean needsSync = false;
  synchronized (this) {
    assert isOpenForWrite() :
      "bad state: " + state;
    
    // wait if an automatic sync is scheduled
    waitIfAutoSyncScheduled();

    beginTransaction(op);
    // check if it is time to schedule an automatic sync
    needsSync = doEditTransaction(op);
    if (needsSync) {
      isAutoSyncScheduled = true;
    }
  }

  // Sync the log if an automatic sync is required.
  if (needsSync) {
    logSync();
  }
}
```


EditLog的关键特性：

**事务性**：每个操作都是一个事务，要么完全成功，要么完全失败。

**持久性**：操作被写入磁盘后才返回成功，确保数据不会丢失。

**顺序性**：操作按照时间顺序记录，保证重放时的一致性。

**自动同步**：根据配置的策略自动同步到磁盘。

### 5.5.2 具体操作的日志记录

EditLog为不同类型的操作提供了专门的记录方法：


```java
public void logAddBlock(String path, INodeFile file) {
  Preconditions.checkArgument(file.isUnderConstruction());
  BlockInfo[] blocks = file.getBlocks();
  Preconditions.checkState(blocks != null && blocks.length > 0);
  BlockInfo pBlock = blocks.length > 1 ? blocks[blocks.length - 2] : null;
  BlockInfo lastBlock = blocks[blocks.length - 1];
  AddBlockOp op = AddBlockOp.getInstance(cache.get()).setPath(path)
      .setPenultimateBlock(pBlock).setLastBlock(lastBlock);
  logEdit(op);
}

public void logUpdateBlocks(String path, INodeFile file, boolean toLogRpcIds) {
  Preconditions.checkArgument(file.isUnderConstruction());
  UpdateBlocksOp op = UpdateBlocksOp.getInstance(cache.get())
    .setPath(path)
    .setBlocks(file.getBlocks());
  logRpcIds(op, toLogRpcIds);
  logEdit(op);
}
```


### 5.5.3 FSImage：检查点机制

FSImage是文件系统元数据的完整快照：


```java
/**
 * Save the contents of the FS image to a new image file in each of the
 * current storage directories.
 */
public synchronized void saveNamespace(FSNamesystem source, NameNodeFile nnf,
    Canceler canceler) throws IOException {
  assert editLog != null : "editLog must be initialized";
  LOG.info("Save namespace ...");
  storage.attemptRestoreRemovedStorage();

  boolean editLogWasOpen = editLog.isSegmentOpen();
  
  if (editLogWasOpen) {
    editLog.endCurrentLogSegment(true);
  }
  long imageTxId = getCorrectLastAppliedOrWrittenTxId();
  if (!addToCheckpointing(imageTxId)) {
    throw new IOException(
        "FS image is being downloaded from another NN at txid " + imageTxId);
  }
  // ... 保存逻辑
}
```


**检查点操作**：
- 定期将内存中的文件系统状态保存到磁盘
- 合并EditLog和FSImage，生成新的FSImage
- 清理旧的EditLog文件，减少启动时间

**启动恢复**：
- 加载最新的FSImage
- 重放EditLog中的操作
- 重建文件系统的内存状态

## 5.6 NameNode高可用机制

### 5.6.1 Active/Standby架构

Hadoop 2.x引入了NameNode高可用（HA）机制，解决了单点故障问题：

**Active NameNode**：
- 提供正常的文件系统服务
- 处理客户端请求
- 写入EditLog到共享存储

**Standby NameNode**：
- 实时同步Active NameNode的状态
- 从共享存储读取EditLog
- 准备随时接管服务

### 5.6.2 EditLogTailer：日志同步机制

Standby NameNode通过EditLogTailer实时同步EditLog：


```java
public T call() throws IOException {
  // reset the loop count on success
  nnLoopCount = 0;
  while ((cachedActiveProxy = getActiveNodeProxy()) != null) {
    try {
      T ret = doWork();
      return ret;
    } catch (IOException e) {
      LOG.warn("Exception from remote name node " + currentNN
          + ", try next.", e);

      // Try next name node if exception happens.
      cachedActiveProxy = null;
      nnLoopCount++;
    }
  }
  throw new IOException("Cannot find any valid remote NN to service request!");
}
```


### 5.6.3 状态管理

NameNode的状态通过HAState进行管理：


```java
@Override
public void enterState(HAContext context) throws ServiceFailedException {
  try {
    context.startStandbyServices();
  } catch (IOException e) {
    throw new ServiceFailedException("Failed to start standby services", e);
  }
}

@Override
public void prepareToExitState(HAContext context) throws ServiceFailedException {
  context.prepareToStopStandbyServices();
}
```


**状态转换**：
- STANDBY → ACTIVE：故障转移或手动切换
- ACTIVE → STANDBY：维护或故障恢复
- 状态转换过程中的服务协调

## 5.7 本章小结

本章深入分析了HDFS的架构设计和NameNode的核心实现。通过对源码的详细分析，我们可以看到HDFS设计的几个重要特点：

**清晰的职责分离**：NameNode专注于元数据管理，DataNode专注于数据存储，职责分离使得系统更容易理解和维护。

**完善的元数据管理**：通过INode体系、EditLog和FSImage机制，实现了高效、可靠的元数据管理。

**强大的一致性保证**：通过读写锁、事务日志等机制，确保了文件系统的一致性。

**高可用架构**：通过Active/Standby模式和共享存储，解决了单点故障问题。

**可扩展的设计**：模块化的架构设计为系统的扩展和优化提供了良好的基础。

在下一章中，我们将继续分析HDFS的另一个核心组件——DataNode，了解数据存储和管理的具体实现机制。

---

**本章要点回顾**：
- HDFS采用Master-Slave架构，NameNode管理元数据，DataNode存储数据
- FSNamesystem是NameNode的核心，负责文件系统的所有管理工作
- INode体系实现了完整的文件系统元数据表示和管理
- EditLog和FSImage机制保证了元数据的持久性和一致性
- NameNode HA通过Active/Standby模式解决了单点故障问题
