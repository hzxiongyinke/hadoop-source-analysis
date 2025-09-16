# 第16章：Hadoop高可用与容灾机制

## 16.1 引言

高可用性（High Availability，HA）是企业级Hadoop集群的核心要求之一。在大规模数据处理环境中，任何单点故障都可能导致严重的业务中断和数据丢失。传统的Hadoop架构中，NameNode和ResourceManager作为单点存在，一旦发生故障，整个集群将无法正常工作。为了解决这个问题，Hadoop引入了完善的高可用机制，通过主备切换、共享存储、故障检测等技术手段，确保关键组件的持续可用性。

Hadoop高可用架构的设计遵循了分布式系统的经典原则：消除单点故障、实现故障快速检测和自动恢复、保证数据一致性和完整性。NameNode HA通过Active/Standby模式实现主备切换，使用共享存储或QJM（Quorum Journal Manager）来同步元数据；ResourceManager HA采用类似的架构，确保YARN调度服务的连续性。这些高可用机制不仅提高了系统的可靠性，还大大减少了运维成本和故障恢复时间。

容灾机制是高可用性的重要补充，它关注的是在更大范围的灾难（如数据中心故障、自然灾害等）发生时，如何快速恢复业务运行。Hadoop的容灾策略包括跨数据中心的数据复制、备份恢复机制、灾难恢复预案等。通过合理的容灾设计，可以将灾难对业务的影响降到最低，确保关键数据和服务的快速恢复。

本章将深入分析Hadoop高可用与容灾机制的设计原理、技术实现和最佳实践，帮助读者全面理解如何构建高可靠的Hadoop集群，并在生产环境中有效应对各种故障场景。

## 16.2 NameNode高可用架构

### 16.2.1 NameNode HA架构设计

NameNode高可用是HDFS可靠性的基础：

```java
/**
 * NameNode高可用管理器
 */
public class NameNodeHAManager {
  
  private final Configuration conf;
  private final HAServiceState currentState;
  private final FailoverController failoverController;
  private final SharedEditsManager sharedEditsManager;
  private final ZKFailoverController zkfc;
  
  public NameNodeHAManager(Configuration conf) {
    this.conf = conf;
    this.currentState = HAServiceState.STANDBY;
    this.failoverController = new FailoverController(conf);
    this.sharedEditsManager = new SharedEditsManager(conf);
    this.zkfc = new ZKFailoverController(conf);
  }
  
  /**
   * 初始化HA服务
   */
  public void initializeHA() throws IOException {
    // 初始化共享存储
    sharedEditsManager.initialize();
    
    // 启动故障转移控制器
    failoverController.start();
    
    // 启动ZooKeeper故障转移控制器
    zkfc.start();
    
    LOG.info("NameNode HA initialized successfully");
  }
  
  /**
   * 转换为Active状态
   */
  public synchronized void transitionToActive() throws IOException {
    if (currentState == HAServiceState.ACTIVE) {
      LOG.warn("Already in active state");
      return;
    }
    
    try {
      // 1. 获取共享锁
      if (!sharedEditsManager.acquireSharedLock()) {
        throw new IOException("Failed to acquire shared lock");
      }
      
      // 2. 读取共享编辑日志
      long lastTxId = sharedEditsManager.getLastTransactionId();
      FSEditLog editLog = getFSNamesystem().getEditLog();
      editLog.recoverUnclosedStreams();
      
      // 3. 应用未处理的编辑日志
      if (lastTxId > editLog.getLastWrittenTxId()) {
        sharedEditsManager.tailSharedEdits(editLog.getLastWrittenTxId() + 1);
      }
      
      // 4. 启动Active服务
      startActiveServices();
      
      // 5. 更新状态
      currentState = HAServiceState.ACTIVE;
      
      LOG.info("Successfully transitioned to Active state");
      
    } catch (Exception e) {
      LOG.error("Failed to transition to Active state", e);
      throw new IOException("Transition to Active failed", e);
    }
  }
  
  /**
   * 转换为Standby状态
   */
  public synchronized void transitionToStandby() throws IOException {
    if (currentState == HAServiceState.STANDBY) {
      LOG.warn("Already in standby state");
      return;
    }
    
    try {
      // 1. 停止Active服务
      stopActiveServices();
      
      // 2. 释放共享锁
      sharedEditsManager.releaseSharedLock();
      
      // 3. 启动Standby服务
      startStandbyServices();
      
      // 4. 更新状态
      currentState = HAServiceState.STANDBY;
      
      LOG.info("Successfully transitioned to Standby state");
      
    } catch (Exception e) {
      LOG.error("Failed to transition to Standby state", e);
      throw new IOException("Transition to Standby failed", e);
    }
  }
  
  private void startActiveServices() throws IOException {
    // 启动RPC服务器
    NameNode.getNameNodeMetrics().incrNumActiveServices();
    
    // 启动HTTP服务器
    startHttpServer();
    
    // 启动块管理服务
    getFSNamesystem().getBlockManager().activate();
    
    // 启动心跳处理
    getFSNamesystem().startActiveServices();
    
    // 启动编辑日志写入
    getFSNamesystem().getEditLog().initJournalsForWrite();
  }
  
  private void stopActiveServices() throws IOException {
    // 停止编辑日志写入
    getFSNamesystem().getEditLog().close();
    
    // 停止心跳处理
    getFSNamesystem().stopActiveServices();
    
    // 停止块管理服务
    getFSNamesystem().getBlockManager().deactivate();
    
    // 停止HTTP服务器
    stopHttpServer();
    
    NameNode.getNameNodeMetrics().decrNumActiveServices();
  }
  
  private void startStandbyServices() throws IOException {
    // 启动编辑日志跟踪服务
    EditLogTailer tailer = new EditLogTailer(getFSNamesystem(), conf);
    tailer.start();
    
    // 启动Standby检查点服务
    StandbyCheckpointer checkpointer = new StandbyCheckpointer(conf, getFSNamesystem());
    checkpointer.start();
    
    LOG.info("Standby services started");
  }
  
  /**
   * 检查服务健康状态
   */
  public HAServiceStatus getServiceStatus() {
    HAServiceStatus status = new HAServiceStatus(currentState);
    
    // 检查各个组件的健康状态
    status.setReadyToBecomeActive(isReadyToBecomeActive());
    status.setLiveNodes(getFSNamesystem().getNumLiveDataNodes());
    status.setDeadNodes(getFSNamesystem().getNumDeadDataNodes());
    
    return status;
  }
  
  private boolean isReadyToBecomeActive() {
    // 检查是否准备好成为Active
    FSNamesystem fsn = getFSNamesystem();
    
    // 检查安全模式
    if (fsn.isInSafeMode()) {
      return false;
    }
    
    // 检查编辑日志是否同步
    if (!sharedEditsManager.isInSync()) {
      return false;
    }
    
    // 检查最小DataNode数量
    int minDataNodes = conf.getInt("dfs.namenode.safemode.min.datanodes", 1);
    if (fsn.getNumLiveDataNodes() < minDataNodes) {
      return false;
    }
    
    return true;
  }
  
  private FSNamesystem getFSNamesystem() {
    return NameNode.getNamesystem();
  }
  
  private void startHttpServer() throws IOException {
    // 启动HTTP服务器逻辑
  }
  
  private void stopHttpServer() throws IOException {
    // 停止HTTP服务器逻辑
  }
}

/**
 * 共享编辑日志管理器
 */
public class SharedEditsManager {
  
  private final Configuration conf;
  private final List<JournalManager> journalManagers;
  private final QuorumJournalManager qjm;
  
  public SharedEditsManager(Configuration conf) {
    this.conf = conf;
    this.journalManagers = new ArrayList<>();
    this.qjm = createQuorumJournalManager();
  }
  
  public void initialize() throws IOException {
    // 初始化Journal管理器
    qjm.format(NamespaceInfo.getNamespaceInfo(conf));
    
    // 恢复编辑日志
    qjm.recoverUnfinalizedSegments();
    
    LOG.info("Shared edits manager initialized");
  }
  
  /**
   * 获取共享锁
   */
  public boolean acquireSharedLock() throws IOException {
    try {
      qjm.selectInputStreams(Collections.emptyList(), 0, false);
      return true;
    } catch (IOException e) {
      LOG.error("Failed to acquire shared lock", e);
      return false;
    }
  }
  
  /**
   * 释放共享锁
   */
  public void releaseSharedLock() throws IOException {
    // 释放锁的逻辑
    qjm.close();
  }
  
  /**
   * 获取最后的事务ID
   */
  public long getLastTransactionId() throws IOException {
    return qjm.getNumberOfTransactions(Long.MAX_VALUE, false);
  }
  
  /**
   * 跟踪共享编辑日志
   */
  public void tailSharedEdits(long fromTxId) throws IOException {
    Collection<EditLogInputStream> streams = qjm.selectInputStreams(
        Collections.emptyList(), fromTxId, false);
    
    for (EditLogInputStream stream : streams) {
      try {
        FSEditLogLoader loader = new FSEditLogLoader(getFSNamesystem(), fromTxId);
        long numLoaded = loader.loadFSEdits(stream, fromTxId);
        LOG.info("Loaded {} transactions from shared edits", numLoaded);
      } finally {
        stream.close();
      }
    }
  }
  
  /**
   * 检查是否与共享存储同步
   */
  public boolean isInSync() {
    try {
      long sharedTxId = getLastTransactionId();
      long localTxId = getFSNamesystem().getEditLog().getLastWrittenTxId();
      return Math.abs(sharedTxId - localTxId) <= 1; // 允许1个事务的差异
    } catch (IOException e) {
      LOG.error("Failed to check sync status", e);
      return false;
    }
  }
  
  private QuorumJournalManager createQuorumJournalManager() {
    String journalURI = conf.get("dfs.namenode.shared.edits.dir");
    URI uri = URI.create(journalURI);
    
    return new QuorumJournalManager(conf, uri, NamespaceInfo.getNamespaceInfo(conf));
  }
  
  private FSNamesystem getFSNamesystem() {
    return NameNode.getNamesystem();
  }
}

/**
 * 故障转移控制器
 */
public class FailoverController {
  
  private final Configuration conf;
  private final HealthMonitor healthMonitor;
  private final ActiveStandbyElector elector;
  private final FailoverPolicy failoverPolicy;
  
  public FailoverController(Configuration conf) {
    this.conf = conf;
    this.healthMonitor = new HealthMonitor(conf);
    this.elector = new ActiveStandbyElector(conf);
    this.failoverPolicy = new FailoverPolicy(conf);
  }
  
  public void start() throws IOException {
    // 启动健康监控
    healthMonitor.start();
    
    // 启动选举器
    elector.start();
    
    LOG.info("Failover controller started");
  }
  
  /**
   * 执行故障转移
   */
  public void performFailover(String targetNode) throws IOException {
    LOG.info("Performing failover to {}", targetNode);
    
    try {
      // 1. 检查故障转移策略
      if (!failoverPolicy.shouldFailover()) {
        throw new IOException("Failover policy prevents failover");
      }
      
      // 2. 执行故障转移
      HAServiceTarget target = new HAServiceTarget(targetNode);
      
      // 3. 将当前Active转为Standby
      if (getCurrentActiveNode() != null) {
        transitionToStandby(getCurrentActiveNode());
      }
      
      // 4. 将目标节点转为Active
      transitionToActive(target);
      
      LOG.info("Failover completed successfully");
      
    } catch (Exception e) {
      LOG.error("Failover failed", e);
      throw new IOException("Failover failed", e);
    }
  }
  
  private void transitionToActive(HAServiceTarget target) throws IOException {
    HAServiceProtocol proxy = target.getProxy(conf, 10000);
    proxy.transitionToActive(new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER));
  }
  
  private void transitionToStandby(HAServiceTarget target) throws IOException {
    HAServiceProtocol proxy = target.getProxy(conf, 10000);
    proxy.transitionToStandby(new StateChangeRequestInfo(RequestSource.REQUEST_BY_USER));
  }
  
  private HAServiceTarget getCurrentActiveNode() {
    // 获取当前Active节点的逻辑
    return null;
  }
}

/**
 * ZooKeeper故障转移控制器
 */
public class ZKFailoverController extends ActiveStandbyElector.ActiveStandbyElectorCallback {
  
  private final Configuration conf;
  private final ActiveStandbyElector elector;
  private final HealthMonitor healthMonitor;
  private final String zkQuorum;
  private final String zkParentPath;
  
  public ZKFailoverController(Configuration conf) {
    this.conf = conf;
    this.zkQuorum = conf.get("ha.zookeeper.quorum");
    this.zkParentPath = conf.get("ha.zookeeper.parent-znode", "/hadoop-ha");
    
    this.elector = new ActiveStandbyElector(zkQuorum, 5000, zkParentPath, 
                                           Collections.emptyList(), this);
    this.healthMonitor = new HealthMonitor(conf);
  }
  
  public void start() throws IOException {
    // 启动健康监控
    healthMonitor.start();
    
    // 加入选举
    elector.joinElection(getLocalNodeData());
    
    LOG.info("ZK Failover Controller started");
  }
  
  @Override
  public void becomeActive() throws ServiceFailedException {
    try {
      LOG.info("Becoming active via ZK election");
      
      // 转换为Active状态
      HAServiceTarget localTarget = getLocalTarget();
      HAServiceProtocol proxy = localTarget.getProxy(conf, 10000);
      proxy.transitionToActive(new StateChangeRequestInfo(RequestSource.REQUEST_BY_ZK_FC));
      
      LOG.info("Successfully became active");
      
    } catch (Exception e) {
      LOG.error("Failed to become active", e);
      throw new ServiceFailedException("Failed to become active", e);
    }
  }
  
  @Override
  public void becomeStandby() {
    try {
      LOG.info("Becoming standby via ZK election");
      
      // 转换为Standby状态
      HAServiceTarget localTarget = getLocalTarget();
      HAServiceProtocol proxy = localTarget.getProxy(conf, 10000);
      proxy.transitionToStandby(new StateChangeRequestInfo(RequestSource.REQUEST_BY_ZK_FC));
      
      LOG.info("Successfully became standby");
      
    } catch (Exception e) {
      LOG.error("Failed to become standby", e);
    }
  }
  
  @Override
  public void enterNeutralMode() {
    LOG.info("Entering neutral mode");
    // 进入中性模式的逻辑
  }
  
  @Override
  public void notifyFatalError(String errorMessage) {
    LOG.fatal("Fatal error in ZK Failover Controller: {}", errorMessage);
    // 处理致命错误
  }
  
  @Override
  public void fenceOldActive(byte[] oldActiveData) {
    LOG.info("Fencing old active node");
    
    try {
      // 解析旧Active节点信息
      String oldActiveNode = new String(oldActiveData);
      HAServiceTarget oldTarget = new HAServiceTarget(oldActiveNode);
      
      // 强制转换为Standby
      HAServiceProtocol proxy = oldTarget.getProxy(conf, 5000);
      proxy.transitionToStandby(new StateChangeRequestInfo(RequestSource.REQUEST_BY_ZK_FC));
      
      LOG.info("Successfully fenced old active node: {}", oldActiveNode);
      
    } catch (Exception e) {
      LOG.error("Failed to fence old active node", e);
      // 可能需要更强制的隔离措施
    }
  }
  
  private byte[] getLocalNodeData() {
    // 返回本地节点的标识数据
    String localNode = conf.get("dfs.nameservices") + ":" + 
                      conf.get("dfs.ha.namenode.id");
    return localNode.getBytes();
  }
  
  private HAServiceTarget getLocalTarget() {
    String localNode = conf.get("dfs.ha.namenode.id");
    return new HAServiceTarget(localNode);
  }
}

/**
 * 健康监控器
 */
public class HealthMonitor {
  
  private final Configuration conf;
  private final ScheduledExecutorService scheduler;
  private final List<HealthCheck> healthChecks;
  private volatile boolean isHealthy = true;
  
  public HealthMonitor(Configuration conf) {
    this.conf = conf;
    this.scheduler = Executors.newScheduledThreadPool(2);
    this.healthChecks = Arrays.asList(
        new NameNodeHealthCheck(),
        new JVMHealthCheck(),
        new DiskHealthCheck()
    );
  }
  
  public void start() {
    // 定期执行健康检查
    scheduler.scheduleAtFixedRate(this::performHealthCheck, 0, 5, TimeUnit.SECONDS);
    LOG.info("Health monitor started");
  }
  
  private void performHealthCheck() {
    boolean healthy = true;
    
    for (HealthCheck check : healthChecks) {
      try {
        if (!check.isHealthy()) {
          LOG.warn("Health check failed: {}", check.getName());
          healthy = false;
        }
      } catch (Exception e) {
        LOG.error("Health check error: " + check.getName(), e);
        healthy = false;
      }
    }
    
    if (isHealthy != healthy) {
      isHealthy = healthy;
      LOG.info("Health status changed to: {}", healthy ? "HEALTHY" : "UNHEALTHY");
    }
  }
  
  public boolean isHealthy() {
    return isHealthy;
  }
  
  /**
   * NameNode健康检查
   */
  private static class NameNodeHealthCheck implements HealthCheck {
    
    @Override
    public String getName() {
      return "NameNode Health";
    }
    
    @Override
    public boolean isHealthy() {
      try {
        FSNamesystem fsn = NameNode.getNamesystem();
        
        // 检查是否在安全模式
        if (fsn.isInSafeMode()) {
          return false;
        }
        
        // 检查活跃DataNode数量
        int liveNodes = fsn.getNumLiveDataNodes();
        int minNodes = fsn.getConf().getInt("dfs.namenode.safemode.min.datanodes", 1);
        
        return liveNodes >= minNodes;
        
      } catch (Exception e) {
        return false;
      }
    }
  }
  
  /**
   * JVM健康检查
   */
  private static class JVMHealthCheck implements HealthCheck {
    
    @Override
    public String getName() {
      return "JVM Health";
    }
    
    @Override
    public boolean isHealthy() {
      // 检查内存使用率
      MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
      MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
      
      double usagePercent = (double) heapUsage.getUsed() / heapUsage.getMax() * 100;
      
      // 如果内存使用率超过90%，认为不健康
      return usagePercent < 90.0;
    }
  }
  
  /**
   * 磁盘健康检查
   */
  private static class DiskHealthCheck implements HealthCheck {
    
    @Override
    public String getName() {
      return "Disk Health";
    }
    
    @Override
    public boolean isHealthy() {
      // 检查磁盘空间
      File[] roots = File.listRoots();
      
      for (File root : roots) {
        long total = root.getTotalSpace();
        long free = root.getFreeSpace();
        
        if (total > 0) {
          double freePercent = (double) free / total * 100;
          
          // 如果任何磁盘的剩余空间少于10%，认为不健康
          if (freePercent < 10.0) {
            return false;
          }
        }
      }
      
      return true;
    }
  }
  
  /**
   * 健康检查接口
   */
  public interface HealthCheck {
    String getName();
    boolean isHealthy();
  }
}
```

### 16.2.2 QJM（Quorum Journal Manager）实现

QJM是NameNode HA的核心组件，负责管理共享编辑日志：

```java
/**
 * Quorum Journal Manager实现
 */
public class QuorumJournalManager implements JournalManager {
  
  private final Configuration conf;
  private final URI uri;
  private final NamespaceInfo nsInfo;
  private final List<AsyncLogger> loggers;
  private final QuorumCall.Factory<AsyncLogger, Void> qcFactory;
  private final int minimumRedundantJournals;
  
  public QuorumJournalManager(Configuration conf, URI uri, NamespaceInfo nsInfo) {
    this.conf = conf;
    this.uri = uri;
    this.nsInfo = nsInfo;
    this.loggers = createLoggers();
    this.qcFactory = QuorumCall.factory();
    this.minimumRedundantJournals = (loggers.size() + 1) / 2; // 大多数
  }
  
  private List<AsyncLogger> createLoggers() {
    List<AsyncLogger> loggers = new ArrayList<>();
    
    // 解析Journal节点地址
    String[] journalNodes = uri.getAuthority().split(";");
    
    for (String journalNode : journalNodes) {
      String[] parts = journalNode.split(":");
      String host = parts[0];
      int port = parts.length > 1 ? Integer.parseInt(parts[1]) : 8485;
      
      InetSocketAddress addr = new InetSocketAddress(host, port);
      AsyncLogger logger = new IPCLoggerChannel(conf, nsInfo, 
                                               uri.getPath(), addr);
      loggers.add(logger);
    }
    
    return loggers;
  }
  
  @Override
  public void format(NamespaceInfo nsInfo) throws IOException {
    QuorumCall<AsyncLogger, Void> qcall = qcFactory.newQuorumCall(
        loggers, new FormatCall(nsInfo));
    
    try {
      qcall.waitFor(loggers.size(), loggers.size(), 0, 60000, "format");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while formatting", e);
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for format response", e);
    }
    
    LOG.info("Successfully formatted {} journal nodes", loggers.size());
  }
  
  @Override
  public EditLogOutputStream startLogSegment(long txid, int layoutVersion) 
      throws IOException {
    
    QuorumCall<AsyncLogger, Void> qcall = qcFactory.newQuorumCall(
        loggers, new StartLogSegmentCall(txid, layoutVersion));
    
    try {
      qcall.waitFor(minimumRedundantJournals, loggers.size(), 0, 30000, 
                   "startLogSegment");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while starting log segment", e);
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for startLogSegment response", e);
    }
    
    return new QuorumOutputStream(loggers, txid, qcFactory, minimumRedundantJournals);
  }
  
  @Override
  public void finalizeLogSegment(long firstTxId, long lastTxId) throws IOException {
    QuorumCall<AsyncLogger, Void> qcall = qcFactory.newQuorumCall(
        loggers, new FinalizeLogSegmentCall(firstTxId, lastTxId));
    
    try {
      qcall.waitFor(minimumRedundantJournals, loggers.size(), 0, 30000, 
                   "finalizeLogSegment");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new IOException("Interrupted while finalizing log segment", e);
    } catch (TimeoutException e) {
      throw new IOException("Timed out waiting for finalizeLogSegment response", e);
    }
    
    LOG.info("Successfully finalized log segment [{}, {}]", firstTxId, lastTxId);
  }
  
  @Override
  public void recoverUnfinalizedSegments() throws IOException {
    // 1. 获取所有Journal节点的段信息
    Map<AsyncLogger, RemoteEditLogManifest> manifests = new HashMap<>();
    
    for (AsyncLogger logger : loggers) {
      try {
        RemoteEditLogManifest manifest = logger.getEditLogManifest(0, false);
        manifests.put(logger, manifest);
      } catch (IOException e) {
        LOG.warn("Failed to get manifest from logger: " + logger, e);
      }
    }
    
    // 2. 找出需要恢复的段
    Set<LogSegment> unfinalizedSegments = findUnfinalizedSegments(manifests);
    
    // 3. 恢复每个未完成的段
    for (LogSegment segment : unfinalizedSegments) {
      recoverSegment(segment, manifests);
    }
  }
  
  private Set<LogSegment> findUnfinalizedSegments(
      Map<AsyncLogger, RemoteEditLogManifest> manifests) {
    
    Set<LogSegment> allSegments = new HashSet<>();
    
    // 收集所有段
    for (RemoteEditLogManifest manifest : manifests.values()) {
      allSegments.addAll(manifest.getLogs());
    }
    
    // 找出未完成的段
    return allSegments.stream()
        .filter(segment -> !segment.isFinalized())
        .collect(Collectors.toSet());
  }
  
  private void recoverSegment(LogSegment segment, 
                             Map<AsyncLogger, RemoteEditLogManifest> manifests) 
      throws IOException {
    
    LOG.info("Recovering segment: {}", segment);
    
    // 找出拥有此段的Journal节点
    List<AsyncLogger> loggersWithSegment = new ArrayList<>();
    
    for (Map.Entry<AsyncLogger, RemoteEditLogManifest> entry : manifests.entrySet()) {
      if (entry.getValue().getLogs().contains(segment)) {
        loggersWithSegment.add(entry.getKey());
      }
    }
    
    if (loggersWithSegment.size() < minimumRedundantJournals) {
      throw new IOException("Not enough loggers have segment " + segment + 
                           " to perform recovery");
    }
    
    // 选择最长的段进行恢复
    AsyncLogger bestLogger = selectBestLogger(loggersWithSegment, segment);
    
    // 从最佳Logger读取段数据
    EditLogInputStream stream = bestLogger.getInputStream(segment.getStartTxId(), 
                                                         segment.getEndTxId());
    
    // 将数据同步到其他Journal节点
    syncSegmentToOtherLoggers(stream, segment, loggersWithSegment, bestLogger);
    
    // 完成段
    finalizeLogSegment(segment.getStartTxId(), segment.getEndTxId());
  }
  
  private AsyncLogger selectBestLogger(List<AsyncLogger> loggers, LogSegment segment) {
    // 选择拥有最长段数据的Logger
    AsyncLogger bestLogger = null;
    long maxLength = -1;
    
    for (AsyncLogger logger : loggers) {
      try {
        long length = logger.getSegmentLength(segment.getStartTxId());
        if (length > maxLength) {
          maxLength = length;
          bestLogger = logger;
        }
      } catch (IOException e) {
        LOG.warn("Failed to get segment length from logger: " + logger, e);
      }
    }
    
    return bestLogger;
  }
  
  private void syncSegmentToOtherLoggers(EditLogInputStream stream, 
                                        LogSegment segment,
                                        List<AsyncLogger> allLoggers, 
                                        AsyncLogger sourceLogger) throws IOException {
    
    byte[] data = readSegmentData(stream);
    
    for (AsyncLogger logger : allLoggers) {
      if (logger != sourceLogger) {
        try {
          logger.writeSegmentData(segment.getStartTxId(), data);
        } catch (IOException e) {
          LOG.warn("Failed to sync segment data to logger: " + logger, e);
        }
      }
    }
  }
  
  private byte[] readSegmentData(EditLogInputStream stream) throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    byte[] buffer = new byte[8192];
    int bytesRead;
    
    while ((bytesRead = stream.read(buffer)) != -1) {
      baos.write(buffer, 0, bytesRead);
    }
    
    return baos.toByteArray();
  }
  
  @Override
  public Collection<EditLogInputStream> selectInputStreams(
      Collection<EditLogInputStream> streams, long fromTxnId, boolean inProgressOk) 
      throws IOException {
    
    // 从多个Journal节点选择最佳的输入流
    Map<AsyncLogger, RemoteEditLogManifest> manifests = new HashMap<>();
    
    for (AsyncLogger logger : loggers) {
      try {
        RemoteEditLogManifest manifest = logger.getEditLogManifest(fromTxnId, inProgressOk);
        manifests.put(logger, manifest);
      } catch (IOException e) {
        LOG.warn("Failed to get manifest from logger: " + logger, e);
      }
    }
    
    // 选择最佳的段组合
    List<RemoteEditLog> bestLogs = selectBestLogs(manifests, fromTxnId);
    
    // 创建输入流
    List<EditLogInputStream> inputStreams = new ArrayList<>();
    
    for (RemoteEditLog log : bestLogs) {
      AsyncLogger logger = selectLoggerForLog(log, manifests);
      EditLogInputStream stream = logger.getInputStream(log.getStartTxId(), 
                                                       log.getEndTxId());
      inputStreams.add(stream);
    }
    
    return inputStreams;
  }
  
  private List<RemoteEditLog> selectBestLogs(Map<AsyncLogger, RemoteEditLogManifest> manifests, 
                                           long fromTxnId) {
    // 实现最佳日志选择算法
    // 这里简化为选择第一个可用的manifest
    for (RemoteEditLogManifest manifest : manifests.values()) {
      List<RemoteEditLog> logs = manifest.getLogs();
      return logs.stream()
          .filter(log -> log.getEndTxId() >= fromTxnId)
          .collect(Collectors.toList());
    }
    
    return Collections.emptyList();
  }
  
  private AsyncLogger selectLoggerForLog(RemoteEditLog log, 
                                        Map<AsyncLogger, RemoteEditLogManifest> manifests) {
    // 选择拥有此日志的第一个可用Logger
    for (Map.Entry<AsyncLogger, RemoteEditLogManifest> entry : manifests.entrySet()) {
      if (entry.getValue().getLogs().contains(log)) {
        return entry.getKey();
      }
    }
    
    throw new IllegalStateException("No logger found for log: " + log);
  }
  
  @Override
  public void close() throws IOException {
    for (AsyncLogger logger : loggers) {
      try {
        logger.close();
      } catch (IOException e) {
        LOG.warn("Failed to close logger: " + logger, e);
      }
    }
  }
}
```

## 16.3 本章小结

本章深入分析了Hadoop高可用与容灾机制的设计原理和技术实现。高可用性是企业级Hadoop集群的核心要求，通过消除单点故障、实现自动故障转移和数据一致性保证，确保系统的持续可用性。

**核心要点**：

**NameNode HA架构**：通过Active/Standby模式实现主备切换，使用共享存储同步元数据。

**QJM机制**：Quorum Journal Manager提供了可靠的共享编辑日志管理，确保数据一致性。

**故障转移控制**：自动故障检测和转移机制，最小化故障恢复时间。

**ZooKeeper集成**：利用ZooKeeper实现分布式协调和领导者选举。

**健康监控**：全面的健康检查机制，及时发现和处理异常情况。

## 16.3 ResourceManager高可用实现

### 16.3.1 ResourceManager HA架构

ResourceManager高可用确保YARN调度服务的连续性：

```java
/**
 * ResourceManager高可用管理器
 */
public class ResourceManagerHAManager {

  private final Configuration conf;
  private final RMContext rmContext;
  private final HAServiceState currentState;
  private final EmbeddedElectorService electorService;
  private final AdminService adminService;

  public ResourceManagerHAManager(RMContext rmContext, Configuration conf) {
    this.rmContext = rmContext;
    this.conf = conf;
    this.currentState = HAServiceState.STANDBY;
    this.electorService = createElectorService();
    this.adminService = new AdminService(this);
  }

  private EmbeddedElectorService createElectorService() {
    String zkQuorum = conf.get("yarn.resourcemanager.zk-address");
    String zkParentPath = conf.get("yarn.resourcemanager.ha.automatic-failover.zk-base-path",
                                  "/yarn-leader-election");

    return new EmbeddedElectorService(zkQuorum, zkParentPath, this);
  }

  /**
   * 初始化HA服务
   */
  public void initialize() throws Exception {
    // 启动选举服务
    electorService.start();

    // 启动管理服务
    adminService.start();

    LOG.info("ResourceManager HA initialized");
  }

  /**
   * 转换为Active状态
   */
  public synchronized void transitionToActive() throws Exception {
    if (currentState == HAServiceState.ACTIVE) {
      LOG.warn("Already in active state");
      return;
    }

    try {
      LOG.info("Transitioning ResourceManager to Active state");

      // 1. 启动Active服务
      startActiveServices();

      // 2. 恢复应用状态
      recoverApplications();

      // 3. 启动调度器
      startScheduler();

      // 4. 启动NodeManager心跳处理
      startNodeManagerHeartbeat();

      // 5. 更新状态
      currentState = HAServiceState.ACTIVE;

      LOG.info("Successfully transitioned to Active state");

    } catch (Exception e) {
      LOG.error("Failed to transition to Active state", e);
      throw e;
    }
  }

  /**
   * 转换为Standby状态
   */
  public synchronized void transitionToStandby() throws Exception {
    if (currentState == HAServiceState.STANDBY) {
      LOG.warn("Already in standby state");
      return;
    }

    try {
      LOG.info("Transitioning ResourceManager to Standby state");

      // 1. 停止Active服务
      stopActiveServices();

      // 2. 清理内存状态
      cleanupMemoryState();

      // 3. 启动Standby服务
      startStandbyServices();

      // 4. 更新状态
      currentState = HAServiceState.STANDBY;

      LOG.info("Successfully transitioned to Standby state");

    } catch (Exception e) {
      LOG.error("Failed to transition to Standby state", e);
      throw e;
    }
  }

  private void startActiveServices() throws Exception {
    // 启动应用管理器
    rmContext.getApplicationMasterService().start();

    // 启动客户端服务
    rmContext.getClientRMService().start();

    // 启动资源跟踪器
    rmContext.getResourceTrackerService().start();

    // 启动Web应用
    rmContext.getRMWebApp().start();

    LOG.info("Active services started");
  }

  private void stopActiveServices() throws Exception {
    // 停止Web应用
    rmContext.getRMWebApp().stop();

    // 停止资源跟踪器
    rmContext.getResourceTrackerService().stop();

    // 停止客户端服务
    rmContext.getClientRMService().stop();

    // 停止应用管理器
    rmContext.getApplicationMasterService().stop();

    LOG.info("Active services stopped");
  }

  private void recoverApplications() throws Exception {
    RMStateStore stateStore = rmContext.getStateStore();

    // 从状态存储恢复应用
    RMStateStore.RMState state = stateStore.loadState();

    for (ApplicationStateData appState : state.getApplicationState().values()) {
      try {
        // 恢复应用
        ApplicationId appId = appState.getApplicationSubmissionContext().getApplicationId();
        rmContext.getRMApps().put(appId, recoverApplication(appState));

        LOG.info("Recovered application: {}", appId);

      } catch (Exception e) {
        LOG.error("Failed to recover application: " + appState.getApplicationId(), e);
      }
    }

    LOG.info("Application recovery completed");
  }

  private RMApp recoverApplication(ApplicationStateData appState) throws Exception {
    ApplicationSubmissionContext submissionContext = appState.getApplicationSubmissionContext();

    // 创建RMApp实例
    RMApp app = new RMAppImpl(submissionContext.getApplicationId(),
                             rmContext,
                             conf,
                             submissionContext.getApplicationName(),
                             submissionContext.getUser(),
                             submissionContext.getQueue(),
                             submissionContext,
                             rmContext.getScheduler(),
                             rmContext.getApplicationMasterService(),
                             appState.getSubmitTime(),
                             submissionContext.getApplicationType(),
                             submissionContext.getApplicationTags(),
                             appState.getApplicationAttempts());

    // 恢复应用状态
    app.recover(appState);

    return app;
  }

  private void startScheduler() throws Exception {
    ResourceScheduler scheduler = rmContext.getScheduler();

    if (scheduler instanceof AbstractYarnScheduler) {
      ((AbstractYarnScheduler) scheduler).transitionToActive();
    }

    LOG.info("Scheduler started");
  }

  private void startNodeManagerHeartbeat() throws Exception {
    ResourceTrackerService resourceTracker = rmContext.getResourceTrackerService();
    resourceTracker.startHeartbeatHandling();

    LOG.info("NodeManager heartbeat handling started");
  }

  private void cleanupMemoryState() {
    // 清理内存中的应用状态
    rmContext.getRMApps().clear();

    // 清理节点状态
    rmContext.getRMNodes().clear();

    // 重置调度器状态
    ResourceScheduler scheduler = rmContext.getScheduler();
    if (scheduler instanceof AbstractYarnScheduler) {
      ((AbstractYarnScheduler) scheduler).resetSchedulerMetrics();
    }

    LOG.info("Memory state cleaned up");
  }

  private void startStandbyServices() throws Exception {
    // 在Standby模式下，只启动必要的服务
    // 主要是状态存储的监听服务

    LOG.info("Standby services started");
  }

  /**
   * 获取服务状态
   */
  public HAServiceStatus getServiceStatus() {
    HAServiceStatus status = new HAServiceStatus(currentState);

    // 设置准备状态
    status.setReadyToBecomeActive(isReadyToBecomeActive());

    // 设置健康状态
    status.setHealthy(isHealthy());

    return status;
  }

  private boolean isReadyToBecomeActive() {
    // 检查是否准备好成为Active

    // 检查状态存储是否可用
    if (!rmContext.getStateStore().isInState(RMStateStore.State.ACTIVE)) {
      return false;
    }

    // 检查关键服务是否就绪
    if (!rmContext.getScheduler().isReady()) {
      return false;
    }

    return true;
  }

  private boolean isHealthy() {
    // 检查ResourceManager健康状态

    // 检查内存使用
    MemoryMXBean memoryBean = ManagementFactory.getMemoryMXBean();
    MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
    double memoryUsage = (double) heapUsage.getUsed() / heapUsage.getMax();

    if (memoryUsage > 0.9) { // 内存使用超过90%
      return false;
    }

    // 检查线程数
    ThreadMXBean threadBean = ManagementFactory.getThreadMXBean();
    int threadCount = threadBean.getThreadCount();

    if (threadCount > 10000) { // 线程数过多
      return false;
    }

    return true;
  }

  public HAServiceState getCurrentState() {
    return currentState;
  }
}

/**
 * 嵌入式选举服务
 */
public class EmbeddedElectorService extends ActiveStandbyElector.ActiveStandbyElectorCallback {

  private final String zkQuorum;
  private final String zkParentPath;
  private final ResourceManagerHAManager haManager;
  private final ActiveStandbyElector elector;

  public EmbeddedElectorService(String zkQuorum, String zkParentPath,
                               ResourceManagerHAManager haManager) {
    this.zkQuorum = zkQuorum;
    this.zkParentPath = zkParentPath;
    this.haManager = haManager;

    this.elector = new ActiveStandbyElector(zkQuorum, 5000, zkParentPath,
                                           Collections.emptyList(), this);
  }

  public void start() throws Exception {
    // 加入选举
    elector.joinElection(getLocalNodeData());
    LOG.info("Embedded elector service started");
  }

  public void stop() throws Exception {
    elector.quitElection(false);
    elector.terminateConnection();
    LOG.info("Embedded elector service stopped");
  }

  @Override
  public void becomeActive() throws ServiceFailedException {
    try {
      LOG.info("Becoming active via ZK election");
      haManager.transitionToActive();
    } catch (Exception e) {
      throw new ServiceFailedException("Failed to become active", e);
    }
  }

  @Override
  public void becomeStandby() {
    try {
      LOG.info("Becoming standby via ZK election");
      haManager.transitionToStandby();
    } catch (Exception e) {
      LOG.error("Failed to become standby", e);
    }
  }

  @Override
  public void enterNeutralMode() {
    LOG.info("Entering neutral mode");
  }

  @Override
  public void notifyFatalError(String errorMessage) {
    LOG.fatal("Fatal error in embedded elector: {}", errorMessage);
  }

  @Override
  public void fenceOldActive(byte[] oldActiveData) {
    LOG.info("Fencing old active ResourceManager");
    // ResourceManager的隔离逻辑
  }

  private byte[] getLocalNodeData() {
    String rmId = haManager.conf.get("yarn.resourcemanager.ha.rm-ids");
    return rmId.getBytes();
  }
}
```

### 16.3.2 状态存储与恢复

ResourceManager使用状态存储来保持应用状态的持久化：

```java
/**
 * ResourceManager状态存储管理器
 */
public class RMStateStoreManager {

  private final Configuration conf;
  private final RMStateStore stateStore;
  private final StateStoreEventDispatcher dispatcher;

  public RMStateStoreManager(Configuration conf) {
    this.conf = conf;
    this.stateStore = createStateStore();
    this.dispatcher = new StateStoreEventDispatcher();
  }

  private RMStateStore createStateStore() {
    String storeClass = conf.get("yarn.resourcemanager.store.class",
                                "org.apache.hadoop.yarn.server.resourcemanager.recovery.ZKRMStateStore");

    try {
      Class<?> clazz = Class.forName(storeClass);
      return (RMStateStore) clazz.newInstance();
    } catch (Exception e) {
      throw new RuntimeException("Failed to create state store", e);
    }
  }

  /**
   * 初始化状态存储
   */
  public void initialize() throws Exception {
    stateStore.init(conf);
    stateStore.start();

    // 启动事件分发器
    dispatcher.start();

    LOG.info("State store initialized: {}", stateStore.getClass().getSimpleName());
  }

  /**
   * 存储应用状态
   */
  public void storeApplication(RMApp app) throws Exception {
    ApplicationStateData appState = createApplicationStateData(app);

    stateStore.storeApplicationStateInternal(app.getApplicationId(), appState);

    LOG.debug("Stored application state: {}", app.getApplicationId());
  }

  /**
   * 更新应用状态
   */
  public void updateApplication(RMApp app) throws Exception {
    ApplicationStateData appState = createApplicationStateData(app);

    stateStore.updateApplicationStateInternal(app.getApplicationId(), appState);

    LOG.debug("Updated application state: {}", app.getApplicationId());
  }

  /**
   * 删除应用状态
   */
  public void removeApplication(ApplicationId appId) throws Exception {
    stateStore.removeApplicationStateInternal(appId);

    LOG.debug("Removed application state: {}", appId);
  }

  /**
   * 存储应用尝试状态
   */
  public void storeApplicationAttempt(RMAppAttempt appAttempt) throws Exception {
    ApplicationAttemptStateData attemptState = createApplicationAttemptStateData(appAttempt);

    stateStore.storeApplicationAttemptStateInternal(appAttempt.getAppAttemptId(), attemptState);

    LOG.debug("Stored application attempt state: {}", appAttempt.getAppAttemptId());
  }

  /**
   * 更新应用尝试状态
   */
  public void updateApplicationAttempt(RMAppAttempt appAttempt) throws Exception {
    ApplicationAttemptStateData attemptState = createApplicationAttemptStateData(appAttempt);

    stateStore.updateApplicationAttemptStateInternal(appAttempt.getAppAttemptId(), attemptState);

    LOG.debug("Updated application attempt state: {}", appAttempt.getAppAttemptId());
  }

  /**
   * 删除应用尝试状态
   */
  public void removeApplicationAttempt(ApplicationAttemptId attemptId) throws Exception {
    stateStore.removeApplicationAttemptStateInternal(attemptId);

    LOG.debug("Removed application attempt state: {}", attemptId);
  }

  /**
   * 加载状态
   */
  public RMStateStore.RMState loadState() throws Exception {
    return stateStore.loadState();
  }

  private ApplicationStateData createApplicationStateData(RMApp app) {
    ApplicationSubmissionContext submissionContext = app.getApplicationSubmissionContext();

    ApplicationStateData.Builder builder = ApplicationStateData.newBuilder()
        .setApplicationSubmissionContext(submissionContext)
        .setSubmitTime(app.getSubmitTime())
        .setStartTime(app.getStartTime())
        .setUser(app.getUser())
        .setState(app.getState());

    // 添加应用尝试信息
    for (RMAppAttempt attempt : app.getAppAttempts().values()) {
      ApplicationAttemptStateData attemptData = createApplicationAttemptStateData(attempt);
      builder.putApplicationAttempts(attempt.getAppAttemptId().toString(), attemptData);
    }

    return builder.build();
  }

  private ApplicationAttemptStateData createApplicationAttemptStateData(RMAppAttempt attempt) {
    ApplicationAttemptStateData.Builder builder = ApplicationAttemptStateData.newBuilder()
        .setAttemptId(attempt.getAppAttemptId())
        .setMasterContainer(attempt.getMasterContainer())
        .setState(attempt.getAppAttemptState())
        .setFinalTrackingUrl(attempt.getFinalTrackingUrl())
        .setDiagnostics(attempt.getDiagnostics());

    return builder.build();
  }

  /**
   * 状态存储事件分发器
   */
  private class StateStoreEventDispatcher extends Thread {

    private final BlockingQueue<StateStoreEvent> eventQueue;
    private volatile boolean running = true;

    public StateStoreEventDispatcher() {
      this.eventQueue = new LinkedBlockingQueue<>();
      setName("StateStoreEventDispatcher");
      setDaemon(true);
    }

    @Override
    public void run() {
      while (running) {
        try {
          StateStoreEvent event = eventQueue.take();
          handleEvent(event);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          break;
        } catch (Exception e) {
          LOG.error("Error handling state store event", e);
        }
      }
    }

    public void addEvent(StateStoreEvent event) {
      eventQueue.offer(event);
    }

    private void handleEvent(StateStoreEvent event) throws Exception {
      switch (event.getType()) {
        case STORE_APP:
          storeApplication((RMApp) event.getData());
          break;
        case UPDATE_APP:
          updateApplication((RMApp) event.getData());
          break;
        case REMOVE_APP:
          removeApplication((ApplicationId) event.getData());
          break;
        case STORE_APP_ATTEMPT:
          storeApplicationAttempt((RMAppAttempt) event.getData());
          break;
        case UPDATE_APP_ATTEMPT:
          updateApplicationAttempt((RMAppAttempt) event.getData());
          break;
        case REMOVE_APP_ATTEMPT:
          removeApplicationAttempt((ApplicationAttemptId) event.getData());
          break;
        default:
          LOG.warn("Unknown state store event type: {}", event.getType());
      }
    }

    public void shutdown() {
      running = false;
      interrupt();
    }
  }

  /**
   * 状态存储事件
   */
  public static class StateStoreEvent {

    public enum Type {
      STORE_APP, UPDATE_APP, REMOVE_APP,
      STORE_APP_ATTEMPT, UPDATE_APP_ATTEMPT, REMOVE_APP_ATTEMPT
    }

    private final Type type;
    private final Object data;

    public StateStoreEvent(Type type, Object data) {
      this.type = type;
      this.data = data;
    }

    public Type getType() { return type; }
    public Object getData() { return data; }
  }
}
```

## 16.4 容灾机制与数据备份

### 16.4.1 跨数据中心容灾

Hadoop容灾机制确保在大规模故障时的业务连续性：

```java
/**
 * Hadoop容灾管理器
 */
public class HadoopDisasterRecoveryManager {

  private final Configuration conf;
  private final List<DataCenter> dataCenters;
  private final ReplicationManager replicationManager;
  private final BackupManager backupManager;
  private final FailoverOrchestrator failoverOrchestrator;

  public HadoopDisasterRecoveryManager(Configuration conf) {
    this.conf = conf;
    this.dataCenters = loadDataCenters();
    this.replicationManager = new ReplicationManager(conf);
    this.backupManager = new BackupManager(conf);
    this.failoverOrchestrator = new FailoverOrchestrator(conf);
  }

  private List<DataCenter> loadDataCenters() {
    List<DataCenter> centers = new ArrayList<>();

    String[] dcNames = conf.getStrings("hadoop.disaster-recovery.datacenters");

    for (String dcName : dcNames) {
      String dcConfig = "hadoop.disaster-recovery.datacenter." + dcName;

      DataCenter dc = new DataCenter(
          dcName,
          conf.get(dcConfig + ".location"),
          conf.get(dcConfig + ".namenode.address"),
          conf.get(dcConfig + ".resourcemanager.address"),
          conf.getBoolean(dcConfig + ".primary", false)
      );

      centers.add(dc);
    }

    return centers;
  }

  /**
   * 初始化容灾系统
   */
  public void initialize() throws Exception {
    // 初始化复制管理器
    replicationManager.initialize();

    // 初始化备份管理器
    backupManager.initialize();

    // 初始化故障转移编排器
    failoverOrchestrator.initialize();

    // 启动健康监控
    startHealthMonitoring();

    LOG.info("Disaster recovery system initialized");
  }

  /**
   * 执行数据中心故障转移
   */
  public void performDataCenterFailover(String failedDC, String targetDC) throws Exception {
    LOG.info("Performing datacenter failover from {} to {}", failedDC, targetDC);

    DataCenter failed = getDataCenter(failedDC);
    DataCenter target = getDataCenter(targetDC);

    if (failed == null || target == null) {
      throw new IllegalArgumentException("Invalid datacenter names");
    }

    try {
      // 1. 停止故障数据中心的服务
      stopDataCenterServices(failed);

      // 2. 激活目标数据中心
      activateDataCenter(target);

      // 3. 重新路由客户端连接
      rerouteClientConnections(failed, target);

      // 4. 同步数据状态
      synchronizeDataState(failed, target);

      // 5. 更新配置
      updateFailoverConfiguration(failed, target);

      LOG.info("Datacenter failover completed successfully");

    } catch (Exception e) {
      LOG.error("Datacenter failover failed", e);

      // 尝试回滚
      try {
        rollbackFailover(failed, target);
      } catch (Exception rollbackException) {
        LOG.error("Failover rollback failed", rollbackException);
      }

      throw e;
    }
  }

  private void stopDataCenterServices(DataCenter dc) throws Exception {
    LOG.info("Stopping services in datacenter: {}", dc.getName());

    // 停止ResourceManager
    stopResourceManager(dc);

    // 停止NameNode
    stopNameNode(dc);

    // 停止DataNode和NodeManager
    stopWorkerNodes(dc);
  }

  private void activateDataCenter(DataCenter dc) throws Exception {
    LOG.info("Activating datacenter: {}", dc.getName());

    // 启动NameNode
    startNameNode(dc);

    // 启动ResourceManager
    startResourceManager(dc);

    // 启动DataNode和NodeManager
    startWorkerNodes(dc);

    // 等待服务就绪
    waitForServicesReady(dc);
  }

  private void rerouteClientConnections(DataCenter from, DataCenter to) throws Exception {
    LOG.info("Rerouting client connections from {} to {}", from.getName(), to.getName());

    // 更新DNS记录
    updateDNSRecords(from, to);

    // 更新负载均衡器配置
    updateLoadBalancerConfig(from, to);

    // 通知客户端配置更新
    notifyClientConfigUpdate(from, to);
  }

  private void synchronizeDataState(DataCenter from, DataCenter to) throws Exception {
    LOG.info("Synchronizing data state from {} to {}", from.getName(), to.getName());

    // 同步HDFS数据
    synchronizeHDFSData(from, to);

    // 同步YARN应用状态
    synchronizeYARNState(from, to);

    // 验证数据一致性
    validateDataConsistency(from, to);
  }

  private void synchronizeHDFSData(DataCenter from, DataCenter to) throws Exception {
    // 使用DistCp进行数据同步
    String[] distcpArgs = {
        "-update",
        "-delete",
        "-preserveRawXattrs",
        "hdfs://" + from.getNameNodeAddress() + "/",
        "hdfs://" + to.getNameNodeAddress() + "/"
    };

    DistCp distcp = new DistCp(conf, distcpArgs);
    Job job = distcp.execute();

    if (!job.waitForCompletion(true)) {
      throw new Exception("HDFS data synchronization failed");
    }

    LOG.info("HDFS data synchronization completed");
  }

  private void synchronizeYARNState(DataCenter from, DataCenter to) throws Exception {
    // 同步YARN应用状态
    YarnClient fromClient = createYarnClient(from);
    YarnClient toClient = createYarnClient(to);

    try {
      List<ApplicationReport> applications = fromClient.getApplications();

      for (ApplicationReport app : applications) {
        if (app.getYarnApplicationState() == YarnApplicationState.RUNNING ||
            app.getYarnApplicationState() == YarnApplicationState.SUBMITTED) {

          // 在目标数据中心重新提交应用
          resubmitApplication(app, toClient);
        }
      }

    } finally {
      fromClient.close();
      toClient.close();
    }

    LOG.info("YARN state synchronization completed");
  }

  private void resubmitApplication(ApplicationReport app, YarnClient targetClient) throws Exception {
    // 获取应用提交上下文
    ApplicationSubmissionContext submissionContext = reconstructSubmissionContext(app);

    // 在目标集群重新提交
    ApplicationId newAppId = targetClient.submitApplication(submissionContext);

    LOG.info("Resubmitted application {} as {} in target datacenter",
             app.getApplicationId(), newAppId);
  }

  private ApplicationSubmissionContext reconstructSubmissionContext(ApplicationReport app) {
    // 重构应用提交上下文
    ApplicationSubmissionContext context = ApplicationSubmissionContext.newInstance(
        app.getApplicationId(),
        app.getName(),
        app.getQueue(),
        Priority.newInstance(app.getPriority()),
        null, // AM Container Spec需要重新构建
        app.getApplicationType() != null,
        app.getApplicationType() != null,
        1, // maxAppAttempts
        Resource.newInstance(1024, 1), // AM Resource
        app.getApplicationType(),
        false, // keepContainersAcrossApplicationAttempts
        app.getApplicationTags()
    );

    return context;
  }

  private void validateDataConsistency(DataCenter from, DataCenter to) throws Exception {
    LOG.info("Validating data consistency between {} and {}", from.getName(), to.getName());

    // 验证HDFS数据一致性
    validateHDFSConsistency(from, to);

    // 验证元数据一致性
    validateMetadataConsistency(from, to);

    LOG.info("Data consistency validation completed");
  }

  private void validateHDFSConsistency(DataCenter from, DataCenter to) throws Exception {
    FileSystem fromFS = FileSystem.get(URI.create("hdfs://" + from.getNameNodeAddress()), conf);
    FileSystem toFS = FileSystem.get(URI.create("hdfs://" + to.getNameNodeAddress()), conf);

    try {
      // 比较根目录的文件列表
      FileStatus[] fromFiles = fromFS.listStatus(new Path("/"));
      FileStatus[] toFiles = toFS.listStatus(new Path("/"));

      if (fromFiles.length != toFiles.length) {
        throw new Exception("File count mismatch in root directory");
      }

      // 递归验证文件一致性
      for (FileStatus fromFile : fromFiles) {
        validateFileConsistency(fromFS, toFS, fromFile.getPath());
      }

    } finally {
      fromFS.close();
      toFS.close();
    }
  }

  private void validateFileConsistency(FileSystem fromFS, FileSystem toFS, Path path)
      throws Exception {

    FileStatus fromStatus = fromFS.getFileStatus(path);
    FileStatus toStatus = toFS.getFileStatus(path);

    // 比较文件大小
    if (fromStatus.getLen() != toStatus.getLen()) {
      throw new Exception("File size mismatch for " + path);
    }

    // 比较修改时间
    if (Math.abs(fromStatus.getModificationTime() - toStatus.getModificationTime()) > 1000) {
      LOG.warn("Modification time mismatch for {}: {} vs {}",
               path, fromStatus.getModificationTime(), toStatus.getModificationTime());
    }
  }

  private void validateMetadataConsistency(DataCenter from, DataCenter to) throws Exception {
    // 验证NameNode元数据一致性
    // 这里可以比较fsimage和editlog的内容
    LOG.info("Metadata consistency validation completed");
  }

  private void updateFailoverConfiguration(DataCenter from, DataCenter to) throws Exception {
    // 更新Hadoop配置文件
    Configuration newConf = new Configuration(conf);

    // 更新NameNode地址
    newConf.set("fs.defaultFS", "hdfs://" + to.getNameNodeAddress());

    // 更新ResourceManager地址
    newConf.set("yarn.resourcemanager.address", to.getResourceManagerAddress());

    // 保存配置
    saveConfiguration(newConf);

    LOG.info("Failover configuration updated");
  }

  private void rollbackFailover(DataCenter from, DataCenter to) throws Exception {
    LOG.info("Rolling back failover from {} to {}", from.getName(), to.getName());

    // 停止目标数据中心
    stopDataCenterServices(to);

    // 重新启动原数据中心
    activateDataCenter(from);

    // 恢复原始配置
    restoreOriginalConfiguration();
  }

  private void startHealthMonitoring() {
    ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(2);

    // 定期检查数据中心健康状态
    scheduler.scheduleAtFixedRate(this::checkDataCenterHealth, 0, 30, TimeUnit.SECONDS);

    // 定期检查数据同步状态
    scheduler.scheduleAtFixedRate(this::checkDataSynchronization, 0, 60, TimeUnit.SECONDS);
  }

  private void checkDataCenterHealth() {
    for (DataCenter dc : dataCenters) {
      try {
        boolean healthy = isDataCenterHealthy(dc);

        if (!healthy && dc.isPrimary()) {
          LOG.error("Primary datacenter {} is unhealthy, considering failover", dc.getName());

          // 触发自动故障转移
          DataCenter backup = findBackupDataCenter();
          if (backup != null) {
            performDataCenterFailover(dc.getName(), backup.getName());
          }
        }

      } catch (Exception e) {
        LOG.error("Failed to check health for datacenter: " + dc.getName(), e);
      }
    }
  }

  private boolean isDataCenterHealthy(DataCenter dc) {
    try {
      // 检查NameNode健康状态
      if (!isNameNodeHealthy(dc)) {
        return false;
      }

      // 检查ResourceManager健康状态
      if (!isResourceManagerHealthy(dc)) {
        return false;
      }

      // 检查网络连通性
      if (!isNetworkHealthy(dc)) {
        return false;
      }

      return true;

    } catch (Exception e) {
      LOG.error("Health check failed for datacenter: " + dc.getName(), e);
      return false;
    }
  }

  private boolean isNameNodeHealthy(DataCenter dc) {
    try {
      FileSystem fs = FileSystem.get(URI.create("hdfs://" + dc.getNameNodeAddress()), conf);
      fs.getStatus(); // 简单的健康检查
      fs.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isResourceManagerHealthy(DataCenter dc) {
    try {
      YarnClient client = createYarnClient(dc);
      client.getApplications(); // 简单的健康检查
      client.close();
      return true;
    } catch (Exception e) {
      return false;
    }
  }

  private boolean isNetworkHealthy(DataCenter dc) {
    try {
      InetAddress address = InetAddress.getByName(dc.getNameNodeAddress().split(":")[0]);
      return address.isReachable(5000);
    } catch (Exception e) {
      return false;
    }
  }

  private void checkDataSynchronization() {
    // 检查数据中心之间的数据同步状态
    for (int i = 0; i < dataCenters.size(); i++) {
      for (int j = i + 1; j < dataCenters.size(); j++) {
        DataCenter dc1 = dataCenters.get(i);
        DataCenter dc2 = dataCenters.get(j);

        try {
          boolean inSync = areDataCentersInSync(dc1, dc2);

          if (!inSync) {
            LOG.warn("Datacenters {} and {} are out of sync", dc1.getName(), dc2.getName());

            // 触发数据同步
            triggerDataSynchronization(dc1, dc2);
          }

        } catch (Exception e) {
          LOG.error("Failed to check sync status between {} and {}",
                   dc1.getName(), dc2.getName(), e);
        }
      }
    }
  }

  private boolean areDataCentersInSync(DataCenter dc1, DataCenter dc2) throws Exception {
    // 比较数据中心的数据状态
    // 这里简化为比较文件系统的总大小

    FileSystem fs1 = FileSystem.get(URI.create("hdfs://" + dc1.getNameNodeAddress()), conf);
    FileSystem fs2 = FileSystem.get(URI.create("hdfs://" + dc2.getNameNodeAddress()), conf);

    try {
      FsStatus status1 = fs1.getStatus();
      FsStatus status2 = fs2.getStatus();

      long diff = Math.abs(status1.getUsed() - status2.getUsed());
      long threshold = status1.getUsed() * 5 / 100; // 5%的差异阈值

      return diff <= threshold;

    } finally {
      fs1.close();
      fs2.close();
    }
  }

  private void triggerDataSynchronization(DataCenter source, DataCenter target) {
    // 触发数据同步任务
    replicationManager.scheduleReplication(source, target);
  }

  private DataCenter findBackupDataCenter() {
    return dataCenters.stream()
        .filter(dc -> !dc.isPrimary())
        .findFirst()
        .orElse(null);
  }

  private DataCenter getDataCenter(String name) {
    return dataCenters.stream()
        .filter(dc -> dc.getName().equals(name))
        .findFirst()
        .orElse(null);
  }

  // 其他辅助方法的实现...
  private void stopResourceManager(DataCenter dc) throws Exception { /* 实现 */ }
  private void stopNameNode(DataCenter dc) throws Exception { /* 实现 */ }
  private void stopWorkerNodes(DataCenter dc) throws Exception { /* 实现 */ }
  private void startNameNode(DataCenter dc) throws Exception { /* 实现 */ }
  private void startResourceManager(DataCenter dc) throws Exception { /* 实现 */ }
  private void startWorkerNodes(DataCenter dc) throws Exception { /* 实现 */ }
  private void waitForServicesReady(DataCenter dc) throws Exception { /* 实现 */ }
  private void updateDNSRecords(DataCenter from, DataCenter to) throws Exception { /* 实现 */ }
  private void updateLoadBalancerConfig(DataCenter from, DataCenter to) throws Exception { /* 实现 */ }
  private void notifyClientConfigUpdate(DataCenter from, DataCenter to) throws Exception { /* 实现 */ }
  private YarnClient createYarnClient(DataCenter dc) { /* 实现 */ return null; }
  private void saveConfiguration(Configuration conf) throws Exception { /* 实现 */ }
  private void restoreOriginalConfiguration() throws Exception { /* 实现 */ }

  /**
   * 数据中心信息
   */
  public static class DataCenter {
    private final String name;
    private final String location;
    private final String nameNodeAddress;
    private final String resourceManagerAddress;
    private final boolean primary;

    public DataCenter(String name, String location, String nameNodeAddress,
                     String resourceManagerAddress, boolean primary) {
      this.name = name;
      this.location = location;
      this.nameNodeAddress = nameNodeAddress;
      this.resourceManagerAddress = resourceManagerAddress;
      this.primary = primary;
    }

    // Getters
    public String getName() { return name; }
    public String getLocation() { return location; }
    public String getNameNodeAddress() { return nameNodeAddress; }
    public String getResourceManagerAddress() { return resourceManagerAddress; }
    public boolean isPrimary() { return primary; }
  }
}
```

## 16.5 本章小结

理解这些高可用技术的实现原理，对于在生产环境中构建可靠的Hadoop集群具有重要意义。随着业务对可用性要求的不断提高，高可用架构将成为Hadoop部署的标准配置。
