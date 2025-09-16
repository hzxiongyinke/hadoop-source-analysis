# 第15章：Hadoop监控与故障诊断

## 15.1 引言

在大规模分布式环境中，Hadoop集群的监控与故障诊断是确保系统稳定运行的关键环节。一个完善的监控体系不仅能够实时反映集群的健康状态，还能在问题发生前提供预警，帮助运维人员及时采取措施避免系统故障。同时，当故障不可避免地发生时，有效的诊断工具和方法能够快速定位问题根源，缩短故障恢复时间。

Hadoop监控涉及多个层面：从底层的硬件资源监控，到中间层的服务状态监控，再到上层的应用性能监控。每个层面都有其特定的监控指标和诊断方法。硬件层面需要关注CPU、内存、磁盘、网络等资源的使用情况；服务层面需要监控NameNode、DataNode、ResourceManager、NodeManager等核心组件的运行状态；应用层面则需要跟踪作业执行情况、数据处理性能等指标。

故障诊断是监控的延伸和深化。当监控系统发现异常时，需要通过日志分析、性能剖析、状态检查等手段，快速确定故障的性质、范围和影响程度。Hadoop提供了丰富的诊断工具和接口，包括Web UI、JMX接口、命令行工具、日志系统等，这些工具相互配合，形成了完整的故障诊断体系。

本章将系统性地介绍Hadoop监控与故障诊断的理论基础、技术实现和实践方法，帮助读者建立完整的运维知识体系，提升在生产环境中处理复杂问题的能力。

## 15.2 监控指标体系

### 15.2.1 核心监控指标

Hadoop监控指标体系的设计和实现：

```java
/**
 * Hadoop监控指标管理器
 */
public class HadoopMetricsManager {
  
  private final MetricsRegistry registry;
  private final Map<String, MetricsCollector> collectors;
  private final ScheduledExecutorService scheduler;
  
  public HadoopMetricsManager() {
    this.registry = new MetricsRegistry();
    this.collectors = new ConcurrentHashMap<>();
    this.scheduler = Executors.newScheduledThreadPool(8);
    
    // 注册核心指标收集器
    registerCollector("cluster", new ClusterMetricsCollector());
    registerCollector("hdfs", new HDFSMetricsCollector());
    registerCollector("yarn", new YARNMetricsCollector());
    registerCollector("mapreduce", new MapReduceMetricsCollector());
    registerCollector("system", new SystemMetricsCollector());
  }
  
  public void registerCollector(String name, MetricsCollector collector) {
    collectors.put(name, collector);
    
    // 启动定期收集
    scheduler.scheduleAtFixedRate(
        () -> collectMetrics(name, collector),
        0, collector.getCollectionInterval(), TimeUnit.SECONDS
    );
  }
  
  private void collectMetrics(String collectorName, MetricsCollector collector) {
    try {
      Map<String, Object> metrics = collector.collect();
      
      for (Map.Entry<String, Object> entry : metrics.entrySet()) {
        String metricName = collectorName + "." + entry.getKey();
        registry.recordMetric(metricName, entry.getValue(), System.currentTimeMillis());
      }
      
    } catch (Exception e) {
      LOG.error("Failed to collect metrics from " + collectorName, e);
    }
  }
  
  /**
   * 集群级别指标收集器
   */
  public static class ClusterMetricsCollector implements MetricsCollector {
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // 集群整体状态
        ClusterStatus clusterStatus = getClusterStatus();
        
        metrics.put("nodes.total", clusterStatus.getTotalNodes());
        metrics.put("nodes.active", clusterStatus.getActiveNodes());
        metrics.put("nodes.inactive", clusterStatus.getInactiveNodes());
        metrics.put("nodes.decommissioned", clusterStatus.getDecommissionedNodes());
        
        // 集群资源统计
        ClusterResourceInfo resourceInfo = getClusterResourceInfo();
        
        metrics.put("capacity.total", resourceInfo.getTotalCapacity());
        metrics.put("capacity.used", resourceInfo.getUsedCapacity());
        metrics.put("capacity.available", resourceInfo.getAvailableCapacity());
        metrics.put("capacity.utilization", resourceInfo.getCapacityUtilization());
        
        // 集群健康度
        ClusterHealthInfo healthInfo = calculateClusterHealth();
        
        metrics.put("health.score", healthInfo.getHealthScore());
        metrics.put("health.status", healthInfo.getHealthStatus().name());
        metrics.put("health.issues", healthInfo.getIssueCount());
        
      } catch (Exception e) {
        LOG.error("Failed to collect cluster metrics", e);
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 30; // 30秒
    }
    
    private ClusterStatus getClusterStatus() {
      // 实现集群状态获取逻辑
      return new ClusterStatus();
    }
    
    private ClusterResourceInfo getClusterResourceInfo() {
      // 实现集群资源信息获取逻辑
      return new ClusterResourceInfo();
    }
    
    private ClusterHealthInfo calculateClusterHealth() {
      // 实现集群健康度计算逻辑
      return new ClusterHealthInfo();
    }
  }
  
  /**
   * HDFS指标收集器
   */
  public static class HDFSMetricsCollector implements MetricsCollector {
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // NameNode指标
        NameNodeMetrics nnMetrics = getNameNodeMetrics();
        
        metrics.put("namenode.files_total", nnMetrics.getFilesTotal());
        metrics.put("namenode.blocks_total", nnMetrics.getBlocksTotal());
        metrics.put("namenode.missing_blocks", nnMetrics.getMissingBlocks());
        metrics.put("namenode.corrupt_blocks", nnMetrics.getCorruptBlocks());
        metrics.put("namenode.under_replicated_blocks", nnMetrics.getUnderReplicatedBlocks());
        
        // NameNode RPC性能
        metrics.put("namenode.rpc.queue_time_avg", nnMetrics.getRpcQueueTimeAvg());
        metrics.put("namenode.rpc.processing_time_avg", nnMetrics.getRpcProcessingTimeAvg());
        metrics.put("namenode.rpc.queue_length", nnMetrics.getRpcQueueLength());
        
        // DataNode指标
        DataNodeMetrics dnMetrics = getDataNodeMetrics();
        
        metrics.put("datanode.bytes_written", dnMetrics.getBytesWritten());
        metrics.put("datanode.bytes_read", dnMetrics.getBytesRead());
        metrics.put("datanode.blocks_written", dnMetrics.getBlocksWritten());
        metrics.put("datanode.blocks_read", dnMetrics.getBlocksRead());
        metrics.put("datanode.blocks_replicated", dnMetrics.getBlocksReplicated());
        
        // HDFS容量统计
        HDFSCapacityInfo capacityInfo = getHDFSCapacityInfo();
        
        metrics.put("capacity.total", capacityInfo.getTotalCapacity());
        metrics.put("capacity.used", capacityInfo.getUsedCapacity());
        metrics.put("capacity.remaining", capacityInfo.getRemainingCapacity());
        metrics.put("capacity.dfs_used_percent", capacityInfo.getDfsUsedPercent());
        
      } catch (Exception e) {
        LOG.error("Failed to collect HDFS metrics", e);
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 15; // 15秒
    }
    
    private NameNodeMetrics getNameNodeMetrics() {
      // 实现NameNode指标获取逻辑
      return new NameNodeMetrics();
    }
    
    private DataNodeMetrics getDataNodeMetrics() {
      // 实现DataNode指标获取逻辑
      return new DataNodeMetrics();
    }
    
    private HDFSCapacityInfo getHDFSCapacityInfo() {
      // 实现HDFS容量信息获取逻辑
      return new HDFSCapacityInfo();
    }
  }
  
  /**
   * YARN指标收集器
   */
  public static class YARNMetricsCollector implements MetricsCollector {
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // ResourceManager指标
        ResourceManagerMetrics rmMetrics = getResourceManagerMetrics();
        
        metrics.put("resourcemanager.apps_submitted", rmMetrics.getAppsSubmitted());
        metrics.put("resourcemanager.apps_completed", rmMetrics.getAppsCompleted());
        metrics.put("resourcemanager.apps_pending", rmMetrics.getAppsPending());
        metrics.put("resourcemanager.apps_running", rmMetrics.getAppsRunning());
        metrics.put("resourcemanager.apps_failed", rmMetrics.getAppsFailed());
        metrics.put("resourcemanager.apps_killed", rmMetrics.getAppsKilled());
        
        // 集群资源指标
        ClusterResourceMetrics resourceMetrics = getClusterResourceMetrics();
        
        metrics.put("cluster.memory.total", resourceMetrics.getTotalMemory());
        metrics.put("cluster.memory.available", resourceMetrics.getAvailableMemory());
        metrics.put("cluster.memory.allocated", resourceMetrics.getAllocatedMemory());
        metrics.put("cluster.memory.utilization", resourceMetrics.getMemoryUtilization());
        
        metrics.put("cluster.vcores.total", resourceMetrics.getTotalVCores());
        metrics.put("cluster.vcores.available", resourceMetrics.getAvailableVCores());
        metrics.put("cluster.vcores.allocated", resourceMetrics.getAllocatedVCores());
        metrics.put("cluster.vcores.utilization", resourceMetrics.getVCoresUtilization());
        
        // NodeManager指标
        NodeManagerMetrics nmMetrics = getNodeManagerMetrics();
        
        metrics.put("nodemanager.containers_launched", nmMetrics.getContainersLaunched());
        metrics.put("nodemanager.containers_completed", nmMetrics.getContainersCompleted());
        metrics.put("nodemanager.containers_failed", nmMetrics.getContainersFailed());
        metrics.put("nodemanager.containers_killed", nmMetrics.getContainersKilled());
        metrics.put("nodemanager.containers_running", nmMetrics.getContainersRunning());
        
      } catch (Exception e) {
        LOG.error("Failed to collect YARN metrics", e);
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 20; // 20秒
    }
    
    private ResourceManagerMetrics getResourceManagerMetrics() {
      // 实现ResourceManager指标获取逻辑
      return new ResourceManagerMetrics();
    }
    
    private ClusterResourceMetrics getClusterResourceMetrics() {
      // 实现集群资源指标获取逻辑
      return new ClusterResourceMetrics();
    }
    
    private NodeManagerMetrics getNodeManagerMetrics() {
      // 实现NodeManager指标获取逻辑
      return new NodeManagerMetrics();
    }
  }
  
  /**
   * MapReduce指标收集器
   */
  public static class MapReduceMetricsCollector implements MetricsCollector {
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // 作业统计
        JobStatistics jobStats = getJobStatistics();
        
        metrics.put("jobs.submitted", jobStats.getJobsSubmitted());
        metrics.put("jobs.completed", jobStats.getJobsCompleted());
        metrics.put("jobs.running", jobStats.getJobsRunning());
        metrics.put("jobs.failed", jobStats.getJobsFailed());
        metrics.put("jobs.killed", jobStats.getJobsKilled());
        
        // 任务统计
        TaskStatistics taskStats = getTaskStatistics();
        
        metrics.put("tasks.map.total", taskStats.getMapTasksTotal());
        metrics.put("tasks.map.completed", taskStats.getMapTasksCompleted());
        metrics.put("tasks.map.running", taskStats.getMapTasksRunning());
        metrics.put("tasks.map.failed", taskStats.getMapTasksFailed());
        
        metrics.put("tasks.reduce.total", taskStats.getReduceTasksTotal());
        metrics.put("tasks.reduce.completed", taskStats.getReduceTasksCompleted());
        metrics.put("tasks.reduce.running", taskStats.getReduceTasksRunning());
        metrics.put("tasks.reduce.failed", taskStats.getReduceTasksFailed());
        
        // 性能指标
        PerformanceMetrics perfMetrics = getPerformanceMetrics();
        
        metrics.put("performance.avg_map_time", perfMetrics.getAvgMapTime());
        metrics.put("performance.avg_reduce_time", perfMetrics.getAvgReduceTime());
        metrics.put("performance.avg_shuffle_time", perfMetrics.getAvgShuffleTime());
        metrics.put("performance.data_locality_percent", perfMetrics.getDataLocalityPercent());
        
      } catch (Exception e) {
        LOG.error("Failed to collect MapReduce metrics", e);
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 25; // 25秒
    }
    
    private JobStatistics getJobStatistics() {
      // 实现作业统计获取逻辑
      return new JobStatistics();
    }
    
    private TaskStatistics getTaskStatistics() {
      // 实现任务统计获取逻辑
      return new TaskStatistics();
    }
    
    private PerformanceMetrics getPerformanceMetrics() {
      // 实现性能指标获取逻辑
      return new PerformanceMetrics();
    }
  }
  
  /**
   * 系统级指标收集器
   */
  public static class SystemMetricsCollector implements MetricsCollector {
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // CPU指标
        CPUMetrics cpuMetrics = getCPUMetrics();
        
        metrics.put("cpu.usage_percent", cpuMetrics.getUsagePercent());
        metrics.put("cpu.load_average", cpuMetrics.getLoadAverage());
        metrics.put("cpu.cores", cpuMetrics.getCores());
        
        // 内存指标
        MemoryMetrics memoryMetrics = getMemoryMetrics();
        
        metrics.put("memory.total", memoryMetrics.getTotal());
        metrics.put("memory.used", memoryMetrics.getUsed());
        metrics.put("memory.free", memoryMetrics.getFree());
        metrics.put("memory.usage_percent", memoryMetrics.getUsagePercent());
        
        // 磁盘指标
        DiskMetrics diskMetrics = getDiskMetrics();
        
        metrics.put("disk.total", diskMetrics.getTotal());
        metrics.put("disk.used", diskMetrics.getUsed());
        metrics.put("disk.free", diskMetrics.getFree());
        metrics.put("disk.usage_percent", diskMetrics.getUsagePercent());
        metrics.put("disk.read_ops", diskMetrics.getReadOps());
        metrics.put("disk.write_ops", diskMetrics.getWriteOps());
        
        // 网络指标
        NetworkMetrics networkMetrics = getNetworkMetrics();
        
        metrics.put("network.bytes_in", networkMetrics.getBytesIn());
        metrics.put("network.bytes_out", networkMetrics.getBytesOut());
        metrics.put("network.packets_in", networkMetrics.getPacketsIn());
        metrics.put("network.packets_out", networkMetrics.getPacketsOut());
        
      } catch (Exception e) {
        LOG.error("Failed to collect system metrics", e);
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 10; // 10秒
    }
    
    private CPUMetrics getCPUMetrics() {
      // 实现CPU指标获取逻辑
      return new CPUMetrics();
    }
    
    private MemoryMetrics getMemoryMetrics() {
      // 实现内存指标获取逻辑
      return new MemoryMetrics();
    }
    
    private DiskMetrics getDiskMetrics() {
      // 实现磁盘指标获取逻辑
      return new DiskMetrics();
    }
    
    private NetworkMetrics getNetworkMetrics() {
      // 实现网络指标获取逻辑
      return new NetworkMetrics();
    }
  }
  
  /**
   * 指标收集器接口
   */
  public interface MetricsCollector {
    Map<String, Object> collect();
    int getCollectionInterval();
  }
}
```

### 15.2.2 JMX监控接口

Hadoop通过JMX提供了丰富的监控接口：

```java
/**
 * JMX监控管理器
 */
public class JMXMonitoringManager {
  
  private final MBeanServer mBeanServer;
  private final Map<String, ObjectName> registeredMBeans;
  
  public JMXMonitoringManager() {
    this.mBeanServer = ManagementFactory.getPlatformMBeanServer();
    this.registeredMBeans = new ConcurrentHashMap<>();
  }
  
  /**
   * 注册MBean
   */
  public void registerMBean(String name, Object mbean, String domain) {
    try {
      ObjectName objectName = new ObjectName(domain + ":name=" + name);
      
      if (mBeanServer.isRegistered(objectName)) {
        mBeanServer.unregisterMBean(objectName);
      }
      
      mBeanServer.registerMBean(mbean, objectName);
      registeredMBeans.put(name, objectName);
      
      LOG.info("Registered MBean: {}", objectName);
      
    } catch (Exception e) {
      LOG.error("Failed to register MBean: " + name, e);
    }
  }
  
  /**
   * 获取MBean属性值
   */
  public Object getMBeanAttribute(String mbeanName, String attributeName) {
    try {
      ObjectName objectName = registeredMBeans.get(mbeanName);
      if (objectName != null) {
        return mBeanServer.getAttribute(objectName, attributeName);
      }
    } catch (Exception e) {
      LOG.error("Failed to get MBean attribute: " + mbeanName + "." + attributeName, e);
    }
    return null;
  }
  
  /**
   * 调用MBean方法
   */
  public Object invokeMBeanMethod(String mbeanName, String methodName, 
                                 Object[] params, String[] signature) {
    try {
      ObjectName objectName = registeredMBeans.get(mbeanName);
      if (objectName != null) {
        return mBeanServer.invoke(objectName, methodName, params, signature);
      }
    } catch (Exception e) {
      LOG.error("Failed to invoke MBean method: " + mbeanName + "." + methodName, e);
    }
    return null;
  }
  
  /**
   * NameNode JMX Bean
   */
  public static class NameNodeJMXBean implements NameNodeJMXBeanMXBean {
    
    private final FSNamesystem fsNamesystem;
    
    public NameNodeJMXBean(FSNamesystem fsNamesystem) {
      this.fsNamesystem = fsNamesystem;
    }
    
    @Override
    public long getFilesTotal() {
      return fsNamesystem.getFilesTotal();
    }
    
    @Override
    public long getBlocksTotal() {
      return fsNamesystem.getBlocksTotal();
    }
    
    @Override
    public long getMissingBlocksCount() {
      return fsNamesystem.getMissingBlocksCount();
    }
    
    @Override
    public long getCorruptReplicaBlocks() {
      return fsNamesystem.getCorruptReplicaBlocks();
    }
    
    @Override
    public long getUnderReplicatedBlocks() {
      return fsNamesystem.getUnderReplicatedBlocks();
    }
    
    @Override
    public long getPendingReplicationBlocks() {
      return fsNamesystem.getPendingReplicationBlocks();
    }
    
    @Override
    public long getScheduledReplicationBlocks() {
      return fsNamesystem.getScheduledReplicationBlocks();
    }
    
    @Override
    public String getClusterId() {
      return fsNamesystem.getClusterId();
    }
    
    @Override
    public String getBlockPoolId() {
      return fsNamesystem.getBlockPoolId();
    }
    
    @Override
    public long getCapacityTotal() {
      return fsNamesystem.getCapacityTotal();
    }
    
    @Override
    public long getCapacityUsed() {
      return fsNamesystem.getCapacityUsed();
    }
    
    @Override
    public long getCapacityRemaining() {
      return fsNamesystem.getCapacityRemaining();
    }
    
    @Override
    public float getCapacityUsedPercent() {
      return fsNamesystem.getCapacityUsedPercent();
    }
    
    @Override
    public float getCapacityRemainingPercent() {
      return fsNamesystem.getCapacityRemainingPercent();
    }
    
    @Override
    public int getNumLiveDataNodes() {
      return fsNamesystem.getNumLiveDataNodes();
    }
    
    @Override
    public int getNumDeadDataNodes() {
      return fsNamesystem.getNumDeadDataNodes();
    }
    
    @Override
    public int getNumDecomLiveDataNodes() {
      return fsNamesystem.getNumDecomLiveDataNodes();
    }
    
    @Override
    public int getNumDecomDeadDataNodes() {
      return fsNamesystem.getNumDecomDeadDataNodes();
    }
    
    @Override
    public int getNumDecommissioningDataNodes() {
      return fsNamesystem.getNumDecommissioningDataNodes();
    }
  }
  
  /**
   * NameNode JMX Bean接口
   */
  public interface NameNodeJMXBeanMXBean {
    long getFilesTotal();
    long getBlocksTotal();
    long getMissingBlocksCount();
    long getCorruptReplicaBlocks();
    long getUnderReplicatedBlocks();
    long getPendingReplicationBlocks();
    long getScheduledReplicationBlocks();
    String getClusterId();
    String getBlockPoolId();
    long getCapacityTotal();
    long getCapacityUsed();
    long getCapacityRemaining();
    float getCapacityUsedPercent();
    float getCapacityRemainingPercent();
    int getNumLiveDataNodes();
    int getNumDeadDataNodes();
    int getNumDecomLiveDataNodes();
    int getNumDecomDeadDataNodes();
    int getNumDecommissioningDataNodes();
  }
}
```

## 15.3 本章小结

本章深入分析了Hadoop监控与故障诊断的理论基础和技术实现。一个完善的监控体系是确保Hadoop集群稳定运行的重要保障。

**核心要点**：

**监控指标体系**：建立全面的监控指标体系，覆盖集群、服务、应用等各个层面。

**JMX接口**：通过JMX提供标准化的监控接口，便于集成第三方监控工具。

**实时监控**：实现实时的指标收集和状态监控，及时发现异常情况。

**指标分析**：对收集的监控数据进行分析，识别性能瓶颈和潜在问题。

**告警机制**：建立有效的告警机制，在问题发生时及时通知相关人员。

## 15.3 日志分析与故障诊断

### 15.3.1 日志收集与分析

Hadoop日志分析是故障诊断的重要手段：

```java
/**
 * Hadoop日志分析器
 */
public class HadoopLogAnalyzer {

  private final Map<String, LogParser> logParsers;
  private final LogAggregator logAggregator;
  private final AlertManager alertManager;

  public HadoopLogAnalyzer() {
    this.logParsers = new HashMap<>();
    this.logAggregator = new LogAggregator();
    this.alertManager = new AlertManager();

    // 注册各种日志解析器
    registerParser("namenode", new NameNodeLogParser());
    registerParser("datanode", new DataNodeLogParser());
    registerParser("resourcemanager", new ResourceManagerLogParser());
    registerParser("nodemanager", new NodeManagerLogParser());
    registerParser("application", new ApplicationLogParser());
  }

  public void registerParser(String logType, LogParser parser) {
    logParsers.put(logType, parser);
  }

  /**
   * 分析日志文件
   */
  public LogAnalysisResult analyzeLogFile(String logType, Path logFile) {
    LogParser parser = logParsers.get(logType);
    if (parser == null) {
      throw new IllegalArgumentException("Unknown log type: " + logType);
    }

    try {
      List<LogEntry> entries = parser.parseLogFile(logFile);
      return analyzeLogEntries(entries);
    } catch (IOException e) {
      LOG.error("Failed to analyze log file: " + logFile, e);
      return new LogAnalysisResult();
    }
  }

  private LogAnalysisResult analyzeLogEntries(List<LogEntry> entries) {
    LogAnalysisResult result = new LogAnalysisResult();

    // 统计各级别日志数量
    Map<LogLevel, Integer> levelCounts = new HashMap<>();

    // 错误模式检测
    List<ErrorPattern> errorPatterns = new ArrayList<>();

    // 性能问题检测
    List<PerformanceIssue> performanceIssues = new ArrayList<>();

    for (LogEntry entry : entries) {
      // 统计日志级别
      levelCounts.merge(entry.getLevel(), 1, Integer::sum);

      // 检测错误模式
      ErrorPattern errorPattern = detectErrorPattern(entry);
      if (errorPattern != null) {
        errorPatterns.add(errorPattern);
      }

      // 检测性能问题
      PerformanceIssue perfIssue = detectPerformanceIssue(entry);
      if (perfIssue != null) {
        performanceIssues.add(perfIssue);
      }
    }

    result.setLevelCounts(levelCounts);
    result.setErrorPatterns(errorPatterns);
    result.setPerformanceIssues(performanceIssues);

    // 生成告警
    generateAlerts(result);

    return result;
  }

  private ErrorPattern detectErrorPattern(LogEntry entry) {
    if (entry.getLevel() == LogLevel.ERROR || entry.getLevel() == LogLevel.FATAL) {
      String message = entry.getMessage();

      // 检测常见错误模式
      if (message.contains("OutOfMemoryError")) {
        return new ErrorPattern(ErrorType.OUT_OF_MEMORY, message, entry.getTimestamp());
      } else if (message.contains("Connection refused")) {
        return new ErrorPattern(ErrorType.CONNECTION_REFUSED, message, entry.getTimestamp());
      } else if (message.contains("No space left on device")) {
        return new ErrorPattern(ErrorType.DISK_FULL, message, entry.getTimestamp());
      } else if (message.contains("Too many open files")) {
        return new ErrorPattern(ErrorType.TOO_MANY_FILES, message, entry.getTimestamp());
      } else if (message.contains("SocketTimeoutException")) {
        return new ErrorPattern(ErrorType.SOCKET_TIMEOUT, message, entry.getTimestamp());
      }
    }

    return null;
  }

  private PerformanceIssue detectPerformanceIssue(LogEntry entry) {
    String message = entry.getMessage();

    // 检测GC问题
    if (message.contains("GC") && message.contains("ms")) {
      Pattern gcPattern = Pattern.compile("GC.*?(\\d+)ms");
      Matcher matcher = gcPattern.matcher(message);
      if (matcher.find()) {
        long gcTime = Long.parseLong(matcher.group(1));
        if (gcTime > 1000) { // GC时间超过1秒
          return new PerformanceIssue(PerformanceIssueType.LONG_GC,
                                    "Long GC detected: " + gcTime + "ms",
                                    entry.getTimestamp());
        }
      }
    }

    // 检测慢查询
    if (message.contains("slow") && message.contains("operation")) {
      return new PerformanceIssue(PerformanceIssueType.SLOW_OPERATION,
                                message, entry.getTimestamp());
    }

    // 检测高延迟
    if (message.contains("latency") || message.contains("delay")) {
      Pattern latencyPattern = Pattern.compile("(\\d+)ms");
      Matcher matcher = latencyPattern.matcher(message);
      if (matcher.find()) {
        long latency = Long.parseLong(matcher.group(1));
        if (latency > 5000) { // 延迟超过5秒
          return new PerformanceIssue(PerformanceIssueType.HIGH_LATENCY,
                                    "High latency detected: " + latency + "ms",
                                    entry.getTimestamp());
        }
      }
    }

    return null;
  }

  private void generateAlerts(LogAnalysisResult result) {
    // 基于分析结果生成告警
    Map<LogLevel, Integer> levelCounts = result.getLevelCounts();

    // 错误日志过多告警
    int errorCount = levelCounts.getOrDefault(LogLevel.ERROR, 0);
    int fatalCount = levelCounts.getOrDefault(LogLevel.FATAL, 0);

    if (errorCount > 100) {
      alertManager.sendAlert(new Alert(AlertLevel.HIGH,
                                     "High error count: " + errorCount + " errors detected"));
    }

    if (fatalCount > 0) {
      alertManager.sendAlert(new Alert(AlertLevel.CRITICAL,
                                     "Fatal errors detected: " + fatalCount + " fatal errors"));
    }

    // 错误模式告警
    for (ErrorPattern pattern : result.getErrorPatterns()) {
      AlertLevel level = getAlertLevelForErrorType(pattern.getType());
      alertManager.sendAlert(new Alert(level,
                                     "Error pattern detected: " + pattern.getType() +
                                     " - " + pattern.getMessage()));
    }

    // 性能问题告警
    for (PerformanceIssue issue : result.getPerformanceIssues()) {
      alertManager.sendAlert(new Alert(AlertLevel.MEDIUM,
                                     "Performance issue detected: " + issue.getType() +
                                     " - " + issue.getDescription()));
    }
  }

  private AlertLevel getAlertLevelForErrorType(ErrorType errorType) {
    switch (errorType) {
      case OUT_OF_MEMORY:
      case DISK_FULL:
        return AlertLevel.CRITICAL;
      case CONNECTION_REFUSED:
      case SOCKET_TIMEOUT:
        return AlertLevel.HIGH;
      case TOO_MANY_FILES:
        return AlertLevel.MEDIUM;
      default:
        return AlertLevel.LOW;
    }
  }

  /**
   * NameNode日志解析器
   */
  public static class NameNodeLogParser implements LogParser {

    private static final Pattern LOG_PATTERN = Pattern.compile(
        "(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2},\\d{3}) (\\w+) (.+?) - (.+)");

    @Override
    public List<LogEntry> parseLogFile(Path logFile) throws IOException {
      List<LogEntry> entries = new ArrayList<>();

      try (BufferedReader reader = Files.newBufferedReader(logFile)) {
        String line;
        while ((line = reader.readLine()) != null) {
          LogEntry entry = parseLogLine(line);
          if (entry != null) {
            entries.add(entry);
          }
        }
      }

      return entries;
    }

    private LogEntry parseLogLine(String line) {
      Matcher matcher = LOG_PATTERN.matcher(line);
      if (matcher.matches()) {
        try {
          String timestampStr = matcher.group(1);
          String levelStr = matcher.group(2);
          String logger = matcher.group(3);
          String message = matcher.group(4);

          SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss,SSS");
          Date timestamp = sdf.parse(timestampStr);
          LogLevel level = LogLevel.valueOf(levelStr);

          return new LogEntry(timestamp.getTime(), level, logger, message);

        } catch (Exception e) {
          LOG.debug("Failed to parse log line: " + line, e);
        }
      }

      return null;
    }
  }

  /**
   * DataNode日志解析器
   */
  public static class DataNodeLogParser implements LogParser {

    @Override
    public List<LogEntry> parseLogFile(Path logFile) throws IOException {
      // 实现DataNode日志解析逻辑
      return new ArrayList<>();
    }
  }

  /**
   * ResourceManager日志解析器
   */
  public static class ResourceManagerLogParser implements LogParser {

    @Override
    public List<LogEntry> parseLogFile(Path logFile) throws IOException {
      // 实现ResourceManager日志解析逻辑
      return new ArrayList<>();
    }
  }

  /**
   * NodeManager日志解析器
   */
  public static class NodeManagerLogParser implements LogParser {

    @Override
    public List<LogEntry> parseLogFile(Path logFile) throws IOException {
      // 实现NodeManager日志解析逻辑
      return new ArrayList<>();
    }
  }

  /**
   * 应用日志解析器
   */
  public static class ApplicationLogParser implements LogParser {

    @Override
    public List<LogEntry> parseLogFile(Path logFile) throws IOException {
      // 实现应用日志解析逻辑
      return new ArrayList<>();
    }
  }

  /**
   * 日志解析器接口
   */
  public interface LogParser {
    List<LogEntry> parseLogFile(Path logFile) throws IOException;
  }

  /**
   * 日志条目
   */
  public static class LogEntry {
    private final long timestamp;
    private final LogLevel level;
    private final String logger;
    private final String message;

    public LogEntry(long timestamp, LogLevel level, String logger, String message) {
      this.timestamp = timestamp;
      this.level = level;
      this.logger = logger;
      this.message = message;
    }

    // Getters
    public long getTimestamp() { return timestamp; }
    public LogLevel getLevel() { return level; }
    public String getLogger() { return logger; }
    public String getMessage() { return message; }
  }

  /**
   * 日志级别
   */
  public enum LogLevel {
    TRACE, DEBUG, INFO, WARN, ERROR, FATAL
  }

  /**
   * 错误模式
   */
  public static class ErrorPattern {
    private final ErrorType type;
    private final String message;
    private final long timestamp;

    public ErrorPattern(ErrorType type, String message, long timestamp) {
      this.type = type;
      this.message = message;
      this.timestamp = timestamp;
    }

    // Getters
    public ErrorType getType() { return type; }
    public String getMessage() { return message; }
    public long getTimestamp() { return timestamp; }
  }

  /**
   * 错误类型
   */
  public enum ErrorType {
    OUT_OF_MEMORY, CONNECTION_REFUSED, DISK_FULL, TOO_MANY_FILES, SOCKET_TIMEOUT
  }

  /**
   * 性能问题
   */
  public static class PerformanceIssue {
    private final PerformanceIssueType type;
    private final String description;
    private final long timestamp;

    public PerformanceIssue(PerformanceIssueType type, String description, long timestamp) {
      this.type = type;
      this.description = description;
      this.timestamp = timestamp;
    }

    // Getters
    public PerformanceIssueType getType() { return type; }
    public String getDescription() { return description; }
    public long getTimestamp() { return timestamp; }
  }

  /**
   * 性能问题类型
   */
  public enum PerformanceIssueType {
    LONG_GC, SLOW_OPERATION, HIGH_LATENCY, HIGH_CPU, HIGH_MEMORY
  }

  /**
   * 日志分析结果
   */
  public static class LogAnalysisResult {
    private Map<LogLevel, Integer> levelCounts = new HashMap<>();
    private List<ErrorPattern> errorPatterns = new ArrayList<>();
    private List<PerformanceIssue> performanceIssues = new ArrayList<>();

    // Getters and setters
    public Map<LogLevel, Integer> getLevelCounts() { return levelCounts; }
    public void setLevelCounts(Map<LogLevel, Integer> levelCounts) { this.levelCounts = levelCounts; }

    public List<ErrorPattern> getErrorPatterns() { return errorPatterns; }
    public void setErrorPatterns(List<ErrorPattern> errorPatterns) { this.errorPatterns = errorPatterns; }

    public List<PerformanceIssue> getPerformanceIssues() { return performanceIssues; }
    public void setPerformanceIssues(List<PerformanceIssue> performanceIssues) { this.performanceIssues = performanceIssues; }
  }
}
```

### 15.3.2 故障诊断工具

Hadoop提供了多种故障诊断工具：

```java
/**
 * Hadoop故障诊断工具集
 */
public class HadoopDiagnosticTools {

  /**
   * 集群健康检查工具
   */
  public static class ClusterHealthChecker {

    private final Configuration conf;
    private final List<HealthCheck> healthChecks;

    public ClusterHealthChecker(Configuration conf) {
      this.conf = conf;
      this.healthChecks = Arrays.asList(
          new NameNodeHealthCheck(),
          new DataNodeHealthCheck(),
          new ResourceManagerHealthCheck(),
          new NodeManagerHealthCheck(),
          new DiskSpaceHealthCheck(),
          new NetworkConnectivityHealthCheck()
      );
    }

    public ClusterHealthReport performHealthCheck() {
      ClusterHealthReport report = new ClusterHealthReport();

      for (HealthCheck check : healthChecks) {
        try {
          HealthCheckResult result = check.performCheck(conf);
          report.addResult(check.getName(), result);

          if (result.getStatus() == HealthStatus.CRITICAL) {
            LOG.error("Critical health issue detected: {} - {}",
                     check.getName(), result.getMessage());
          } else if (result.getStatus() == HealthStatus.WARNING) {
            LOG.warn("Health warning detected: {} - {}",
                    check.getName(), result.getMessage());
          }

        } catch (Exception e) {
          LOG.error("Failed to perform health check: " + check.getName(), e);
          report.addResult(check.getName(),
                          new HealthCheckResult(HealthStatus.UNKNOWN,
                                              "Health check failed: " + e.getMessage()));
        }
      }

      return report;
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
      public HealthCheckResult performCheck(Configuration conf) {
        try {
          // 检查NameNode是否可访问
          FileSystem fs = FileSystem.get(conf);
          fs.getStatus(); // 简单的状态检查

          // 检查安全模式
          if (fs instanceof DistributedFileSystem) {
            DistributedFileSystem dfs = (DistributedFileSystem) fs;
            if (dfs.isInSafeMode()) {
              return new HealthCheckResult(HealthStatus.WARNING,
                                         "NameNode is in safe mode");
            }
          }

          return new HealthCheckResult(HealthStatus.HEALTHY, "NameNode is healthy");

        } catch (IOException e) {
          return new HealthCheckResult(HealthStatus.CRITICAL,
                                     "Cannot connect to NameNode: " + e.getMessage());
        }
      }
    }

    /**
     * DataNode健康检查
     */
    private static class DataNodeHealthCheck implements HealthCheck {

      @Override
      public String getName() {
        return "DataNode Health";
      }

      @Override
      public HealthCheckResult performCheck(Configuration conf) {
        try {
          DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
          DatanodeInfo[] datanodes = dfs.getDataNodeStats();

          int totalNodes = datanodes.length;
          int deadNodes = 0;
          int decommissioningNodes = 0;

          for (DatanodeInfo datanode : datanodes) {
            if (!datanode.isAlive()) {
              deadNodes++;
            }
            if (datanode.isDecommissionInProgress()) {
              decommissioningNodes++;
            }
          }

          if (deadNodes > totalNodes * 0.1) { // 超过10%的节点死亡
            return new HealthCheckResult(HealthStatus.CRITICAL,
                                       String.format("Too many dead DataNodes: %d/%d",
                                                    deadNodes, totalNodes));
          } else if (deadNodes > 0) {
            return new HealthCheckResult(HealthStatus.WARNING,
                                       String.format("Some DataNodes are dead: %d/%d",
                                                    deadNodes, totalNodes));
          }

          return new HealthCheckResult(HealthStatus.HEALTHY,
                                     String.format("All %d DataNodes are healthy", totalNodes));

        } catch (IOException e) {
          return new HealthCheckResult(HealthStatus.CRITICAL,
                                     "Cannot get DataNode status: " + e.getMessage());
        }
      }
    }

    /**
     * 磁盘空间健康检查
     */
    private static class DiskSpaceHealthCheck implements HealthCheck {

      @Override
      public String getName() {
        return "Disk Space Health";
      }

      @Override
      public HealthCheckResult performCheck(Configuration conf) {
        try {
          DistributedFileSystem dfs = (DistributedFileSystem) FileSystem.get(conf);
          FsStatus status = dfs.getStatus();

          long total = status.getCapacity();
          long used = status.getUsed();
          long remaining = status.getRemaining();

          double usagePercent = (double) used / total * 100;
          double remainingPercent = (double) remaining / total * 100;

          if (remainingPercent < 5) { // 剩余空间少于5%
            return new HealthCheckResult(HealthStatus.CRITICAL,
                                       String.format("Very low disk space: %.1f%% remaining",
                                                    remainingPercent));
          } else if (remainingPercent < 15) { // 剩余空间少于15%
            return new HealthCheckResult(HealthStatus.WARNING,
                                       String.format("Low disk space: %.1f%% remaining",
                                                    remainingPercent));
          }

          return new HealthCheckResult(HealthStatus.HEALTHY,
                                     String.format("Disk space is healthy: %.1f%% used, %.1f%% remaining",
                                                  usagePercent, remainingPercent));

        } catch (IOException e) {
          return new HealthCheckResult(HealthStatus.CRITICAL,
                                     "Cannot get disk space status: " + e.getMessage());
        }
      }
    }

    /**
     * 网络连通性检查
     */
    private static class NetworkConnectivityHealthCheck implements HealthCheck {

      @Override
      public String getName() {
        return "Network Connectivity";
      }

      @Override
      public HealthCheckResult performCheck(Configuration conf) {
        List<String> issues = new ArrayList<>();

        // 检查NameNode连通性
        String nameNodeAddress = conf.get("fs.defaultFS");
        if (!checkConnectivity(nameNodeAddress)) {
          issues.add("Cannot connect to NameNode: " + nameNodeAddress);
        }

        // 检查ResourceManager连通性
        String rmAddress = conf.get("yarn.resourcemanager.address");
        if (rmAddress != null && !checkConnectivity("http://" + rmAddress)) {
          issues.add("Cannot connect to ResourceManager: " + rmAddress);
        }

        if (!issues.isEmpty()) {
          return new HealthCheckResult(HealthStatus.CRITICAL,
                                     "Network connectivity issues: " + String.join(", ", issues));
        }

        return new HealthCheckResult(HealthStatus.HEALTHY, "Network connectivity is good");
      }

      private boolean checkConnectivity(String address) {
        try {
          URI uri = new URI(address);
          String host = uri.getHost();
          int port = uri.getPort();

          if (port == -1) {
            port = uri.getScheme().equals("https") ? 443 : 80;
          }

          try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(host, port), 5000);
            return true;
          }

        } catch (Exception e) {
          return false;
        }
      }
    }

    /**
     * ResourceManager健康检查
     */
    private static class ResourceManagerHealthCheck implements HealthCheck {

      @Override
      public String getName() {
        return "ResourceManager Health";
      }

      @Override
      public HealthCheckResult performCheck(Configuration conf) {
        // 实现ResourceManager健康检查逻辑
        return new HealthCheckResult(HealthStatus.HEALTHY, "ResourceManager is healthy");
      }
    }

    /**
     * NodeManager健康检查
     */
    private static class NodeManagerHealthCheck implements HealthCheck {

      @Override
      public String getName() {
        return "NodeManager Health";
      }

      @Override
      public HealthCheckResult performCheck(Configuration conf) {
        // 实现NodeManager健康检查逻辑
        return new HealthCheckResult(HealthStatus.HEALTHY, "NodeManager is healthy");
      }
    }
  }

  /**
   * 健康检查接口
   */
  public interface HealthCheck {
    String getName();
    HealthCheckResult performCheck(Configuration conf);
  }

  /**
   * 健康检查结果
   */
  public static class HealthCheckResult {
    private final HealthStatus status;
    private final String message;

    public HealthCheckResult(HealthStatus status, String message) {
      this.status = status;
      this.message = message;
    }

    public HealthStatus getStatus() { return status; }
    public String getMessage() { return message; }
  }

  /**
   * 健康状态
   */
  public enum HealthStatus {
    HEALTHY, WARNING, CRITICAL, UNKNOWN
  }

  /**
   * 集群健康报告
   */
  public static class ClusterHealthReport {
    private final Map<String, HealthCheckResult> results = new HashMap<>();
    private final long timestamp = System.currentTimeMillis();

    public void addResult(String checkName, HealthCheckResult result) {
      results.put(checkName, result);
    }

    public Map<String, HealthCheckResult> getResults() {
      return results;
    }

    public long getTimestamp() {
      return timestamp;
    }

    public HealthStatus getOverallStatus() {
      boolean hasCritical = results.values().stream()
          .anyMatch(r -> r.getStatus() == HealthStatus.CRITICAL);

      if (hasCritical) {
        return HealthStatus.CRITICAL;
      }

      boolean hasWarning = results.values().stream()
          .anyMatch(r -> r.getStatus() == HealthStatus.WARNING);

      if (hasWarning) {
        return HealthStatus.WARNING;
      }

      return HealthStatus.HEALTHY;
    }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Cluster Health Report (").append(new Date(timestamp)).append("):\n");
      sb.append("Overall Status: ").append(getOverallStatus()).append("\n\n");

      for (Map.Entry<String, HealthCheckResult> entry : results.entrySet()) {
        HealthCheckResult result = entry.getValue();
        sb.append(entry.getKey()).append(": ").append(result.getStatus())
          .append(" - ").append(result.getMessage()).append("\n");
      }

      return sb.toString();
    }
  }
}
```

## 15.4 性能分析与调优建议

### 15.4.1 性能瓶颈识别

识别和分析Hadoop集群的性能瓶颈：

```java
/**
 * 性能瓶颈分析器
 */
public class PerformanceBottleneckAnalyzer {

  private final MetricsCollector metricsCollector;
  private final Map<String, PerformanceThreshold> thresholds;

  public PerformanceBottleneckAnalyzer() {
    this.metricsCollector = new MetricsCollector();
    this.thresholds = initializeThresholds();
  }

  private Map<String, PerformanceThreshold> initializeThresholds() {
    Map<String, PerformanceThreshold> thresholds = new HashMap<>();

    // CPU阈值
    thresholds.put("cpu.usage", new PerformanceThreshold(80.0, 95.0));
    thresholds.put("cpu.load_average", new PerformanceThreshold(0.8, 1.5));

    // 内存阈值
    thresholds.put("memory.usage", new PerformanceThreshold(80.0, 95.0));
    thresholds.put("memory.heap.usage", new PerformanceThreshold(75.0, 90.0));

    // 磁盘阈值
    thresholds.put("disk.usage", new PerformanceThreshold(80.0, 95.0));
    thresholds.put("disk.io.utilization", new PerformanceThreshold(70.0, 90.0));

    // 网络阈值
    thresholds.put("network.utilization", new PerformanceThreshold(70.0, 90.0));
    thresholds.put("network.latency", new PerformanceThreshold(100.0, 500.0)); // ms

    // HDFS阈值
    thresholds.put("hdfs.namenode.rpc.queue_time", new PerformanceThreshold(100.0, 1000.0)); // ms
    thresholds.put("hdfs.missing_blocks", new PerformanceThreshold(1.0, 10.0));

    // YARN阈值
    thresholds.put("yarn.memory.utilization", new PerformanceThreshold(80.0, 95.0));
    thresholds.put("yarn.pending_applications", new PerformanceThreshold(10.0, 50.0));

    return thresholds;
  }

  /**
   * 分析性能瓶颈
   */
  public PerformanceAnalysisReport analyzePerformance() {
    PerformanceAnalysisReport report = new PerformanceAnalysisReport();

    // 收集当前指标
    Map<String, Double> currentMetrics = metricsCollector.collectCurrentMetrics();

    // 分析各个维度的性能
    analyzeResourceUtilization(currentMetrics, report);
    analyzeHDFSPerformance(currentMetrics, report);
    analyzeYARNPerformance(currentMetrics, report);
    analyzeApplicationPerformance(currentMetrics, report);

    // 生成调优建议
    generateTuningRecommendations(report);

    return report;
  }

  private void analyzeResourceUtilization(Map<String, Double> metrics,
                                        PerformanceAnalysisReport report) {

    // CPU分析
    Double cpuUsage = metrics.get("cpu.usage");
    if (cpuUsage != null) {
      PerformanceThreshold threshold = thresholds.get("cpu.usage");
      if (cpuUsage > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.CPU,
            BottleneckSeverity.CRITICAL,
            "CPU usage is critically high: " + String.format("%.1f%%", cpuUsage),
            "Consider adding more CPU cores or optimizing CPU-intensive tasks"
        ));
      } else if (cpuUsage > threshold.getWarning()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.CPU,
            BottleneckSeverity.WARNING,
            "CPU usage is high: " + String.format("%.1f%%", cpuUsage),
            "Monitor CPU usage and consider optimization"
        ));
      }
    }

    // 内存分析
    Double memoryUsage = metrics.get("memory.usage");
    if (memoryUsage != null) {
      PerformanceThreshold threshold = thresholds.get("memory.usage");
      if (memoryUsage > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.MEMORY,
            BottleneckSeverity.CRITICAL,
            "Memory usage is critically high: " + String.format("%.1f%%", memoryUsage),
            "Add more memory or optimize memory usage"
        ));
      } else if (memoryUsage > threshold.getWarning()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.MEMORY,
            BottleneckSeverity.WARNING,
            "Memory usage is high: " + String.format("%.1f%%", memoryUsage),
            "Monitor memory usage trends"
        ));
      }
    }

    // 磁盘分析
    Double diskUsage = metrics.get("disk.usage");
    if (diskUsage != null) {
      PerformanceThreshold threshold = thresholds.get("disk.usage");
      if (diskUsage > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.DISK,
            BottleneckSeverity.CRITICAL,
            "Disk usage is critically high: " + String.format("%.1f%%", diskUsage),
            "Clean up disk space or add more storage"
        ));
      }
    }

    // 网络分析
    Double networkLatency = metrics.get("network.latency");
    if (networkLatency != null) {
      PerformanceThreshold threshold = thresholds.get("network.latency");
      if (networkLatency > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.NETWORK,
            BottleneckSeverity.CRITICAL,
            "Network latency is high: " + String.format("%.1fms", networkLatency),
            "Check network configuration and hardware"
        ));
      }
    }
  }

  private void analyzeHDFSPerformance(Map<String, Double> metrics,
                                     PerformanceAnalysisReport report) {

    // NameNode RPC队列时间
    Double rpcQueueTime = metrics.get("hdfs.namenode.rpc.queue_time");
    if (rpcQueueTime != null) {
      PerformanceThreshold threshold = thresholds.get("hdfs.namenode.rpc.queue_time");
      if (rpcQueueTime > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.HDFS_NAMENODE,
            BottleneckSeverity.CRITICAL,
            "NameNode RPC queue time is high: " + String.format("%.1fms", rpcQueueTime),
            "Increase NameNode handler count or optimize NameNode performance"
        ));
      }
    }

    // 缺失块
    Double missingBlocks = metrics.get("hdfs.missing_blocks");
    if (missingBlocks != null && missingBlocks > 0) {
      report.addBottleneck(new PerformanceBottleneck(
          BottleneckType.HDFS_DATA_INTEGRITY,
          BottleneckSeverity.CRITICAL,
          "Missing blocks detected: " + missingBlocks.intValue(),
          "Investigate DataNode failures and restore missing blocks"
      ));
    }
  }

  private void analyzeYARNPerformance(Map<String, Double> metrics,
                                     PerformanceAnalysisReport report) {

    // YARN内存利用率
    Double yarnMemoryUtil = metrics.get("yarn.memory.utilization");
    if (yarnMemoryUtil != null) {
      PerformanceThreshold threshold = thresholds.get("yarn.memory.utilization");
      if (yarnMemoryUtil > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.YARN_RESOURCE,
            BottleneckSeverity.CRITICAL,
            "YARN memory utilization is high: " + String.format("%.1f%%", yarnMemoryUtil),
            "Add more NodeManager nodes or optimize application memory usage"
        ));
      }
    }

    // 等待中的应用
    Double pendingApps = metrics.get("yarn.pending_applications");
    if (pendingApps != null) {
      PerformanceThreshold threshold = thresholds.get("yarn.pending_applications");
      if (pendingApps > threshold.getCritical()) {
        report.addBottleneck(new PerformanceBottleneck(
            BottleneckType.YARN_SCHEDULING,
            BottleneckSeverity.WARNING,
            "Many applications are pending: " + pendingApps.intValue(),
            "Check resource availability and scheduling configuration"
        ));
      }
    }
  }

  private void analyzeApplicationPerformance(Map<String, Double> metrics,
                                           PerformanceAnalysisReport report) {

    // 应用性能分析逻辑
    Double avgMapTime = metrics.get("mapreduce.avg_map_time");
    Double avgReduceTime = metrics.get("mapreduce.avg_reduce_time");

    if (avgMapTime != null && avgMapTime > 300000) { // 5分钟
      report.addBottleneck(new PerformanceBottleneck(
          BottleneckType.APPLICATION,
          BottleneckSeverity.WARNING,
          "Map tasks are running slowly: " + String.format("%.1fs", avgMapTime / 1000),
          "Optimize map task logic or increase map task memory"
      ));
    }

    if (avgReduceTime != null && avgReduceTime > 600000) { // 10分钟
      report.addBottleneck(new PerformanceBottleneck(
          BottleneckType.APPLICATION,
          BottleneckSeverity.WARNING,
          "Reduce tasks are running slowly: " + String.format("%.1fs", avgReduceTime / 1000),
          "Optimize reduce task logic or tune shuffle parameters"
      ));
    }
  }

  private void generateTuningRecommendations(PerformanceAnalysisReport report) {
    List<PerformanceBottleneck> bottlenecks = report.getBottlenecks();

    // 基于瓶颈类型生成调优建议
    Map<BottleneckType, List<PerformanceBottleneck>> groupedBottlenecks =
        bottlenecks.stream().collect(Collectors.groupingBy(PerformanceBottleneck::getType));

    for (Map.Entry<BottleneckType, List<PerformanceBottleneck>> entry : groupedBottlenecks.entrySet()) {
      BottleneckType type = entry.getKey();
      List<PerformanceBottleneck> typeBottlenecks = entry.getValue();

      TuningRecommendation recommendation = generateRecommendationForType(type, typeBottlenecks);
      if (recommendation != null) {
        report.addRecommendation(recommendation);
      }
    }
  }

  private TuningRecommendation generateRecommendationForType(BottleneckType type,
                                                           List<PerformanceBottleneck> bottlenecks) {

    switch (type) {
      case CPU:
        return new TuningRecommendation(
            "CPU Optimization",
            Arrays.asList(
                "Increase the number of CPU cores",
                "Optimize CPU-intensive algorithms",
                "Adjust task parallelism settings",
                "Consider using more efficient data structures"
            ),
            TuningPriority.HIGH
        );

      case MEMORY:
        return new TuningRecommendation(
            "Memory Optimization",
            Arrays.asList(
                "Increase available memory",
                "Optimize JVM heap settings",
                "Reduce memory usage in applications",
                "Enable memory compression if available"
            ),
            TuningPriority.HIGH
        );

      case DISK:
        return new TuningRecommendation(
            "Disk Optimization",
            Arrays.asList(
                "Add more disk storage",
                "Use faster storage devices (SSD)",
                "Optimize disk I/O patterns",
                "Enable data compression"
            ),
            TuningPriority.MEDIUM
        );

      case NETWORK:
        return new TuningRecommendation(
            "Network Optimization",
            Arrays.asList(
                "Upgrade network hardware",
                "Optimize network topology",
                "Tune TCP parameters",
                "Enable data compression for network transfers"
            ),
            TuningPriority.MEDIUM
        );

      case HDFS_NAMENODE:
        return new TuningRecommendation(
            "NameNode Optimization",
            Arrays.asList(
                "Increase NameNode handler count",
                "Optimize NameNode heap size",
                "Enable NameNode federation",
                "Use SSD for NameNode metadata storage"
            ),
            TuningPriority.HIGH
        );

      case YARN_RESOURCE:
        return new TuningRecommendation(
            "YARN Resource Optimization",
            Arrays.asList(
                "Add more NodeManager nodes",
                "Optimize container resource allocation",
                "Tune scheduler parameters",
                "Optimize application resource requests"
            ),
            TuningPriority.HIGH
        );

      default:
        return null;
    }
  }

  /**
   * 性能阈值
   */
  public static class PerformanceThreshold {
    private final double warning;
    private final double critical;

    public PerformanceThreshold(double warning, double critical) {
      this.warning = warning;
      this.critical = critical;
    }

    public double getWarning() { return warning; }
    public double getCritical() { return critical; }
  }

  /**
   * 性能瓶颈
   */
  public static class PerformanceBottleneck {
    private final BottleneckType type;
    private final BottleneckSeverity severity;
    private final String description;
    private final String recommendation;

    public PerformanceBottleneck(BottleneckType type, BottleneckSeverity severity,
                               String description, String recommendation) {
      this.type = type;
      this.severity = severity;
      this.description = description;
      this.recommendation = recommendation;
    }

    // Getters
    public BottleneckType getType() { return type; }
    public BottleneckSeverity getSeverity() { return severity; }
    public String getDescription() { return description; }
    public String getRecommendation() { return recommendation; }
  }

  /**
   * 瓶颈类型
   */
  public enum BottleneckType {
    CPU, MEMORY, DISK, NETWORK, HDFS_NAMENODE, HDFS_DATA_INTEGRITY,
    YARN_RESOURCE, YARN_SCHEDULING, APPLICATION
  }

  /**
   * 瓶颈严重程度
   */
  public enum BottleneckSeverity {
    WARNING, CRITICAL
  }

  /**
   * 调优建议
   */
  public static class TuningRecommendation {
    private final String title;
    private final List<String> actions;
    private final TuningPriority priority;

    public TuningRecommendation(String title, List<String> actions, TuningPriority priority) {
      this.title = title;
      this.actions = actions;
      this.priority = priority;
    }

    // Getters
    public String getTitle() { return title; }
    public List<String> getActions() { return actions; }
    public TuningPriority getPriority() { return priority; }
  }

  /**
   * 调优优先级
   */
  public enum TuningPriority {
    LOW, MEDIUM, HIGH, CRITICAL
  }

  /**
   * 性能分析报告
   */
  public static class PerformanceAnalysisReport {
    private final List<PerformanceBottleneck> bottlenecks = new ArrayList<>();
    private final List<TuningRecommendation> recommendations = new ArrayList<>();
    private final long timestamp = System.currentTimeMillis();

    public void addBottleneck(PerformanceBottleneck bottleneck) {
      bottlenecks.add(bottleneck);
    }

    public void addRecommendation(TuningRecommendation recommendation) {
      recommendations.add(recommendation);
    }

    public List<PerformanceBottleneck> getBottlenecks() { return bottlenecks; }
    public List<TuningRecommendation> getRecommendations() { return recommendations; }
    public long getTimestamp() { return timestamp; }

    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("Performance Analysis Report (").append(new Date(timestamp)).append("):\n\n");

      if (!bottlenecks.isEmpty()) {
        sb.append("Performance Bottlenecks:\n");
        for (PerformanceBottleneck bottleneck : bottlenecks) {
          sb.append("- [").append(bottleneck.getSeverity()).append("] ")
            .append(bottleneck.getType()).append(": ")
            .append(bottleneck.getDescription()).append("\n");
        }
        sb.append("\n");
      }

      if (!recommendations.isEmpty()) {
        sb.append("Tuning Recommendations:\n");
        for (TuningRecommendation rec : recommendations) {
          sb.append("- ").append(rec.getTitle()).append(" (Priority: ")
            .append(rec.getPriority()).append("):\n");
          for (String action : rec.getActions()) {
            sb.append("  * ").append(action).append("\n");
          }
        }
      }

      return sb.toString();
    }
  }
}
```

## 15.5 本章小结

理解这些监控和诊断技术，对于在生产环境中运维大规模Hadoop集群具有重要意义。随着集群规模的不断扩大，监控与故障诊断将变得越来越重要。
