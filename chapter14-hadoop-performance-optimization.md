# 第14章：Hadoop性能优化与调优

## 14.1 引言

Hadoop性能优化是一个复杂而系统性的工程，涉及硬件配置、软件参数、网络拓扑、存储策略等多个层面。在大数据处理场景中，即使是微小的性能提升也可能带来显著的成本节约和效率改进。一个经过精心调优的Hadoop集群，其性能可能比默认配置提升数倍甚至数十倍。

Hadoop性能优化的挑战在于其分布式特性和组件间的复杂交互。HDFS的读写性能、YARN的资源调度效率、MapReduce的任务执行速度，以及网络带宽、磁盘I/O、内存使用等底层资源，都会相互影响并最终决定整体系统性能。因此，性能优化需要采用系统性的方法，从监控诊断开始，识别性能瓶颈，然后针对性地进行调优。

现代Hadoop生态系统提供了丰富的性能监控和调优工具。从JVM级别的垃圾回收优化，到集群级别的资源配置调整，再到应用级别的算法优化，每个层面都有相应的优化策略和最佳实践。同时，随着硬件技术的发展，如SSD存储、高速网络、大内存服务器等，也为Hadoop性能优化提供了新的机遇和挑战。

本章将系统性地介绍Hadoop性能优化的理论基础、实践方法和工具技术，帮助读者建立完整的性能调优知识体系，并掌握在生产环境中进行有效性能优化的技能。

## 14.2 性能监控与诊断

### 14.2.1 系统级性能监控

全面的性能监控是优化的基础：

```java
/**
 * 系统性能监控器
 */
public class SystemPerformanceMonitor {
  
  private final ScheduledExecutorService scheduler;
  private final MetricsRegistry metricsRegistry;
  private final List<PerformanceCollector> collectors;
  
  public SystemPerformanceMonitor() {
    this.scheduler = Executors.newScheduledThreadPool(4);
    this.metricsRegistry = new MetricsRegistry();
    this.collectors = Arrays.asList(
        new CPUPerformanceCollector(),
        new MemoryPerformanceCollector(),
        new DiskPerformanceCollector(),
        new NetworkPerformanceCollector(),
        new JVMPerformanceCollector()
    );
  }
  
  public void startMonitoring() {
    for (PerformanceCollector collector : collectors) {
      scheduler.scheduleAtFixedRate(
          () -> collectMetrics(collector),
          0, collector.getCollectionInterval(), TimeUnit.SECONDS
      );
    }
  }
  
  private void collectMetrics(PerformanceCollector collector) {
    try {
      Map<String, Object> metrics = collector.collect();
      for (Map.Entry<String, Object> entry : metrics.entrySet()) {
        metricsRegistry.recordMetric(entry.getKey(), entry.getValue());
      }
    } catch (Exception e) {
      LOG.error("Failed to collect metrics from " + collector.getClass().getSimpleName(), e);
    }
  }
  
  /**
   * CPU性能收集器
   */
  private static class CPUPerformanceCollector implements PerformanceCollector {
    
    private final OperatingSystemMXBean osBean;
    private long lastCpuTime = 0;
    private long lastUpTime = 0;
    
    public CPUPerformanceCollector() {
      this.osBean = ManagementFactory.getOperatingSystemMXBean();
    }
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      // CPU使用率
      if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
        com.sun.management.OperatingSystemMXBean sunOsBean = 
            (com.sun.management.OperatingSystemMXBean) osBean;
        
        double processCpuLoad = sunOsBean.getProcessCpuLoad();
        double systemCpuLoad = sunOsBean.getSystemCpuLoad();
        
        metrics.put("cpu.process.usage", processCpuLoad * 100);
        metrics.put("cpu.system.usage", systemCpuLoad * 100);
      }
      
      // 负载平均值
      double loadAverage = osBean.getSystemLoadAverage();
      if (loadAverage >= 0) {
        metrics.put("cpu.load.average", loadAverage);
      }
      
      // CPU核心数
      metrics.put("cpu.cores", osBean.getAvailableProcessors());
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 10; // 10秒
    }
  }
  
  /**
   * 内存性能收集器
   */
  private static class MemoryPerformanceCollector implements PerformanceCollector {
    
    private final MemoryMXBean memoryBean;
    private final List<MemoryPoolMXBean> memoryPools;
    
    public MemoryPerformanceCollector() {
      this.memoryBean = ManagementFactory.getMemoryMXBean();
      this.memoryPools = ManagementFactory.getMemoryPoolMXBeans();
    }
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      // 堆内存使用情况
      MemoryUsage heapUsage = memoryBean.getHeapMemoryUsage();
      metrics.put("memory.heap.used", heapUsage.getUsed());
      metrics.put("memory.heap.max", heapUsage.getMax());
      metrics.put("memory.heap.committed", heapUsage.getCommitted());
      metrics.put("memory.heap.usage.percent", 
                 (double) heapUsage.getUsed() / heapUsage.getMax() * 100);
      
      // 非堆内存使用情况
      MemoryUsage nonHeapUsage = memoryBean.getNonHeapMemoryUsage();
      metrics.put("memory.nonheap.used", nonHeapUsage.getUsed());
      metrics.put("memory.nonheap.max", nonHeapUsage.getMax());
      metrics.put("memory.nonheap.committed", nonHeapUsage.getCommitted());
      
      // 各内存池使用情况
      for (MemoryPoolMXBean pool : memoryPools) {
        String poolName = pool.getName().replaceAll("\\s+", "_").toLowerCase();
        MemoryUsage usage = pool.getUsage();
        
        if (usage != null) {
          metrics.put("memory.pool." + poolName + ".used", usage.getUsed());
          metrics.put("memory.pool." + poolName + ".max", usage.getMax());
          
          if (usage.getMax() > 0) {
            metrics.put("memory.pool." + poolName + ".usage.percent",
                       (double) usage.getUsed() / usage.getMax() * 100);
          }
        }
      }
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 15; // 15秒
    }
  }
  
  /**
   * 磁盘性能收集器
   */
  private static class DiskPerformanceCollector implements PerformanceCollector {
    
    private final Map<String, DiskStats> lastStats = new ConcurrentHashMap<>();
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // 获取文件系统信息
        FileSystem[] fileSystems = File.listRoots();
        for (FileSystem fs : fileSystems) {
          String path = fs.getAbsolutePath();
          String name = path.replaceAll("[^a-zA-Z0-9]", "_");
          
          long totalSpace = fs.getTotalSpace();
          long freeSpace = fs.getFreeSpace();
          long usedSpace = totalSpace - freeSpace;
          
          metrics.put("disk." + name + ".total", totalSpace);
          metrics.put("disk." + name + ".free", freeSpace);
          metrics.put("disk." + name + ".used", usedSpace);
          
          if (totalSpace > 0) {
            metrics.put("disk." + name + ".usage.percent",
                       (double) usedSpace / totalSpace * 100);
          }
        }
        
        // 收集磁盘I/O统计（Linux系统）
        collectDiskIOStats(metrics);
        
      } catch (Exception e) {
        LOG.warn("Failed to collect disk metrics", e);
      }
      
      return metrics;
    }
    
    private void collectDiskIOStats(Map<String, Object> metrics) {
      try {
        // 读取/proc/diskstats文件
        Path diskStatsPath = Paths.get("/proc/diskstats");
        if (Files.exists(diskStatsPath)) {
          List<String> lines = Files.readAllLines(diskStatsPath);
          
          for (String line : lines) {
            String[] fields = line.trim().split("\\s+");
            if (fields.length >= 14) {
              String deviceName = fields[2];
              
              // 跳过分区，只处理整个磁盘
              if (deviceName.matches(".*\\d+$")) {
                continue;
              }
              
              DiskStats currentStats = new DiskStats(
                  Long.parseLong(fields[3]),  // reads completed
                  Long.parseLong(fields[7]),  // writes completed
                  Long.parseLong(fields[5]),  // sectors read
                  Long.parseLong(fields[9])   // sectors written
              );
              
              DiskStats lastStat = lastStats.get(deviceName);
              if (lastStat != null) {
                long readOps = currentStats.readsCompleted - lastStat.readsCompleted;
                long writeOps = currentStats.writesCompleted - lastStat.writesCompleted;
                long readBytes = (currentStats.sectorsRead - lastStat.sectorsRead) * 512;
                long writeBytes = (currentStats.sectorsWritten - lastStat.sectorsWritten) * 512;
                
                metrics.put("disk." + deviceName + ".read.ops", readOps);
                metrics.put("disk." + deviceName + ".write.ops", writeOps);
                metrics.put("disk." + deviceName + ".read.bytes", readBytes);
                metrics.put("disk." + deviceName + ".write.bytes", writeBytes);
              }
              
              lastStats.put(deviceName, currentStats);
            }
          }
        }
      } catch (Exception e) {
        LOG.debug("Failed to collect disk I/O stats", e);
      }
    }
    
    @Override
    public int getCollectionInterval() {
      return 30; // 30秒
    }
    
    private static class DiskStats {
      final long readsCompleted;
      final long writesCompleted;
      final long sectorsRead;
      final long sectorsWritten;
      
      DiskStats(long readsCompleted, long writesCompleted, 
               long sectorsRead, long sectorsWritten) {
        this.readsCompleted = readsCompleted;
        this.writesCompleted = writesCompleted;
        this.sectorsRead = sectorsRead;
        this.sectorsWritten = sectorsWritten;
      }
    }
  }
  
  /**
   * 网络性能收集器
   */
  private static class NetworkPerformanceCollector implements PerformanceCollector {
    
    private final Map<String, NetworkStats> lastStats = new ConcurrentHashMap<>();
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      try {
        // 获取网络接口统计
        Enumeration<NetworkInterface> interfaces = NetworkInterface.getNetworkInterfaces();
        
        while (interfaces.hasMoreElements()) {
          NetworkInterface ni = interfaces.nextElement();
          
          if (ni.isLoopback() || !ni.isUp()) {
            continue;
          }
          
          String interfaceName = ni.getName();
          
          // 收集网络统计（需要系统支持）
          collectNetworkStats(interfaceName, metrics);
        }
        
      } catch (Exception e) {
        LOG.warn("Failed to collect network metrics", e);
      }
      
      return metrics;
    }
    
    private void collectNetworkStats(String interfaceName, Map<String, Object> metrics) {
      try {
        // 读取/proc/net/dev文件（Linux系统）
        Path netDevPath = Paths.get("/proc/net/dev");
        if (Files.exists(netDevPath)) {
          List<String> lines = Files.readAllLines(netDevPath);
          
          for (String line : lines) {
            if (line.contains(interfaceName + ":")) {
              String[] parts = line.split(":");
              if (parts.length == 2) {
                String[] fields = parts[1].trim().split("\\s+");
                
                if (fields.length >= 16) {
                  NetworkStats currentStats = new NetworkStats(
                      Long.parseLong(fields[0]),  // bytes received
                      Long.parseLong(fields[8]),  // bytes transmitted
                      Long.parseLong(fields[1]),  // packets received
                      Long.parseLong(fields[9])   // packets transmitted
                  );
                  
                  NetworkStats lastStat = lastStats.get(interfaceName);
                  if (lastStat != null) {
                    long rxBytes = currentStats.bytesReceived - lastStat.bytesReceived;
                    long txBytes = currentStats.bytesTransmitted - lastStat.bytesTransmitted;
                    long rxPackets = currentStats.packetsReceived - lastStat.packetsReceived;
                    long txPackets = currentStats.packetsTransmitted - lastStat.packetsTransmitted;
                    
                    metrics.put("network." + interfaceName + ".rx.bytes", rxBytes);
                    metrics.put("network." + interfaceName + ".tx.bytes", txBytes);
                    metrics.put("network." + interfaceName + ".rx.packets", rxPackets);
                    metrics.put("network." + interfaceName + ".tx.packets", txPackets);
                  }
                  
                  lastStats.put(interfaceName, currentStats);
                }
              }
              break;
            }
          }
        }
      } catch (Exception e) {
        LOG.debug("Failed to collect network stats for " + interfaceName, e);
      }
    }
    
    @Override
    public int getCollectionInterval() {
      return 20; // 20秒
    }
    
    private static class NetworkStats {
      final long bytesReceived;
      final long bytesTransmitted;
      final long packetsReceived;
      final long packetsTransmitted;
      
      NetworkStats(long bytesReceived, long bytesTransmitted,
                  long packetsReceived, long packetsTransmitted) {
        this.bytesReceived = bytesReceived;
        this.bytesTransmitted = bytesTransmitted;
        this.packetsReceived = packetsReceived;
        this.packetsTransmitted = packetsTransmitted;
      }
    }
  }
  
  /**
   * JVM性能收集器
   */
  private static class JVMPerformanceCollector implements PerformanceCollector {
    
    private final List<GarbageCollectorMXBean> gcBeans;
    private final ThreadMXBean threadBean;
    private final Map<String, Long> lastGcCollections = new ConcurrentHashMap<>();
    private final Map<String, Long> lastGcTime = new ConcurrentHashMap<>();
    
    public JVMPerformanceCollector() {
      this.gcBeans = ManagementFactory.getGarbageCollectorMXBeans();
      this.threadBean = ManagementFactory.getThreadMXBean();
    }
    
    @Override
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      // GC统计
      for (GarbageCollectorMXBean gcBean : gcBeans) {
        String gcName = gcBean.getName().replaceAll("\\s+", "_").toLowerCase();
        
        long collections = gcBean.getCollectionCount();
        long time = gcBean.getCollectionTime();
        
        Long lastCollections = lastGcCollections.get(gcName);
        Long lastTime = lastGcTime.get(gcName);
        
        if (lastCollections != null && lastTime != null) {
          long collectionsDelta = collections - lastCollections;
          long timeDelta = time - lastTime;
          
          metrics.put("gc." + gcName + ".collections", collectionsDelta);
          metrics.put("gc." + gcName + ".time", timeDelta);
          
          if (collectionsDelta > 0) {
            metrics.put("gc." + gcName + ".avg_time", (double) timeDelta / collectionsDelta);
          }
        }
        
        lastGcCollections.put(gcName, collections);
        lastGcTime.put(gcName, time);
      }
      
      // 线程统计
      metrics.put("threads.count", threadBean.getThreadCount());
      metrics.put("threads.daemon.count", threadBean.getDaemonThreadCount());
      metrics.put("threads.peak.count", threadBean.getPeakThreadCount());
      
      // 类加载统计
      ClassLoadingMXBean classBean = ManagementFactory.getClassLoadingMXBean();
      metrics.put("classes.loaded", classBean.getLoadedClassCount());
      metrics.put("classes.total_loaded", classBean.getTotalLoadedClassCount());
      metrics.put("classes.unloaded", classBean.getUnloadedClassCount());
      
      return metrics;
    }
    
    @Override
    public int getCollectionInterval() {
      return 30; // 30秒
    }
  }
  
  public interface PerformanceCollector {
    Map<String, Object> collect();
    int getCollectionInterval();
  }
}
```

### 14.2.2 Hadoop组件性能监控

针对Hadoop各组件的专门监控：

```java
/**
 * HDFS性能监控器
 */
public class HDFSPerformanceMonitor {
  
  private final NameNodeMetrics nameNodeMetrics;
  private final DataNodeMetrics dataNodeMetrics;
  private final ClientMetrics clientMetrics;
  
  public HDFSPerformanceMonitor() {
    this.nameNodeMetrics = new NameNodeMetrics();
    this.dataNodeMetrics = new DataNodeMetrics();
    this.clientMetrics = new ClientMetrics();
  }
  
  /**
   * NameNode性能指标
   */
  public static class NameNodeMetrics {
    
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      // 文件系统统计
      FSNamesystem fsn = NameNode.getNameNodeMetrics().getFSNamesystem();
      if (fsn != null) {
        metrics.put("hdfs.namenode.files_total", fsn.getFilesTotal());
        metrics.put("hdfs.namenode.blocks_total", fsn.getBlocksTotal());
        metrics.put("hdfs.namenode.missing_blocks", fsn.getMissingBlocksCount());
        metrics.put("hdfs.namenode.corrupt_blocks", fsn.getCorruptReplicaBlocks());
        metrics.put("hdfs.namenode.under_replicated_blocks", fsn.getUnderReplicatedBlocks());
        metrics.put("hdfs.namenode.pending_replication_blocks", fsn.getPendingReplicationBlocks());
        metrics.put("hdfs.namenode.scheduled_replication_blocks", fsn.getScheduledReplicationBlocks());
      }
      
      // RPC性能统计
      RpcMetrics rpcMetrics = NameNode.getNameNodeMetrics().getRpcMetrics();
      if (rpcMetrics != null) {
        metrics.put("hdfs.namenode.rpc.queue_time_avg", rpcMetrics.getRpcQueueTimeAvgTime());
        metrics.put("hdfs.namenode.rpc.processing_time_avg", rpcMetrics.getRpcProcessingTimeAvgTime());
        metrics.put("hdfs.namenode.rpc.queue_length", rpcMetrics.getRpcQueueLength());
        metrics.put("hdfs.namenode.rpc.num_open_connections", rpcMetrics.getNumOpenConnections());
      }
      
      // JVM内存使用
      MemoryUsage heapUsage = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage();
      metrics.put("hdfs.namenode.memory.heap.used", heapUsage.getUsed());
      metrics.put("hdfs.namenode.memory.heap.max", heapUsage.getMax());
      metrics.put("hdfs.namenode.memory.heap.usage_percent", 
                 (double) heapUsage.getUsed() / heapUsage.getMax() * 100);
      
      return metrics;
    }
  }
  
  /**
   * DataNode性能指标
   */
  public static class DataNodeMetrics {
    
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      // 数据传输统计
      DataNodeMetrics dnMetrics = DataNode.getDataNodeMetrics();
      if (dnMetrics != null) {
        metrics.put("hdfs.datanode.bytes_written", dnMetrics.getBytesWritten());
        metrics.put("hdfs.datanode.bytes_read", dnMetrics.getBytesRead());
        metrics.put("hdfs.datanode.blocks_written", dnMetrics.getBlocksWritten());
        metrics.put("hdfs.datanode.blocks_read", dnMetrics.getBlocksRead());
        metrics.put("hdfs.datanode.blocks_replicated", dnMetrics.getBlocksReplicated());
        metrics.put("hdfs.datanode.blocks_removed", dnMetrics.getBlocksRemoved());
        metrics.put("hdfs.datanode.blocks_verified", dnMetrics.getBlocksVerified());
        metrics.put("hdfs.datanode.block_verification_failures", dnMetrics.getBlockVerificationFailures());
      }
      
      // 存储统计
      FsDatasetSpi<?> dataset = DataNode.getFSDataset();
      if (dataset != null) {
        try {
          long capacity = dataset.getCapacity();
          long dfsUsed = dataset.getDfsUsed();
          long remaining = dataset.getRemaining();
          
          metrics.put("hdfs.datanode.capacity", capacity);
          metrics.put("hdfs.datanode.dfs_used", dfsUsed);
          metrics.put("hdfs.datanode.remaining", remaining);
          metrics.put("hdfs.datanode.dfs_used_percent", (double) dfsUsed / capacity * 100);
          metrics.put("hdfs.datanode.remaining_percent", (double) remaining / capacity * 100);
        } catch (IOException e) {
          LOG.warn("Failed to get DataNode storage stats", e);
        }
      }
      
      return metrics;
    }
  }
  
  /**
   * HDFS客户端性能指标
   */
  public static class ClientMetrics {
    
    private final Map<String, Long> operationCounts = new ConcurrentHashMap<>();
    private final Map<String, Long> operationTimes = new ConcurrentHashMap<>();
    
    public void recordOperation(String operation, long duration) {
      operationCounts.merge(operation, 1L, Long::sum);
      operationTimes.merge(operation, duration, Long::sum);
    }
    
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      for (String operation : operationCounts.keySet()) {
        long count = operationCounts.get(operation);
        long totalTime = operationTimes.get(operation);
        
        metrics.put("hdfs.client." + operation + ".count", count);
        metrics.put("hdfs.client." + operation + ".total_time", totalTime);
        
        if (count > 0) {
          metrics.put("hdfs.client." + operation + ".avg_time", (double) totalTime / count);
        }
      }
      
      return metrics;
    }
  }
}

/**
 * YARN性能监控器
 */
public class YARNPerformanceMonitor {
  
  /**
   * ResourceManager性能指标
   */
  public static class ResourceManagerMetrics {
    
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      ResourceManager rm = ResourceManager.getInstance();
      if (rm != null) {
        RMContext rmContext = rm.getRMContext();
        
        // 集群资源统计
        ClusterMetrics clusterMetrics = ClusterMetrics.getMetrics();
        metrics.put("yarn.rm.apps_submitted", clusterMetrics.getAppsSubmitted());
        metrics.put("yarn.rm.apps_completed", clusterMetrics.getAppsCompleted());
        metrics.put("yarn.rm.apps_pending", clusterMetrics.getAppsPending());
        metrics.put("yarn.rm.apps_running", clusterMetrics.getAppsRunning());
        metrics.put("yarn.rm.apps_failed", clusterMetrics.getAppsFailed());
        metrics.put("yarn.rm.apps_killed", clusterMetrics.getAppsKilled());
        
        // 容器统计
        metrics.put("yarn.rm.containers_allocated", clusterMetrics.getAllocatedContainers());
        metrics.put("yarn.rm.containers_pending", clusterMetrics.getPendingContainers());
        metrics.put("yarn.rm.containers_reserved", clusterMetrics.getReservedContainers());
        
        // 节点统计
        metrics.put("yarn.rm.nodes_active", clusterMetrics.getNumActiveNMs());
        metrics.put("yarn.rm.nodes_lost", clusterMetrics.getNumLostNMs());
        metrics.put("yarn.rm.nodes_unhealthy", clusterMetrics.getNumUnhealthyNMs());
        metrics.put("yarn.rm.nodes_decommissioned", clusterMetrics.getNumDecommissionedNMs());
        
        // 资源统计
        Resource totalResource = clusterMetrics.getTotalResource();
        Resource availableResource = clusterMetrics.getAvailableResource();
        Resource allocatedResource = clusterMetrics.getAllocatedResource();
        
        metrics.put("yarn.rm.memory.total", totalResource.getMemorySize());
        metrics.put("yarn.rm.memory.available", availableResource.getMemorySize());
        metrics.put("yarn.rm.memory.allocated", allocatedResource.getMemorySize());
        metrics.put("yarn.rm.memory.utilization", 
                   (double) allocatedResource.getMemorySize() / totalResource.getMemorySize() * 100);
        
        metrics.put("yarn.rm.vcores.total", totalResource.getVirtualCores());
        metrics.put("yarn.rm.vcores.available", availableResource.getVirtualCores());
        metrics.put("yarn.rm.vcores.allocated", allocatedResource.getVirtualCores());
        metrics.put("yarn.rm.vcores.utilization",
                   (double) allocatedResource.getVirtualCores() / totalResource.getVirtualCores() * 100);
      }
      
      return metrics;
    }
  }
  
  /**
   * NodeManager性能指标
   */
  public static class NodeManagerMetrics {
    
    public Map<String, Object> collect() {
      Map<String, Object> metrics = new HashMap<>();
      
      NodeManager nm = NodeManager.getInstance();
      if (nm != null) {
        NodeManagerMetrics nmMetrics = nm.getNMMetrics();
        
        // 容器统计
        metrics.put("yarn.nm.containers_launched", nmMetrics.getContainersLaunched());
        metrics.put("yarn.nm.containers_completed", nmMetrics.getContainersCompleted());
        metrics.put("yarn.nm.containers_failed", nmMetrics.getContainersFailed());
        metrics.put("yarn.nm.containers_killed", nmMetrics.getContainersKilled());
        metrics.put("yarn.nm.containers_running", nmMetrics.getContainersRunning());
        
        // 资源使用统计
        ResourceUtilization utilization = nm.getResourceUtilization();
        if (utilization != null) {
          metrics.put("yarn.nm.memory.utilized", utilization.getPhysicalMemory());
          metrics.put("yarn.nm.cpu.utilized", utilization.getCPU());
        }
        
        // 本地化统计
        metrics.put("yarn.nm.public_cache_size", nmMetrics.getPublicCacheSize());
        metrics.put("yarn.nm.private_cache_size", nmMetrics.getPrivateCacheSize());
      }
      
      return metrics;
    }
  }
}
```

## 14.3 JVM调优与垃圾回收优化

### 14.3.1 JVM参数优化

针对Hadoop组件的JVM调优策略：

```java
/**
 * JVM调优配置生成器
 */
public class JVMTuningConfigGenerator {
  
  /**
   * NameNode JVM配置
   */
  public static class NameNodeJVMConfig {
    
    public static List<String> generateJVMArgs(long heapSizeGB, int gcThreads) {
      List<String> args = new ArrayList<>();
      
      // 堆内存设置
      args.add("-Xms" + heapSizeGB + "g");
      args.add("-Xmx" + heapSizeGB + "g");
      
      // 使用G1GC（推荐用于大堆）
      if (heapSizeGB >= 8) {
        args.addAll(getG1GCArgs(gcThreads));
      } else {
        args.addAll(getParallelGCArgs(gcThreads));
      }
      
      // NameNode特定优化
      args.add("-XX:+UseCompressedOops");
      args.add("-XX:+UseCompressedClassPointers");
      
      // 大对象处理优化
      args.add("-XX:+UseLargePages");
      args.add("-XX:LargePageSizeInBytes=2m");
      
      // JIT编译优化
      args.add("-XX:+TieredCompilation");
      args.add("-XX:+UseStringDeduplication");
      
      // 内存映射文件优化
      args.add("-XX:+UnlockExperimentalVMOptions");
      args.add("-XX:+UseTransparentHugePages");
      
      // 监控和诊断
      args.add("-XX:+PrintGCDetails");
      args.add("-XX:+PrintGCTimeStamps");
      args.add("-XX:+PrintGCApplicationStoppedTime");
      args.add("-Xloggc:/var/log/hadoop/namenode-gc.log");
      args.add("-XX:+UseGCLogFileRotation");
      args.add("-XX:NumberOfGCLogFiles=10");
      args.add("-XX:GCLogFileSize=10M");
      
      return args;
    }
    
    private static List<String> getG1GCArgs(int gcThreads) {
      List<String> args = new ArrayList<>();
      
      args.add("-XX:+UseG1GC");
      args.add("-XX:MaxGCPauseMillis=200");
      args.add("-XX:G1HeapRegionSize=16m");
      args.add("-XX:G1NewSizePercent=20");
      args.add("-XX:G1MaxNewSizePercent=40");
      args.add("-XX:G1MixedGCCountTarget=8");
      args.add("-XX:InitiatingHeapOccupancyPercent=45");
      args.add("-XX:G1MixedGCLiveThresholdPercent=85");
      args.add("-XX:G1HeapWastePercent=5");
      
      if (gcThreads > 0) {
        args.add("-XX:ParallelGCThreads=" + gcThreads);
        args.add("-XX:ConcGCThreads=" + Math.max(1, gcThreads / 4));
      }
      
      return args;
    }
    
    private static List<String> getParallelGCArgs(int gcThreads) {
      List<String> args = new ArrayList<>();
      
      args.add("-XX:+UseParallelGC");
      args.add("-XX:+UseParallelOldGC");
      args.add("-XX:NewRatio=3");
      args.add("-XX:SurvivorRatio=8");
      args.add("-XX:MaxGCPauseMillis=100");
      args.add("-XX:GCTimeRatio=19");
      
      if (gcThreads > 0) {
        args.add("-XX:ParallelGCThreads=" + gcThreads);
      }
      
      return args;
    }
  }
  
  /**
   * DataNode JVM配置
   */
  public static class DataNodeJVMConfig {
    
    public static List<String> generateJVMArgs(long heapSizeGB, int gcThreads) {
      List<String> args = new ArrayList<>();
      
      // 堆内存设置（DataNode通常不需要很大的堆）
      args.add("-Xms" + heapSizeGB + "g");
      args.add("-Xmx" + heapSizeGB + "g");
      
      // 使用Parallel GC（适合吞吐量优先的场景）
      args.addAll(getParallelGCArgs(gcThreads));
      
      // DataNode特定优化
      args.add("-XX:+UseCompressedOops");
      args.add("-XX:+UseCompressedClassPointers");
      
      // 直接内存优化（DataNode大量使用直接内存）
      args.add("-XX:MaxDirectMemorySize=" + (heapSizeGB * 2) + "g");
      
      // 网络I/O优化
      args.add("-Djava.net.preferIPv4Stack=true");
      args.add("-Dsun.net.inetaddr.ttl=300");
      
      // 文件I/O优化
      args.add("-Djava.io.tmpdir=/tmp/hadoop-datanode");
      
      // 监控和诊断
      args.add("-XX:+PrintGCDetails");
      args.add("-XX:+PrintGCTimeStamps");
      args.add("-Xloggc:/var/log/hadoop/datanode-gc.log");
      args.add("-XX:+UseGCLogFileRotation");
      args.add("-XX:NumberOfGCLogFiles=5");
      args.add("-XX:GCLogFileSize=10M");
      
      return args;
    }
    
    private static List<String> getParallelGCArgs(int gcThreads) {
      List<String> args = new ArrayList<>();
      
      args.add("-XX:+UseParallelGC");
      args.add("-XX:+UseParallelOldGC");
      args.add("-XX:NewRatio=2");
      args.add("-XX:SurvivorRatio=8");
      args.add("-XX:MaxGCPauseMillis=50");
      args.add("-XX:GCTimeRatio=99");
      
      if (gcThreads > 0) {
        args.add("-XX:ParallelGCThreads=" + gcThreads);
      }
      
      return args;
    }
  }
  
  /**
   * ResourceManager JVM配置
   */
  public static class ResourceManagerJVMConfig {
    
    public static List<String> generateJVMArgs(long heapSizeGB, int gcThreads) {
      List<String> args = new ArrayList<>();
      
      // 堆内存设置
      args.add("-Xms" + heapSizeGB + "g");
      args.add("-Xmx" + heapSizeGB + "g");
      
      // 使用G1GC（适合低延迟要求）
      args.addAll(getG1GCArgs(gcThreads));
      
      // ResourceManager特定优化
      args.add("-XX:+UseCompressedOops");
      args.add("-XX:+UseCompressedClassPointers");
      
      // 调度器优化
      args.add("-XX:+UseBiasedLocking");
      args.add("-XX:BiasedLockingStartupDelay=0");
      
      // 网络优化
      args.add("-Djava.net.preferIPv4Stack=true");
      args.add("-Dsun.net.inetaddr.ttl=300");
      
      // 监控和诊断
      args.add("-XX:+PrintGCDetails");
      args.add("-XX:+PrintGCTimeStamps");
      args.add("-XX:+PrintGCApplicationStoppedTime");
      args.add("-Xloggc:/var/log/hadoop/resourcemanager-gc.log");
      args.add("-XX:+UseGCLogFileRotation");
      args.add("-XX:NumberOfGCLogFiles=10");
      args.add("-XX:GCLogFileSize=10M");
      
      return args;
    }
    
    private static List<String> getG1GCArgs(int gcThreads) {
      List<String> args = new ArrayList<>();
      
      args.add("-XX:+UseG1GC");
      args.add("-XX:MaxGCPauseMillis=100");
      args.add("-XX:G1HeapRegionSize=8m");
      args.add("-XX:G1NewSizePercent=30");
      args.add("-XX:G1MaxNewSizePercent=50");
      args.add("-XX:InitiatingHeapOccupancyPercent=40");
      args.add("-XX:G1MixedGCLiveThresholdPercent=90");
      
      if (gcThreads > 0) {
        args.add("-XX:ParallelGCThreads=" + gcThreads);
        args.add("-XX:ConcGCThreads=" + Math.max(1, gcThreads / 4));
      }
      
      return args;
    }
  }
}
```

### 14.3.2 垃圾回收监控与分析

GC性能监控和分析工具：

```java
/**
 * GC性能分析器
 */
public class GCPerformanceAnalyzer {
  
  private final List<GCEvent> gcEvents = new ArrayList<>();
  private final Object lock = new Object();
  
  /**
   * GC事件记录
   */
  public static class GCEvent {
    private final String gcType;
    private final long timestamp;
    private final long duration;
    private final long beforeGC;
    private final long afterGC;
    private final long totalHeap;
    
    public GCEvent(String gcType, long timestamp, long duration,
                   long beforeGC, long afterGC, long totalHeap) {
      this.gcType = gcType;
      this.timestamp = timestamp;
      this.duration = duration;
      this.beforeGC = beforeGC;
      this.afterGC = afterGC;
      this.totalHeap = totalHeap;
    }
    
    public double getReclamationRate() {
      return (double) (beforeGC - afterGC) / beforeGC * 100;
    }
    
    public double getHeapUtilizationBefore() {
      return (double) beforeGC / totalHeap * 100;
    }
    
    public double getHeapUtilizationAfter() {
      return (double) afterGC / totalHeap * 100;
    }
    
    // Getters
    public String getGcType() { return gcType; }
    public long getTimestamp() { return timestamp; }
    public long getDuration() { return duration; }
    public long getBeforeGC() { return beforeGC; }
    public long getAfterGC() { return afterGC; }
    public long getTotalHeap() { return totalHeap; }
  }
  
  /**
   * 记录GC事件
   */
  public void recordGCEvent(GCEvent event) {
    synchronized (lock) {
      gcEvents.add(event);
      
      // 保持最近1000个事件
      if (gcEvents.size() > 1000) {
        gcEvents.remove(0);
      }
    }
  }
  
  /**
   * 分析GC性能
   */
  public GCAnalysisReport analyzeGCPerformance(long timeWindowMs) {
    synchronized (lock) {
      long currentTime = System.currentTimeMillis();
      long startTime = currentTime - timeWindowMs;
      
      List<GCEvent> recentEvents = gcEvents.stream()
          .filter(event -> event.getTimestamp() >= startTime)
          .collect(Collectors.toList());
      
      if (recentEvents.isEmpty()) {
        return new GCAnalysisReport();
      }
      
      return analyzeEvents(recentEvents, timeWindowMs);
    }
  }
  
  private GCAnalysisReport analyzeEvents(List<GCEvent> events, long timeWindowMs) {
    GCAnalysisReport report = new GCAnalysisReport();
    
    // 基本统计
    report.totalGCCount = events.size();
    report.totalGCTime = events.stream().mapToLong(GCEvent::getDuration).sum();
    report.gcTimePercentage = (double) report.totalGCTime / timeWindowMs * 100;
    
    // 按GC类型分组统计
    Map<String, List<GCEvent>> eventsByType = events.stream()
        .collect(Collectors.groupingBy(GCEvent::getGcType));
    
    for (Map.Entry<String, List<GCEvent>> entry : eventsByType.entrySet()) {
      String gcType = entry.getKey();
      List<GCEvent> typeEvents = entry.getValue();
      
      GCTypeStats stats = new GCTypeStats();
      stats.count = typeEvents.size();
      stats.totalTime = typeEvents.stream().mapToLong(GCEvent::getDuration).sum();
      stats.avgTime = (double) stats.totalTime / stats.count;
      stats.maxTime = typeEvents.stream().mapToLong(GCEvent::getDuration).max().orElse(0);
      stats.minTime = typeEvents.stream().mapToLong(GCEvent::getDuration).min().orElse(0);
      
      // 计算回收效率
      double avgReclamationRate = typeEvents.stream()
          .mapToDouble(GCEvent::getReclamationRate)
          .average().orElse(0.0);
      stats.avgReclamationRate = avgReclamationRate;
      
      report.gcTypeStats.put(gcType, stats);
    }
    
    // 计算趋势
    if (events.size() >= 10) {
      report.gcFrequencyTrend = calculateFrequencyTrend(events);
      report.gcDurationTrend = calculateDurationTrend(events);
    }
    
    // 检测异常
    report.anomalies = detectAnomalies(events);
    
    return report;
  }
  
  private double calculateFrequencyTrend(List<GCEvent> events) {
    // 将时间窗口分为两半，比较GC频率
    int midPoint = events.size() / 2;
    List<GCEvent> firstHalf = events.subList(0, midPoint);
    List<GCEvent> secondHalf = events.subList(midPoint, events.size());
    
    long firstHalfDuration = firstHalf.get(firstHalf.size() - 1).getTimestamp() - 
                            firstHalf.get(0).getTimestamp();
    long secondHalfDuration = secondHalf.get(secondHalf.size() - 1).getTimestamp() - 
                             secondHalf.get(0).getTimestamp();
    
    double firstHalfFreq = (double) firstHalf.size() / firstHalfDuration * 1000; // per second
    double secondHalfFreq = (double) secondHalf.size() / secondHalfDuration * 1000;
    
    return (secondHalfFreq - firstHalfFreq) / firstHalfFreq * 100; // percentage change
  }
  
  private double calculateDurationTrend(List<GCEvent> events) {
    // 计算GC持续时间的趋势
    int midPoint = events.size() / 2;
    List<GCEvent> firstHalf = events.subList(0, midPoint);
    List<GCEvent> secondHalf = events.subList(midPoint, events.size());
    
    double firstHalfAvgDuration = firstHalf.stream()
        .mapToLong(GCEvent::getDuration)
        .average().orElse(0.0);
    double secondHalfAvgDuration = secondHalf.stream()
        .mapToLong(GCEvent::getDuration)
        .average().orElse(0.0);
    
    return (secondHalfAvgDuration - firstHalfAvgDuration) / firstHalfAvgDuration * 100;
  }
  
  private List<String> detectAnomalies(List<GCEvent> events) {
    List<String> anomalies = new ArrayList<>();
    
    // 检测异常长的GC
    double avgDuration = events.stream().mapToLong(GCEvent::getDuration).average().orElse(0.0);
    double threshold = avgDuration * 3; // 3倍标准差
    
    long longGCCount = events.stream()
        .mapToLong(GCEvent::getDuration)
        .filter(duration -> duration > threshold)
        .count();
    
    if (longGCCount > 0) {
      anomalies.add("Detected " + longGCCount + " unusually long GC events (>" + 
                   String.format("%.1f", threshold) + "ms)");
    }
    
    // 检测GC频率异常
    if (events.size() > 100) { // 需要足够的样本
      double gcFrequency = (double) events.size() / 
          (events.get(events.size() - 1).getTimestamp() - events.get(0).getTimestamp()) * 1000;
      
      if (gcFrequency > 1.0) { // 每秒超过1次GC
        anomalies.add("High GC frequency detected: " + String.format("%.2f", gcFrequency) + " GC/sec");
      }
    }
    
    // 检测内存回收效率低
    double avgReclamationRate = events.stream()
        .mapToDouble(GCEvent::getReclamationRate)
        .average().orElse(0.0);
    
    if (avgReclamationRate < 10.0) { // 回收率低于10%
      anomalies.add("Low memory reclamation rate: " + String.format("%.1f", avgReclamationRate) + "%");
    }
    
    return anomalies;
  }
  
  /**
   * GC分析报告
   */
  public static class GCAnalysisReport {
    public int totalGCCount;
    public long totalGCTime;
    public double gcTimePercentage;
    public Map<String, GCTypeStats> gcTypeStats = new HashMap<>();
    public double gcFrequencyTrend;
    public double gcDurationTrend;
    public List<String> anomalies = new ArrayList<>();
    
    @Override
    public String toString() {
      StringBuilder sb = new StringBuilder();
      sb.append("GC Analysis Report:\n");
      sb.append("  Total GC Count: ").append(totalGCCount).append("\n");
      sb.append("  Total GC Time: ").append(totalGCTime).append("ms\n");
      sb.append("  GC Time Percentage: ").append(String.format("%.2f", gcTimePercentage)).append("%\n");
      
      sb.append("  GC Type Statistics:\n");
      for (Map.Entry<String, GCTypeStats> entry : gcTypeStats.entrySet()) {
        GCTypeStats stats = entry.getValue();
        sb.append("    ").append(entry.getKey()).append(":\n");
        sb.append("      Count: ").append(stats.count).append("\n");
        sb.append("      Avg Time: ").append(String.format("%.1f", stats.avgTime)).append("ms\n");
        sb.append("      Max Time: ").append(stats.maxTime).append("ms\n");
        sb.append("      Avg Reclamation: ").append(String.format("%.1f", stats.avgReclamationRate)).append("%\n");
      }
      
      if (!anomalies.isEmpty()) {
        sb.append("  Anomalies Detected:\n");
        for (String anomaly : anomalies) {
          sb.append("    - ").append(anomaly).append("\n");
        }
      }
      
      return sb.toString();
    }
  }
  
  /**
   * GC类型统计
   */
  public static class GCTypeStats {
    public int count;
    public long totalTime;
    public double avgTime;
    public long maxTime;
    public long minTime;
    public double avgReclamationRate;
  }
}
```

## 14.4 本章小结

本章深入分析了Hadoop性能优化的理论基础和实践方法。性能优化是一个系统性工程，需要从监控诊断开始，识别瓶颈，然后针对性地进行调优。

**核心要点**：

**性能监控**：建立全面的监控体系，包括系统级和组件级监控。

**JVM调优**：针对不同Hadoop组件的特点进行JVM参数优化。

**垃圾回收优化**：选择合适的GC算法并进行参数调优。

**性能分析**：使用专业工具进行性能瓶颈分析和诊断。

**系统性方法**：采用科学的方法论进行性能优化。

## 14.4 网络与存储优化

### 14.4.1 网络性能优化

网络是分布式系统的关键瓶颈：

```java
/**
 * 网络性能优化配置
 */
public class NetworkOptimizationConfig {

  /**
   * TCP参数优化
   */
  public static class TCPOptimization {

    public static Map<String, String> getOptimizedTCPSettings() {
      Map<String, String> settings = new HashMap<>();

      // TCP缓冲区大小优化
      settings.put("net.core.rmem_default", "262144");      // 256KB
      settings.put("net.core.rmem_max", "16777216");        // 16MB
      settings.put("net.core.wmem_default", "262144");      // 256KB
      settings.put("net.core.wmem_max", "16777216");        // 16MB

      // TCP窗口缩放
      settings.put("net.ipv4.tcp_window_scaling", "1");

      // TCP拥塞控制算法
      settings.put("net.ipv4.tcp_congestion_control", "cubic");

      // TCP快速重传
      settings.put("net.ipv4.tcp_fastopen", "3");

      // TCP keepalive设置
      settings.put("net.ipv4.tcp_keepalive_time", "600");
      settings.put("net.ipv4.tcp_keepalive_probes", "3");
      settings.put("net.ipv4.tcp_keepalive_intvl", "90");

      // TCP重传设置
      settings.put("net.ipv4.tcp_retries2", "5");

      // 网络队列长度
      settings.put("net.core.netdev_max_backlog", "5000");

      return settings;
    }

    public static void applyTCPOptimizations() {
      Map<String, String> settings = getOptimizedTCPSettings();

      for (Map.Entry<String, String> entry : settings.entrySet()) {
        try {
          String command = "echo " + entry.getValue() + " > /proc/sys/" +
                          entry.getKey().replace(".", "/");
          Runtime.getRuntime().exec(new String[]{"sh", "-c", command});
          LOG.info("Applied TCP setting: {} = {}", entry.getKey(), entry.getValue());
        } catch (IOException e) {
          LOG.warn("Failed to apply TCP setting: {} = {}", entry.getKey(), entry.getValue(), e);
        }
      }
    }
  }

  /**
   * Hadoop网络配置优化
   */
  public static class HadoopNetworkConfig {

    public static Configuration getOptimizedNetworkConfig() {
      Configuration conf = new Configuration();

      // RPC优化
      conf.setInt("ipc.server.handler.queue.size", 1000);
      conf.setInt("ipc.server.read.threadpool.size", 10);
      conf.setInt("ipc.client.connection.maxidletime", 10000);
      conf.setInt("ipc.client.connect.timeout", 20000);
      conf.setInt("ipc.client.connect.max.retries", 10);
      conf.setInt("ipc.client.connect.retry.interval", 1000);

      // 数据传输优化
      conf.setInt("dfs.datanode.handler.count", 40);
      conf.setInt("dfs.namenode.handler.count", 100);
      conf.setInt("dfs.datanode.max.transfer.threads", 8192);
      conf.setInt("dfs.client.socket.timeout", 60000);
      conf.setInt("dfs.socket.timeout", 60000);

      // 网络缓冲区大小
      conf.setInt("io.file.buffer.size", 131072);  // 128KB
      conf.setInt("dfs.stream-buffer-size", 131072);

      // 压缩传输
      conf.setBoolean("mapreduce.map.output.compress", true);
      conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

      return conf;
    }
  }

  /**
   * 网络拓扑优化
   */
  public static class NetworkTopologyOptimizer {

    private final NetworkTopology topology;
    private final Map<String, Integer> rackSwitchCapacity;

    public NetworkTopologyOptimizer() {
      this.topology = NetworkTopology.getInstance(new Configuration());
      this.rackSwitchCapacity = new HashMap<>();
    }

    /**
     * 优化数据放置策略
     */
    public List<DatanodeDescriptor> optimizeDataPlacement(
        List<DatanodeDescriptor> candidates, int replicationFactor) {

      List<DatanodeDescriptor> selected = new ArrayList<>();
      Set<String> usedRacks = new HashSet<>();

      // 第一个副本：选择最近的节点
      DatanodeDescriptor primary = selectPrimaryNode(candidates);
      if (primary != null) {
        selected.add(primary);
        usedRacks.add(topology.getRack(primary));
      }

      // 第二个副本：选择不同机架的节点
      if (replicationFactor > 1 && selected.size() < replicationFactor) {
        DatanodeDescriptor secondary = selectSecondaryNode(candidates, usedRacks);
        if (secondary != null) {
          selected.add(secondary);
          usedRacks.add(topology.getRack(secondary));
        }
      }

      // 第三个副本：在第二个副本的机架中选择不同节点
      if (replicationFactor > 2 && selected.size() < replicationFactor) {
        String secondaryRack = selected.size() > 1 ?
            topology.getRack(selected.get(1)) : null;
        DatanodeDescriptor tertiary = selectTertiaryNode(candidates, usedRacks, secondaryRack);
        if (tertiary != null) {
          selected.add(tertiary);
        }
      }

      // 剩余副本：尽量分散到不同机架
      while (selected.size() < replicationFactor && selected.size() < candidates.size()) {
        DatanodeDescriptor additional = selectAdditionalNode(candidates, selected);
        if (additional != null) {
          selected.add(additional);
        } else {
          break;
        }
      }

      return selected;
    }

    private DatanodeDescriptor selectPrimaryNode(List<DatanodeDescriptor> candidates) {
      // 选择负载最低的节点
      return candidates.stream()
          .min(Comparator.comparingDouble(this::calculateNodeLoad))
          .orElse(null);
    }

    private DatanodeDescriptor selectSecondaryNode(List<DatanodeDescriptor> candidates,
                                                  Set<String> usedRacks) {
      // 选择不同机架中负载最低的节点
      return candidates.stream()
          .filter(node -> !usedRacks.contains(topology.getRack(node)))
          .min(Comparator.comparingDouble(this::calculateNodeLoad))
          .orElse(null);
    }

    private DatanodeDescriptor selectTertiaryNode(List<DatanodeDescriptor> candidates,
                                                 Set<String> usedRacks,
                                                 String preferredRack) {
      // 优先在指定机架中选择
      Optional<DatanodeDescriptor> inRack = candidates.stream()
          .filter(node -> preferredRack != null &&
                         preferredRack.equals(topology.getRack(node)) &&
                         !usedRacks.contains(node.getHostName()))
          .min(Comparator.comparingDouble(this::calculateNodeLoad));

      if (inRack.isPresent()) {
        return inRack.get();
      }

      // 否则选择其他机架
      return candidates.stream()
          .filter(node -> !usedRacks.contains(topology.getRack(node)))
          .min(Comparator.comparingDouble(this::calculateNodeLoad))
          .orElse(null);
    }

    private DatanodeDescriptor selectAdditionalNode(List<DatanodeDescriptor> candidates,
                                                   List<DatanodeDescriptor> selected) {
      Set<String> usedNodes = selected.stream()
          .map(DatanodeDescriptor::getHostName)
          .collect(Collectors.toSet());

      return candidates.stream()
          .filter(node -> !usedNodes.contains(node.getHostName()))
          .min(Comparator.comparingDouble(this::calculateNodeLoad))
          .orElse(null);
    }

    private double calculateNodeLoad(DatanodeDescriptor node) {
      // 综合考虑CPU、内存、网络、磁盘负载
      double cpuLoad = node.getCpuUsage();
      double memoryLoad = node.getMemoryUsage();
      double networkLoad = node.getNetworkUsage();
      double diskLoad = node.getDiskUsage();

      // 加权平均
      return cpuLoad * 0.3 + memoryLoad * 0.2 + networkLoad * 0.3 + diskLoad * 0.2;
    }
  }
}

/**
 * 网络监控和诊断工具
 */
public class NetworkDiagnosticTool {

  /**
   * 网络延迟测试
   */
  public static class LatencyTester {

    public static Map<String, Long> testLatency(List<String> hosts, int iterations) {
      Map<String, Long> results = new HashMap<>();

      for (String host : hosts) {
        long totalLatency = 0;
        int successCount = 0;

        for (int i = 0; i < iterations; i++) {
          try {
            long startTime = System.nanoTime();

            // 简单的TCP连接测试
            Socket socket = new Socket();
            socket.connect(new InetSocketAddress(host, 8020), 5000);
            socket.close();

            long endTime = System.nanoTime();
            totalLatency += (endTime - startTime) / 1_000_000; // 转换为毫秒
            successCount++;

          } catch (IOException e) {
            LOG.warn("Failed to connect to {}: {}", host, e.getMessage());
          }

          try {
            Thread.sleep(100); // 间隔100ms
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            break;
          }
        }

        if (successCount > 0) {
          results.put(host, totalLatency / successCount);
        } else {
          results.put(host, -1L); // 表示连接失败
        }
      }

      return results;
    }
  }

  /**
   * 带宽测试
   */
  public static class BandwidthTester {

    public static Map<String, Double> testBandwidth(List<String> hosts, int testSizeKB) {
      Map<String, Double> results = new HashMap<>();

      for (String host : hosts) {
        try {
          double bandwidth = measureBandwidth(host, testSizeKB);
          results.put(host, bandwidth);
        } catch (IOException e) {
          LOG.warn("Failed to test bandwidth to {}: {}", host, e.getMessage());
          results.put(host, -1.0);
        }
      }

      return results;
    }

    private static double measureBandwidth(String host, int testSizeKB) throws IOException {
      byte[] testData = new byte[testSizeKB * 1024];
      new Random().nextBytes(testData);

      long startTime = System.nanoTime();

      try (Socket socket = new Socket(host, 8020);
           OutputStream out = socket.getOutputStream()) {

        out.write(testData);
        out.flush();

        long endTime = System.nanoTime();
        double durationSeconds = (endTime - startTime) / 1_000_000_000.0;

        // 返回带宽 (MB/s)
        return (testSizeKB / 1024.0) / durationSeconds;
      }
    }
  }
}
```

### 14.4.2 存储性能优化

存储系统的优化对Hadoop性能至关重要：

```java
/**
 * 存储性能优化器
 */
public class StoragePerformanceOptimizer {

  /**
   * 磁盘配置优化
   */
  public static class DiskOptimization {

    public static Map<String, String> getOptimizedDiskSettings() {
      Map<String, String> settings = new HashMap<>();

      // I/O调度器优化
      settings.put("scheduler", "deadline");  // 对于机械硬盘
      settings.put("scheduler_ssd", "noop");  // 对于SSD

      // 读取预取优化
      settings.put("read_ahead_kb", "4096");  // 4MB预读

      // 队列深度
      settings.put("queue_depth", "32");

      // 文件系统挂载选项
      settings.put("mount_options", "noatime,nodiratime,nobarrier");

      return settings;
    }

    public static void optimizeDiskPerformance(String devicePath) {
      try {
        // 设置I/O调度器
        String schedulerPath = "/sys/block/" + getDeviceName(devicePath) + "/queue/scheduler";
        if (isSSD(devicePath)) {
          writeToSysFile(schedulerPath, "noop");
        } else {
          writeToSysFile(schedulerPath, "deadline");
        }

        // 设置读取预取
        String readAheadPath = "/sys/block/" + getDeviceName(devicePath) + "/queue/read_ahead_kb";
        writeToSysFile(readAheadPath, "4096");

        // 设置队列深度
        String queueDepthPath = "/sys/block/" + getDeviceName(devicePath) + "/queue/nr_requests";
        writeToSysFile(queueDepthPath, "32");

        LOG.info("Optimized disk performance for {}", devicePath);

      } catch (IOException e) {
        LOG.error("Failed to optimize disk performance for " + devicePath, e);
      }
    }

    private static String getDeviceName(String devicePath) {
      // 从/dev/sda1提取sda
      String deviceName = devicePath.substring(devicePath.lastIndexOf('/') + 1);
      return deviceName.replaceAll("\\d+$", ""); // 移除分区号
    }

    private static boolean isSSD(String devicePath) {
      try {
        String rotationalPath = "/sys/block/" + getDeviceName(devicePath) + "/queue/rotational";
        String rotational = Files.readString(Paths.get(rotationalPath)).trim();
        return "0".equals(rotational);
      } catch (IOException e) {
        return false; // 默认假设是机械硬盘
      }
    }

    private static void writeToSysFile(String path, String value) throws IOException {
      Files.writeString(Paths.get(path), value);
    }
  }

  /**
   * HDFS存储优化
   */
  public static class HDFSStorageOptimization {

    public static Configuration getOptimizedStorageConfig() {
      Configuration conf = new Configuration();

      // 块大小优化
      conf.setLong("dfs.blocksize", 256 * 1024 * 1024); // 256MB

      // 副本数优化
      conf.setInt("dfs.replication", 3);

      // DataNode存储优化
      conf.setInt("dfs.datanode.handler.count", 40);
      conf.setInt("dfs.datanode.max.transfer.threads", 8192);
      conf.setBoolean("dfs.datanode.drop.cache.behind.writes", true);
      conf.setBoolean("dfs.datanode.drop.cache.behind.reads", true);
      conf.setBoolean("dfs.datanode.sync.behind.writes", false);

      // 写入优化
      conf.setInt("dfs.datanode.fsdataset.volume.choosing.policy",
                 "org.apache.hadoop.hdfs.server.datanode.fsdataset.AvailableSpaceVolumeChoosingPolicy");

      // 读取优化
      conf.setBoolean("dfs.client.read.shortcircuit", true);
      conf.set("dfs.domain.socket.path", "/var/lib/hadoop-hdfs/dn_socket");
      conf.setBoolean("dfs.client.cache.readahead", true);
      conf.setLong("dfs.client.cache.readahead.bytes", 4 * 1024 * 1024); // 4MB

      // 缓存优化
      conf.setLong("dfs.client.mmap.cache.size", 256);
      conf.setLong("dfs.client.mmap.cache.timeout.ms", 3600000); // 1小时

      return conf;
    }

    /**
     * 存储类型感知优化
     */
    public static void optimizeStorageTypes(Configuration conf) {
      // SSD存储类型配置
      conf.set("dfs.datanode.data.dir",
              "[SSD]/data1/hadoop/hdfs/data," +
              "[DISK]/data2/hadoop/hdfs/data," +
              "[DISK]/data3/hadoop/hdfs/data");

      // 存储策略
      conf.set("dfs.storage.policy.enabled", "true");

      // 热数据存储在SSD
      conf.set("dfs.namenode.storage.policy.satisfier.mode", "external");
    }
  }

  /**
   * 存储性能监控
   */
  public static class StoragePerformanceMonitor {

    private final Map<String, StorageMetrics> storageMetrics = new ConcurrentHashMap<>();

    public void collectStorageMetrics() {
      try {
        // 收集磁盘使用情况
        FileSystem[] roots = File.listRoots();
        for (FileSystem root : roots) {
          String path = root.getAbsolutePath();
          StorageMetrics metrics = new StorageMetrics();

          metrics.totalSpace = root.getTotalSpace();
          metrics.freeSpace = root.getFreeSpace();
          metrics.usedSpace = metrics.totalSpace - metrics.freeSpace;
          metrics.usagePercent = (double) metrics.usedSpace / metrics.totalSpace * 100;

          // 收集I/O统计
          collectIOStats(path, metrics);

          storageMetrics.put(path, metrics);
        }

      } catch (Exception e) {
        LOG.error("Failed to collect storage metrics", e);
      }
    }

    private void collectIOStats(String path, StorageMetrics metrics) {
      try {
        // 读取/proc/diskstats获取I/O统计
        List<String> lines = Files.readAllLines(Paths.get("/proc/diskstats"));

        for (String line : lines) {
          String[] fields = line.trim().split("\\s+");
          if (fields.length >= 14) {
            String deviceName = fields[2];

            if (isDeviceForPath(deviceName, path)) {
              metrics.readOps = Long.parseLong(fields[3]);
              metrics.writeOps = Long.parseLong(fields[7]);
              metrics.readBytes = Long.parseLong(fields[5]) * 512;
              metrics.writeBytes = Long.parseLong(fields[9]) * 512;
              metrics.ioTime = Long.parseLong(fields[12]);
              break;
            }
          }
        }
      } catch (IOException e) {
        LOG.debug("Failed to collect I/O stats for " + path, e);
      }
    }

    private boolean isDeviceForPath(String deviceName, String path) {
      // 简化的设备匹配逻辑
      return path.startsWith("/") && deviceName.matches("sd[a-z]+\\d*");
    }

    public Map<String, StorageMetrics> getStorageMetrics() {
      return new HashMap<>(storageMetrics);
    }

    /**
     * 存储指标
     */
    public static class StorageMetrics {
      public long totalSpace;
      public long freeSpace;
      public long usedSpace;
      public double usagePercent;
      public long readOps;
      public long writeOps;
      public long readBytes;
      public long writeBytes;
      public long ioTime;

      public double getIOUtilization() {
        // 简化的I/O利用率计算
        return Math.min(100.0, (double) ioTime / 1000.0 * 100);
      }

      @Override
      public String toString() {
        return String.format(
            "StorageMetrics{usage=%.1f%%, readOps=%d, writeOps=%d, ioUtil=%.1f%%}",
            usagePercent, readOps, writeOps, getIOUtilization()
        );
      }
    }
  }
}
```

## 14.5 集群配置调优

### 14.5.1 资源配置优化

合理的资源配置是性能优化的基础：

```java
/**
 * 集群资源配置优化器
 */
public class ClusterResourceOptimizer {

  /**
   * 内存配置优化
   */
  public static class MemoryConfigOptimizer {

    public static Configuration optimizeMemoryConfig(long totalMemoryGB,
                                                    int cpuCores,
                                                    String nodeType) {
      Configuration conf = new Configuration();

      switch (nodeType.toLowerCase()) {
        case "namenode":
          return optimizeNameNodeMemory(conf, totalMemoryGB);
        case "datanode":
          return optimizeDataNodeMemory(conf, totalMemoryGB, cpuCores);
        case "resourcemanager":
          return optimizeResourceManagerMemory(conf, totalMemoryGB);
        case "nodemanager":
          return optimizeNodeManagerMemory(conf, totalMemoryGB, cpuCores);
        default:
          throw new IllegalArgumentException("Unknown node type: " + nodeType);
      }
    }

    private static Configuration optimizeNameNodeMemory(Configuration conf, long totalMemoryGB) {
      // NameNode堆内存：总内存的50-70%
      long nameNodeHeapGB = Math.min(32, (long) (totalMemoryGB * 0.6));

      conf.set("HADOOP_NAMENODE_OPTS",
              "-Xms" + nameNodeHeapGB + "g -Xmx" + nameNodeHeapGB + "g");

      // 元数据缓存配置
      long metadataCacheSize = nameNodeHeapGB * 1024 * 1024 * 1024 / 4; // 25%的堆内存
      conf.setLong("dfs.namenode.name.cache.threshold", metadataCacheSize);

      // 块报告处理优化
      conf.setInt("dfs.namenode.handler.count", Math.max(20, (int) (nameNodeHeapGB * 2)));
      conf.setInt("dfs.namenode.service.handler.count", Math.max(10, (int) nameNodeHeapGB));

      return conf;
    }

    private static Configuration optimizeDataNodeMemory(Configuration conf,
                                                       long totalMemoryGB,
                                                       int cpuCores) {
      // DataNode堆内存：4-8GB通常足够
      long dataNodeHeapGB = Math.min(8, Math.max(4, totalMemoryGB / 8));

      conf.set("HADOOP_DATANODE_OPTS",
              "-Xms" + dataNodeHeapGB + "g -Xmx" + dataNodeHeapGB + "g");

      // 直接内存配置（用于零拷贝）
      long directMemoryGB = Math.min(totalMemoryGB - dataNodeHeapGB - 2, dataNodeHeapGB * 2);
      conf.set("HADOOP_DATANODE_OPTS",
              conf.get("HADOOP_DATANODE_OPTS") + " -XX:MaxDirectMemorySize=" + directMemoryGB + "g");

      // 数据传输线程数
      conf.setInt("dfs.datanode.max.transfer.threads", Math.min(8192, cpuCores * 256));

      return conf;
    }

    private static Configuration optimizeResourceManagerMemory(Configuration conf,
                                                              long totalMemoryGB) {
      // ResourceManager堆内存：8-16GB
      long rmHeapGB = Math.min(16, Math.max(8, totalMemoryGB / 4));

      conf.set("YARN_RESOURCEMANAGER_OPTS",
              "-Xms" + rmHeapGB + "g -Xmx" + rmHeapGB + "g");

      // 调度器配置
      conf.setInt("yarn.resourcemanager.scheduler.class",
                 "org.apache.hadoop.yarn.server.resourcemanager.scheduler.capacity.CapacityScheduler");

      // 应用程序管理
      conf.setInt("yarn.resourcemanager.max-completed-applications", 1000);
      conf.setLong("yarn.resourcemanager.application-timeouts.monitor.interval-ms", 30000);

      return conf;
    }

    private static Configuration optimizeNodeManagerMemory(Configuration conf,
                                                          long totalMemoryGB,
                                                          int cpuCores) {
      // NodeManager堆内存：2-4GB
      long nmHeapGB = Math.min(4, Math.max(2, totalMemoryGB / 16));

      conf.set("YARN_NODEMANAGER_OPTS",
              "-Xms" + nmHeapGB + "g -Xmx" + nmHeapGB + "g");

      // 容器内存配置
      long containerMemoryGB = totalMemoryGB - nmHeapGB - 2; // 保留2GB给系统
      conf.setLong("yarn.nodemanager.resource.memory-mb", containerMemoryGB * 1024);

      // 虚拟内存检查
      conf.setFloat("yarn.nodemanager.vmem-pmem-ratio", 2.1f);
      conf.setBoolean("yarn.nodemanager.vmem-check-enabled", false);

      // CPU配置
      conf.setInt("yarn.nodemanager.resource.cpu-vcores", cpuCores);

      return conf;
    }
  }

  /**
   * 并发配置优化
   */
  public static class ConcurrencyConfigOptimizer {

    public static Configuration optimizeConcurrencyConfig(int cpuCores,
                                                         long memoryGB,
                                                         String workloadType) {
      Configuration conf = new Configuration();

      switch (workloadType.toLowerCase()) {
        case "cpu_intensive":
          return optimizeCPUIntensiveWorkload(conf, cpuCores, memoryGB);
        case "io_intensive":
          return optimizeIOIntensiveWorkload(conf, cpuCores, memoryGB);
        case "memory_intensive":
          return optimizeMemoryIntensiveWorkload(conf, cpuCores, memoryGB);
        case "balanced":
          return optimizeBalancedWorkload(conf, cpuCores, memoryGB);
        default:
          return optimizeBalancedWorkload(conf, cpuCores, memoryGB);
      }
    }

    private static Configuration optimizeCPUIntensiveWorkload(Configuration conf,
                                                             int cpuCores,
                                                             long memoryGB) {
      // CPU密集型：线程数接近CPU核心数
      conf.setInt("mapreduce.job.maps", cpuCores);
      conf.setInt("mapreduce.job.reduces", cpuCores / 2);

      // Map任务配置
      conf.setInt("mapreduce.map.cpu.vcores", 1);
      conf.setLong("mapreduce.map.memory.mb", Math.max(1024, memoryGB * 1024 / cpuCores));

      // Reduce任务配置
      conf.setInt("mapreduce.reduce.cpu.vcores", 2);
      conf.setLong("mapreduce.reduce.memory.mb", Math.max(2048, memoryGB * 1024 / (cpuCores / 2)));

      // JVM重用
      conf.setInt("mapreduce.job.jvm.numtasks", 1);

      return conf;
    }

    private static Configuration optimizeIOIntensiveWorkload(Configuration conf,
                                                            int cpuCores,
                                                            long memoryGB) {
      // I/O密集型：可以有更多并发任务
      conf.setInt("mapreduce.job.maps", cpuCores * 2);
      conf.setInt("mapreduce.job.reduces", cpuCores);

      // 较小的内存分配
      conf.setLong("mapreduce.map.memory.mb", 512);
      conf.setLong("mapreduce.reduce.memory.mb", 1024);

      // I/O优化
      conf.setInt("mapreduce.task.io.sort.mb", 256);
      conf.setFloat("mapreduce.task.io.sort.spill.percent", 0.9f);
      conf.setInt("mapreduce.task.io.sort.factor", 64);

      // 压缩优化
      conf.setBoolean("mapreduce.map.output.compress", true);
      conf.set("mapreduce.map.output.compress.codec", "org.apache.hadoop.io.compress.SnappyCodec");

      return conf;
    }

    private static Configuration optimizeMemoryIntensiveWorkload(Configuration conf,
                                                                int cpuCores,
                                                                long memoryGB) {
      // 内存密集型：较少的并发任务，更多内存
      conf.setInt("mapreduce.job.maps", Math.max(1, cpuCores / 2));
      conf.setInt("mapreduce.job.reduces", Math.max(1, cpuCores / 4));

      // 大内存分配
      long mapMemoryMB = Math.min(8192, memoryGB * 1024 / 2);
      long reduceMemoryMB = Math.min(16384, memoryGB * 1024 / 2);

      conf.setLong("mapreduce.map.memory.mb", mapMemoryMB);
      conf.setLong("mapreduce.reduce.memory.mb", reduceMemoryMB);

      // JVM堆内存
      conf.set("mapreduce.map.java.opts", "-Xmx" + (mapMemoryMB * 0.8) + "m");
      conf.set("mapreduce.reduce.java.opts", "-Xmx" + (reduceMemoryMB * 0.8) + "m");

      // 排序缓冲区
      conf.setInt("mapreduce.task.io.sort.mb", (int) Math.min(2048, mapMemoryMB / 4));

      return conf;
    }

    private static Configuration optimizeBalancedWorkload(Configuration conf,
                                                         int cpuCores,
                                                         long memoryGB) {
      // 平衡型工作负载
      conf.setInt("mapreduce.job.maps", cpuCores);
      conf.setInt("mapreduce.job.reduces", cpuCores / 2);

      // 平衡的资源分配
      long mapMemoryMB = Math.max(1024, memoryGB * 1024 / cpuCores);
      long reduceMemoryMB = Math.max(2048, memoryGB * 1024 / (cpuCores / 2));

      conf.setLong("mapreduce.map.memory.mb", mapMemoryMB);
      conf.setLong("mapreduce.reduce.memory.mb", reduceMemoryMB);

      conf.setInt("mapreduce.map.cpu.vcores", 1);
      conf.setInt("mapreduce.reduce.cpu.vcores", 1);

      // 适中的缓冲区设置
      conf.setInt("mapreduce.task.io.sort.mb", (int) Math.min(512, mapMemoryMB / 4));
      conf.setFloat("mapreduce.task.io.sort.spill.percent", 0.8f);
      conf.setInt("mapreduce.task.io.sort.factor", 32);

      return conf;
    }
  }
}
```

理解这些优化技术和方法，对于在生产环境中运维高性能Hadoop集群具有重要意义。随着数据规模的不断增长，性能优化将继续是Hadoop运维的重要课题。
