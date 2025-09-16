# 第12章：MapReduce任务调度与数据处理

## 12.1 引言

MapReduce任务调度是整个框架性能的关键所在。一个高效的调度策略不仅能够最大化集群资源利用率，还能显著提升作业执行效率。MapReduce的任务调度涉及多个层面：从作业级别的资源分配，到任务级别的节点选择，再到数据处理过程中的优化策略。

在YARN架构下，MapReduce的任务调度变得更加复杂和灵活。MRAppMaster需要与ResourceManager协商获取容器资源，同时还要考虑数据本地性、负载均衡、容错恢复等多个因素。此外，MapReduce框架还需要处理大规模数据的分片、排序、合并等操作，这些都对系统性能产生重要影响。

本章将深入分析MapReduce的任务调度算法、数据分片策略、排序合并机制以及各种性能优化技术。通过对这些核心机制的理解，读者将能够更好地优化MapReduce应用的性能，并为大规模数据处理提供更高效的解决方案。

## 12.2 MapReduce任务调度算法

### 12.2.1 任务调度器架构

MapReduce的任务调度器负责决定何时、在哪里执行任务：

```java
public abstract class TaskScheduler extends AbstractService 
    implements EventHandler<TaskSchedulerEvent> {

  protected final AppContext appContext;
  protected final Clock clock;
  protected final EventHandler eventHandler;

  // Container allocation and management
  protected ContainerAllocator containerAllocator;
  protected ContainerLauncher containerLauncher;
  
  // Task tracking
  protected final Map<TaskId, Task> tasks = new ConcurrentHashMap<>();
  protected final Map<TaskAttemptId, TaskAttempt> taskAttempts = new ConcurrentHashMap<>();
  
  // Scheduling policies
  protected final SchedulingPolicy mapSchedulingPolicy;
  protected final SchedulingPolicy reduceSchedulingPolicy;
  
  // Resource management
  protected final ResourceCalculator resourceCalculator;
  protected final Resource clusterResource;
  protected final Resource maxContainerCapability;

  @Override
  public void handle(TaskSchedulerEvent event) {
    switch (event.getType()) {
      case TS_SCHEDULE:
        scheduleTask((TaskSchedulerTaskEvent) event);
        break;
      case TS_REQUEUE:
        requeueTask((TaskSchedulerTaskEvent) event);
        break;
      case TS_DONE:
        taskCompleted((TaskSchedulerTaskEvent) event);
        break;
      default:
        throw new YarnRuntimeException("Unknown event type: " + event.getType());
    }
  }

  protected abstract void scheduleTask(TaskSchedulerTaskEvent event);
  
  protected void scheduleMapTasks() {
    // Get pending map tasks
    List<Task> pendingMaps = getPendingTasks(TaskType.MAP);
    
    // Sort by priority and data locality
    Collections.sort(pendingMaps, new TaskComparator());
    
    for (Task task : pendingMaps) {
      if (canScheduleTask(task)) {
        Container container = selectContainer(task);
        if (container != null) {
          assignTaskToContainer(task, container);
        }
      }
    }
  }
  
  protected void scheduleReduceTasks() {
    // Only schedule reduce tasks after sufficient map progress
    float mapProgress = getMapProgress();
    if (mapProgress < getSlowStartThreshold()) {
      return;
    }
    
    List<Task> pendingReduces = getPendingTasks(TaskType.REDUCE);
    Collections.sort(pendingReduces, new TaskComparator());
    
    for (Task task : pendingReduces) {
      if (canScheduleTask(task)) {
        Container container = selectContainer(task);
        if (container != null) {
          assignTaskToContainer(task, container);
        }
      }
    }
  }
}
```

**调度器核心功能**：
- **任务排队管理**：维护待调度任务队列
- **资源匹配**：将任务与可用容器进行匹配
- **本地性优化**：优先选择数据本地的节点
- **负载均衡**：避免某些节点过载

### 12.2.2 数据本地性调度

数据本地性是MapReduce调度的核心考虑因素：

```java
public class LocalityScheduler {
  
  public enum LocalityLevel {
    NODE_LOCAL,    // 节点本地性
    RACK_LOCAL,    // 机架本地性  
    OFF_SWITCH     // 跨机架
  }
  
  private final Map<String, Set<String>> rackToNodes;
  private final Map<String, String> nodeToRack;
  private final NetworkTopology networkTopology;
  
  public LocalityLevel getLocalityLevel(TaskAttempt taskAttempt, Container container) {
    String containerNode = container.getNodeId().getHost();
    String containerRack = nodeToRack.get(containerNode);
    
    // Check for node locality
    if (hasDataOnNode(taskAttempt, containerNode)) {
      return LocalityLevel.NODE_LOCAL;
    }
    
    // Check for rack locality
    if (hasDataOnRack(taskAttempt, containerRack)) {
      return LocalityLevel.RACK_LOCAL;
    }
    
    // Off-switch
    return LocalityLevel.OFF_SWITCH;
  }
  
  public List<Container> selectContainersForTask(Task task, 
                                                List<Container> availableContainers) {
    List<Container> nodeLocalContainers = new ArrayList<>();
    List<Container> rackLocalContainers = new ArrayList<>();
    List<Container> offSwitchContainers = new ArrayList<>();
    
    // Classify containers by locality level
    for (Container container : availableContainers) {
      LocalityLevel level = getLocalityLevel(task.getAttempts().values().iterator().next(), 
                                           container);
      switch (level) {
        case NODE_LOCAL:
          nodeLocalContainers.add(container);
          break;
        case RACK_LOCAL:
          rackLocalContainers.add(container);
          break;
        case OFF_SWITCH:
          offSwitchContainers.add(container);
          break;
      }
    }
    
    // Return containers in order of preference
    List<Container> result = new ArrayList<>();
    result.addAll(nodeLocalContainers);
    result.addAll(rackLocalContainers);
    result.addAll(offSwitchContainers);
    
    return result;
  }
  
  private boolean hasDataOnNode(TaskAttempt taskAttempt, String node) {
    if (taskAttempt.getTask().getType() == TaskType.MAP) {
      InputSplit split = ((MapTask) taskAttempt.getTask()).getInputSplit();
      String[] locations = split.getLocations();
      for (String location : locations) {
        if (location.equals(node)) {
          return true;
        }
      }
    }
    return false;
  }
  
  private boolean hasDataOnRack(TaskAttempt taskAttempt, String rack) {
    if (taskAttempt.getTask().getType() == TaskType.MAP) {
      InputSplit split = ((MapTask) taskAttempt.getTask()).getInputSplit();
      String[] locations = split.getLocations();
      for (String location : locations) {
        String nodeRack = nodeToRack.get(location);
        if (rack.equals(nodeRack)) {
          return true;
        }
      }
    }
    return false;
  }
  
  public void updateLocalityStatistics(TaskAttempt taskAttempt, Container container) {
    LocalityLevel level = getLocalityLevel(taskAttempt, container);
    
    // Update metrics
    switch (level) {
      case NODE_LOCAL:
        incrementCounter("NODE_LOCAL_TASKS");
        break;
      case RACK_LOCAL:
        incrementCounter("RACK_LOCAL_TASKS");
        break;
      case OFF_SWITCH:
        incrementCounter("OFF_SWITCH_TASKS");
        break;
    }
    
    LOG.info("Task " + taskAttempt.getID() + " scheduled with " + level + 
             " locality on " + container.getNodeId());
  }
}
```

**本地性调度策略**：
- **节点本地性**：任务在数据所在节点执行
- **机架本地性**：任务在数据所在机架的其他节点执行
- **跨机架调度**：任务在不同机架的节点执行
- **本地性统计**：跟踪和优化本地性比例

### 12.2.3 容量感知调度

调度器需要考虑容器的资源容量：

```java
public class CapacityAwareScheduler extends TaskScheduler {
  
  private final ResourceCalculator resourceCalculator;
  private final Resource maxContainerCapability;
  private final Map<TaskType, Resource> taskResourceRequirements;
  
  @Override
  protected void scheduleTask(TaskSchedulerTaskEvent event) {
    Task task = event.getTask();
    TaskType taskType = task.getType();
    
    // Get resource requirements for this task type
    Resource required = taskResourceRequirements.get(taskType);
    
    // Find suitable containers
    List<Container> suitableContainers = findSuitableContainers(required);
    
    if (!suitableContainers.isEmpty()) {
      // Select best container based on locality and capacity
      Container selectedContainer = selectBestContainer(task, suitableContainers);
      
      if (selectedContainer != null) {
        // Assign task to container
        assignTaskToContainer(task, selectedContainer);
        
        LOG.info("Assigned " + taskType + " task " + task.getID() + 
                 " to container " + selectedContainer.getId() + 
                 " on node " + selectedContainer.getNodeId());
      }
    } else {
      // No suitable containers available, request new ones
      requestContainer(required, task);
    }
  }
  
  private List<Container> findSuitableContainers(Resource required) {
    List<Container> suitable = new ArrayList<>();
    
    for (Container container : availableContainers) {
      if (resourceCalculator.fitsIn(required, container.getResource())) {
        suitable.add(container);
      }
    }
    
    return suitable;
  }
  
  private Container selectBestContainer(Task task, List<Container> containers) {
    // Score containers based on multiple factors
    Map<Container, Double> scores = new HashMap<>();
    
    for (Container container : containers) {
      double score = calculateContainerScore(task, container);
      scores.put(container, score);
    }
    
    // Return container with highest score
    return Collections.max(scores.entrySet(), 
                          Map.Entry.comparingByValue()).getKey();
  }
  
  private double calculateContainerScore(Task task, Container container) {
    double score = 0.0;
    
    // Locality score (higher is better)
    LocalityLevel locality = getLocalityLevel(task, container);
    switch (locality) {
      case NODE_LOCAL:
        score += 100.0;
        break;
      case RACK_LOCAL:
        score += 50.0;
        break;
      case OFF_SWITCH:
        score += 10.0;
        break;
    }
    
    // Resource utilization score
    Resource required = getTaskResourceRequirement(task);
    Resource available = container.getResource();
    
    double cpuUtilization = (double) required.getVirtualCores() / 
                           available.getVirtualCores();
    double memUtilization = (double) required.getMemorySize() / 
                           available.getMemorySize();
    
    // Prefer higher utilization (less waste)
    score += (cpuUtilization + memUtilization) * 25.0;
    
    // Node load score (prefer less loaded nodes)
    int nodeLoad = getNodeLoad(container.getNodeId());
    score -= nodeLoad * 5.0;
    
    return score;
  }
  
  private void requestContainer(Resource required, Task task) {
    // Create container request with locality preferences
    List<String> preferredNodes = getPreferredNodes(task);
    List<String> preferredRacks = getPreferredRacks(task);
    
    ContainerRequest request = new ContainerRequest(
        required,
        preferredNodes.toArray(new String[0]),
        preferredRacks.toArray(new String[0]),
        Priority.newInstance(getPriority(task)),
        true, // relaxLocality
        null  // nodeLabelsExpression
    );
    
    // Submit request to ResourceManager
    containerAllocator.addContainerRequest(request);
    
    LOG.info("Requested container for " + task.getType() + " task " + 
             task.getID() + " with resource " + required);
  }
}
```

**容量感知特性**：
- **资源匹配**：确保容器资源满足任务需求
- **利用率优化**：最大化容器资源利用率
- **负载均衡**：避免节点过载
- **动态请求**：根据需要动态申请容器

## 12.3 数据分片策略与优化

### 12.3.1 输入分片算法

合理的数据分片是MapReduce性能的基础：

```java
public class OptimizedFileInputFormat<K, V> extends FileInputFormat<K, V> {
  
  private static final double SPLIT_SLOP = 1.1; // 10% slop
  
  @Override
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);
    
    // Collect all input files
    List<FileStatus> files = listStatus(job);
    
    // Calculate total input size
    long totalSize = 0;
    for (FileStatus file : files) {
      totalSize += file.getLen();
    }
    
    // Determine optimal split size
    long optimalSplitSize = calculateOptimalSplitSize(job, totalSize, files.size());
    
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    for (FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();
      
      if (length != 0) {
        BlockLocation[] blkLocations = getFileBlockLocations(file, 0, length);
        
        if (isSplitable(job, path)) {
          long splitSize = Math.max(minSize, Math.min(maxSize, optimalSplitSize));
          
          // Align splits with block boundaries when possible
          splits.addAll(createAlignedSplits(path, length, splitSize, blkLocations));
        } else {
          // Non-splitable file
          splits.add(makeSplit(path, 0, length, 
                              blkLocations[0].getHosts(),
                              blkLocations[0].getCachedHosts()));
        }
      }
    }
    
    // Optimize small splits
    splits = optimizeSmallSplits(splits, minSize);
    
    LOG.info("Total # of splits generated: " + splits.size() + 
             ", Total input size: " + totalSize + 
             ", Average split size: " + (totalSize / splits.size()) +
             ", Time taken: " + sw.now(TimeUnit.MILLISECONDS) + "ms");
    
    return splits;
  }
  
  private long calculateOptimalSplitSize(JobContext job, long totalSize, int numFiles) {
    Configuration conf = job.getConfiguration();
    
    // Get cluster capacity
    int maxMapTasks = conf.getInt("mapreduce.job.maps", 200);
    long blockSize = conf.getLong("dfs.blocksize", 128 * 1024 * 1024);
    
    // Calculate based on total size and desired parallelism
    long targetSplitSize = totalSize / maxMapTasks;
    
    // Align with block size
    long alignedSplitSize = ((targetSplitSize + blockSize - 1) / blockSize) * blockSize;
    
    // Ensure reasonable bounds
    long minSplitSize = Math.max(blockSize / 4, 16 * 1024 * 1024); // 16MB minimum
    long maxSplitSize = blockSize * 4; // 4 blocks maximum
    
    return Math.max(minSplitSize, Math.min(maxSplitSize, alignedSplitSize));
  }
  
  private List<InputSplit> createAlignedSplits(Path path, long length, 
                                              long splitSize, 
                                              BlockLocation[] blkLocations) 
      throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    
    long bytesRemaining = length;
    long offset = 0;
    
    while (bytesRemaining > 0) {
      // Calculate actual split size
      long actualSplitSize = Math.min(splitSize, bytesRemaining);
      
      // Adjust split size to avoid very small remainder
      if (bytesRemaining > actualSplitSize && 
          bytesRemaining < actualSplitSize * SPLIT_SLOP) {
        actualSplitSize = bytesRemaining;
      }
      
      // Find block locations for this split
      int blkIndex = getBlockIndex(blkLocations, offset);
      
      // Create split
      splits.add(makeSplit(path, offset, actualSplitSize,
                          blkLocations[blkIndex].getHosts(),
                          blkLocations[blkIndex].getCachedHosts()));
      
      offset += actualSplitSize;
      bytesRemaining -= actualSplitSize;
    }
    
    return splits;
  }
  
  private List<InputSplit> optimizeSmallSplits(List<InputSplit> splits, long minSize) {
    List<InputSplit> optimized = new ArrayList<InputSplit>();
    List<InputSplit> smallSplits = new ArrayList<InputSplit>();
    
    // Separate small and normal splits
    for (InputSplit split : splits) {
      if (split.getLength() < minSize) {
        smallSplits.add(split);
      } else {
        optimized.add(split);
      }
    }
    
    // Combine small splits when possible
    if (!smallSplits.isEmpty()) {
      optimized.addAll(combineSmallSplits(smallSplits, minSize));
    }
    
    return optimized;
  }
  
  private List<InputSplit> combineSmallSplits(List<InputSplit> smallSplits, long minSize) {
    List<InputSplit> combined = new ArrayList<InputSplit>();
    
    // Group splits by location for better locality
    Map<String, List<InputSplit>> locationGroups = groupSplitsByLocation(smallSplits);
    
    for (List<InputSplit> group : locationGroups.values()) {
      combined.addAll(combineGroup(group, minSize));
    }
    
    return combined;
  }
}
```

**分片优化策略**：
- **大小优化**：避免过小或过大的分片
- **边界对齐**：与HDFS块边界对齐
- **本地性保持**：保持数据本地性
- **小文件合并**：合并小文件减少任务数

### 12.3.2 自适应分片策略

根据集群状态动态调整分片策略：

```java
public class AdaptiveSplitStrategy {
  
  private final ClusterMetrics clusterMetrics;
  private final JobHistory jobHistory;
  private final Configuration conf;
  
  public SplitConfiguration calculateOptimalSplitConfig(JobContext job) {
    // Analyze cluster current state
    ClusterState clusterState = analyzeClusterState();
    
    // Analyze job characteristics
    JobCharacteristics jobChars = analyzeJobCharacteristics(job);
    
    // Get historical performance data
    HistoricalData histData = getHistoricalData(job);
    
    // Calculate optimal configuration
    return calculateConfiguration(clusterState, jobChars, histData);
  }
  
  private ClusterState analyzeClusterState() {
    ClusterState state = new ClusterState();
    
    // Get cluster capacity
    state.totalNodes = clusterMetrics.getNumActiveNMs();
    state.totalMemory = clusterMetrics.getAvailableMB();
    state.totalCores = clusterMetrics.getAvailableVirtualCores();
    
    // Get current load
    state.runningApplications = clusterMetrics.getNumActiveApplications();
    state.pendingApplications = clusterMetrics.getNumPendingApplications();
    
    // Calculate available capacity
    state.availableCapacity = calculateAvailableCapacity();
    
    return state;
  }
  
  private JobCharacteristics analyzeJobCharacteristics(JobContext job) {
    JobCharacteristics chars = new JobCharacteristics();
    
    // Analyze input data
    chars.totalInputSize = calculateTotalInputSize(job);
    chars.numInputFiles = countInputFiles(job);
    chars.avgFileSize = chars.totalInputSize / chars.numInputFiles;
    
    // Analyze processing complexity
    chars.mapperClass = job.getMapperClass();
    chars.reducerClass = job.getReducerClass();
    chars.numReduceTasks = job.getNumReduceTasks();
    
    // Estimate resource requirements
    chars.estimatedMapMemory = estimateMapMemoryRequirement(job);
    chars.estimatedReduceMemory = estimateReduceMemoryRequirement(job);
    
    return chars;
  }
  
  private SplitConfiguration calculateConfiguration(ClusterState cluster,
                                                   JobCharacteristics job,
                                                   HistoricalData history) {
    SplitConfiguration config = new SplitConfiguration();
    
    // Calculate target parallelism
    int targetMapTasks = calculateTargetMapTasks(cluster, job);
    
    // Calculate split size
    config.targetSplitSize = job.totalInputSize / targetMapTasks;
    
    // Adjust based on cluster capacity
    config.targetSplitSize = adjustForClusterCapacity(config.targetSplitSize, cluster);
    
    // Adjust based on historical performance
    if (history != null) {
      config.targetSplitSize = adjustForHistoricalPerformance(config.targetSplitSize, history);
    }
    
    // Set bounds
    config.minSplitSize = Math.max(16 * 1024 * 1024, config.targetSplitSize / 4);
    config.maxSplitSize = Math.min(1024 * 1024 * 1024, config.targetSplitSize * 4);
    
    // Calculate other parameters
    config.combineSmallFiles = shouldCombineSmallFiles(job);
    config.alignWithBlocks = shouldAlignWithBlocks(job);
    
    return config;
  }
  
  private int calculateTargetMapTasks(ClusterState cluster, JobCharacteristics job) {
    // Base calculation on available capacity
    int maxPossibleMaps = cluster.availableCapacity / job.estimatedMapMemory;
    
    // Consider data size
    int dataDrivenMaps = (int) (job.totalInputSize / (128 * 1024 * 1024)); // 128MB per map
    
    // Consider file count
    int fileDrivenMaps = Math.min(job.numInputFiles, maxPossibleMaps);
    
    // Use the most appropriate strategy
    if (job.avgFileSize < 64 * 1024 * 1024) {
      // Small files - use file-driven approach
      return Math.min(fileDrivenMaps, maxPossibleMaps);
    } else {
      // Large files - use data-driven approach
      return Math.min(dataDrivenMaps, maxPossibleMaps);
    }
  }
}
```

**自适应特性**：
- **集群感知**：根据集群当前状态调整
- **作业特征分析**：分析作业的数据和计算特征
- **历史数据利用**：基于历史性能数据优化
- **动态调整**：运行时动态调整策略

## 12.4 排序合并机制

### 12.4.1 外部排序算法

MapReduce使用外部排序处理大规模数据：

```java
public class ExternalSorter<K, V> {
  
  private final RawComparator<K> comparator;
  private final Class<K> keyClass;
  private final Class<V> valueClass;
  private final Configuration conf;
  private final int sortFactor;
  private final long sortBufferSize;
  
  public void sort(List<Path> inputFiles, Path outputFile) throws IOException {
    // Phase 1: Create sorted runs
    List<Path> sortedRuns = createSortedRuns(inputFiles);
    
    // Phase 2: Merge sorted runs
    mergeSortedRuns(sortedRuns, outputFile);
  }
  
  private List<Path> createSortedRuns(List<Path> inputFiles) throws IOException {
    List<Path> runs = new ArrayList<Path>();
    
    for (Path inputFile : inputFiles) {
      runs.addAll(createRunsFromFile(inputFile));
    }
    
    return runs;
  }
  
  private List<Path> createRunsFromFile(Path inputFile) throws IOException {
    List<Path> runs = new ArrayList<Path>();
    
    SequenceFile.Reader reader = new SequenceFile.Reader(conf, 
        SequenceFile.Reader.file(inputFile));
    
    List<KeyValue<K, V>> buffer = new ArrayList<KeyValue<K, V>>();
    long bufferSize = 0;
    
    K key = ReflectionUtils.newInstance(keyClass, conf);
    V value = ReflectionUtils.newInstance(valueClass, conf);
    
    try {
      while (reader.next(key, value)) {
        KeyValue<K, V> kv = new KeyValue<K, V>(
            ReflectionUtils.copy(conf, key, ReflectionUtils.newInstance(keyClass, conf)),
            ReflectionUtils.copy(conf, value, ReflectionUtils.newInstance(valueClass, conf))
        );
        
        buffer.add(kv);
        bufferSize += getSerializedSize(kv);
        
        if (bufferSize >= sortBufferSize) {
          // Sort and write run
          Path runFile = writeRun(buffer);
          runs.add(runFile);
          
          buffer.clear();
          bufferSize = 0;
        }
        
        key = ReflectionUtils.newInstance(keyClass, conf);
        value = ReflectionUtils.newInstance(valueClass, conf);
      }
      
      // Write final run
      if (!buffer.isEmpty()) {
        Path runFile = writeRun(buffer);
        runs.add(runFile);
      }
      
    } finally {
      reader.close();
    }
    
    return runs;
  }
  
  private Path writeRun(List<KeyValue<K, V>> buffer) throws IOException {
    // Sort buffer in memory
    Collections.sort(buffer, new Comparator<KeyValue<K, V>>() {
      @Override
      public int compare(KeyValue<K, V> o1, KeyValue<K, V> o2) {
        return comparator.compare(o1.getKey(), o2.getKey());
      }
    });
    
    // Write sorted run to disk
    Path runFile = new Path(getTempDir(), "run_" + UUID.randomUUID().toString());
    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(runFile),
        SequenceFile.Writer.keyClass(keyClass),
        SequenceFile.Writer.valueClass(valueClass),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK));
    
    try {
      for (KeyValue<K, V> kv : buffer) {
        writer.append(kv.getKey(), kv.getValue());
      }
    } finally {
      writer.close();
    }
    
    return runFile;
  }
  
  private void mergeSortedRuns(List<Path> runs, Path outputFile) throws IOException {
    if (runs.size() == 1) {
      // Only one run, just rename it
      FileSystem fs = outputFile.getFileSystem(conf);
      fs.rename(runs.get(0), outputFile);
      return;
    }
    
    // Multi-way merge
    while (runs.size() > 1) {
      List<Path> nextLevelRuns = new ArrayList<Path>();
      
      // Merge runs in groups of sortFactor
      for (int i = 0; i < runs.size(); i += sortFactor) {
        int end = Math.min(i + sortFactor, runs.size());
        List<Path> group = runs.subList(i, end);
        
        Path mergedRun = mergeGroup(group);
        nextLevelRuns.add(mergedRun);
      }
      
      runs = nextLevelRuns;
    }
    
    // Final result
    FileSystem fs = outputFile.getFileSystem(conf);
    fs.rename(runs.get(0), outputFile);
  }
  
  private Path mergeGroup(List<Path> group) throws IOException {
    if (group.size() == 1) {
      return group.get(0);
    }
    
    // Create priority queue for merge
    PriorityQueue<MergeEntry<K, V>> pq = new PriorityQueue<MergeEntry<K, V>>(
        group.size(), new Comparator<MergeEntry<K, V>>() {
          @Override
          public int compare(MergeEntry<K, V> o1, MergeEntry<K, V> o2) {
            return comparator.compare(o1.getKey(), o2.getKey());
          }
        });
    
    // Initialize readers
    List<SequenceFile.Reader> readers = new ArrayList<SequenceFile.Reader>();
    for (Path path : group) {
      SequenceFile.Reader reader = new SequenceFile.Reader(conf,
          SequenceFile.Reader.file(path));
      readers.add(reader);
      
      // Read first record from each reader
      K key = ReflectionUtils.newInstance(keyClass, conf);
      V value = ReflectionUtils.newInstance(valueClass, conf);
      
      if (reader.next(key, value)) {
        pq.offer(new MergeEntry<K, V>(key, value, reader, readers.size() - 1));
      }
    }
    
    // Create output file
    Path outputPath = new Path(getTempDir(), "merged_" + UUID.randomUUID().toString());
    SequenceFile.Writer writer = SequenceFile.createWriter(conf,
        SequenceFile.Writer.file(outputPath),
        SequenceFile.Writer.keyClass(keyClass),
        SequenceFile.Writer.valueClass(valueClass),
        SequenceFile.Writer.compression(SequenceFile.CompressionType.BLOCK));
    
    try {
      // Merge process
      while (!pq.isEmpty()) {
        MergeEntry<K, V> entry = pq.poll();
        writer.append(entry.getKey(), entry.getValue());
        
        // Read next record from the same reader
        K nextKey = ReflectionUtils.newInstance(keyClass, conf);
        V nextValue = ReflectionUtils.newInstance(valueClass, conf);
        
        if (entry.getReader().next(nextKey, nextValue)) {
          pq.offer(new MergeEntry<K, V>(nextKey, nextValue, 
                                       entry.getReader(), entry.getReaderIndex()));
        }
      }
    } finally {
      writer.close();
      
      // Close all readers
      for (SequenceFile.Reader reader : readers) {
        reader.close();
      }
    }
    
    return outputPath;
  }
}
```

**外部排序特性**：
- **内存管理**：控制内存使用量避免OOM
- **分阶段处理**：分为排序和合并两个阶段
- **多路归并**：支持多路归并提高效率
- **压缩优化**：使用压缩减少I/O开销

### 12.4.2 Combiner优化

Combiner可以显著减少Shuffle阶段的数据传输量：

```java
public class CombinerRunner<K, V> {
  
  private final Class<? extends Reducer> combinerClass;
  private final JobConf job;
  private final TaskReporter reporter;
  private final Counters.Counter combineInputCounter;
  private final Counters.Counter combineOutputCounter;
  
  public void combine(RawKeyValueIterator kvIter, 
                     OutputCollector<K, V> output) throws IOException {
    
    // Create combiner instance
    Reducer<K, V, K, V> combiner = 
        (Reducer<K, V, K, V>) ReflectionUtils.newInstance(combinerClass, job);
    
    Class<K> keyClass = (Class<K>) job.getMapOutputKeyClass();
    Class<V> valueClass = (Class<V>) job.getMapOutputValueClass();
    RawComparator<K> comparator = job.getOutputKeyComparator();
    
    try {
      CombineValuesIterator<K, V> values = 
          new CombineValuesIterator<K, V>(kvIter, comparator, keyClass, valueClass, job, reporter);
      
      while (values.more()) {
        combiner.reduce(values.getKey(), values, output, reporter);
        values.nextKey();
        
        // Update progress
        reporter.progress();
      }
    } finally {
      combiner.close();
    }
  }
  
  public boolean shouldRunCombiner(int numRecords, long dataSize) {
    // Only run combiner if there's sufficient data
    int minRecords = job.getInt("mapreduce.map.combine.minspills", 3);
    long minSize = job.getLong("mapreduce.map.combine.minsize", 1024 * 1024); // 1MB
    
    return numRecords >= minRecords || dataSize >= minSize;
  }
  
  private static class CombineValuesIterator<K, V> implements Iterator<V> {
    
    private final RawKeyValueIterator in;
    private final RawComparator<K> comparator;
    private final Class<K> keyClass;
    private final Class<V> valueClass;
    private final Configuration conf;
    private final TaskReporter reporter;
    
    private K key;
    private K nextKey;
    private V value;
    private boolean hasNext = false;
    private boolean more = true;
    
    public CombineValuesIterator(RawKeyValueIterator in,
                                RawComparator<K> comparator,
                                Class<K> keyClass,
                                Class<V> valueClass,
                                Configuration conf,
                                TaskReporter reporter) throws IOException {
      this.in = in;
      this.comparator = comparator;
      this.keyClass = keyClass;
      this.valueClass = valueClass;
      this.conf = conf;
      this.reporter = reporter;
      
      readNextKey();
      key = nextKey;
      nextKey = null;
      hasNext = more;
    }
    
    public boolean more() {
      return more;
    }
    
    public K getKey() {
      return key;
    }
    
    public void nextKey() throws IOException {
      // Skip remaining values for current key
      while (hasNext()) {
        next();
      }
      
      // Move to next key
      if (more) {
        key = nextKey;
        nextKey = null;
        hasNext = more;
      }
    }
    
    @Override
    public boolean hasNext() {
      return hasNext;
    }
    
    @Override
    public V next() {
      if (!hasNext) {
        throw new NoSuchElementException("No more values");
      }
      
      V currentValue = value;
      
      try {
        readNextKey();
        hasNext = more && (comparator.compare(key, nextKey) == 0);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
      
      return currentValue;
    }
    
    private void readNextKey() throws IOException {
      if (!in.next()) {
        more = false;
        return;
      }
      
      DataInputBuffer keyBuffer = in.getKey();
      DataInputBuffer valueBuffer = in.getValue();
      
      // Deserialize key and value
      nextKey = ReflectionUtils.newInstance(keyClass, conf);
      value = ReflectionUtils.newInstance(valueClass, conf);
      
      SerializationFactory serializationFactory = new SerializationFactory(conf);
      Deserializer<K> keyDeserializer = serializationFactory.getDeserializer(keyClass);
      Deserializer<V> valueDeserializer = serializationFactory.getDeserializer(valueClass);
      
      keyDeserializer.open(keyBuffer);
      valueDeserializer.open(valueBuffer);
      
      nextKey = keyDeserializer.deserialize(nextKey);
      value = valueDeserializer.deserialize(value);
      
      keyDeserializer.close();
      valueDeserializer.close();
    }
  }
}
```

**Combiner优化效果**：
- **数据量减少**：减少Map输出数据量
- **网络传输优化**：减少Shuffle阶段网络传输
- **磁盘I/O优化**：减少中间文件大小
- **内存使用优化**：减少Reduce端内存压力

## 12.5 本章小结

本章深入分析了MapReduce的任务调度算法、数据分片策略、排序合并机制等核心技术。这些机制的优化直接影响MapReduce应用的性能和资源利用率。

**核心要点**：

**任务调度**：基于数据本地性、容量感知和负载均衡的智能调度策略。

**数据分片**：自适应的分片策略，根据集群状态和作业特征动态优化。

**排序合并**：高效的外部排序算法和多路归并机制。

**Combiner优化**：通过本地聚合减少数据传输量。

**性能优化**：从多个维度优化MapReduce应用性能。

理解这些核心机制的实现原理，对于开发高性能的MapReduce应用和优化集群资源利用率具有重要意义。随着大数据处理需求的不断增长，这些优化技术将继续发挥重要作用。
