# 第11章：MapReduce框架与作业执行

## 11.1 引言

MapReduce是Google在2004年提出的分布式计算编程模型，后来成为Hadoop生态系统的核心计算框架。它通过简单而强大的编程抽象，让开发者能够轻松编写处理大规模数据集的并行程序，而无需关心分布式系统的复杂细节。

MapReduce的设计哲学体现了"分而治之"的思想：将大规模数据处理任务分解为两个阶段——Map阶段和Reduce阶段。Map阶段负责数据的过滤和转换，Reduce阶段负责数据的聚合和汇总。这种设计不仅简化了并行编程的复杂性，还提供了良好的容错性和可扩展性。

在YARN架构下，MapReduce作为一个应用框架运行，它实现了自己的ApplicationMaster（MRAppMaster），负责管理MapReduce作业的执行。MapReduce框架处理了数据分片、任务调度、容错恢复、数据传输等复杂问题，让开发者只需要专注于业务逻辑的实现。

本章将深入分析MapReduce框架的设计原理和实现机制，包括作业执行流程、任务调度、Shuffle机制、容错处理等核心内容，帮助读者全面理解这个经典的大数据处理框架。

## 11.2 MapReduce编程模型概述

### 11.2.1 Map和Reduce函数

MapReduce编程模型的核心是两个用户定义的函数：

```java
/**
 * Map function interface
 */
public abstract class Mapper<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Called once at the beginning of the task.
   */
  protected void setup(Context context) throws IOException, InterruptedException {
    // Default implementation does nothing
  }

  /**
   * Called once for each key/value pair in the input split.
   */
  public abstract void map(KEYIN key, VALUEIN value, Context context)
      throws IOException, InterruptedException;

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Default implementation does nothing
  }

  /**
   * Expert users can override this method for more complete control over the
   * execution of the Mapper.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      while (context.nextKeyValue()) {
        map(context.getCurrentKey(), context.getCurrentValue(), context);
      }
    } finally {
      cleanup(context);
    }
  }
}

/**
 * Reduce function interface
 */
public abstract class Reducer<KEYIN, VALUEIN, KEYOUT, VALUEOUT> {

  /**
   * Called once at the start of the task.
   */
  protected void setup(Context context) throws IOException, InterruptedException {
    // Default implementation does nothing
  }

  /**
   * This method is called once for each key. Most applications will define
   * their reduce class by overriding this method.
   */
  public abstract void reduce(KEYIN key, Iterable<VALUEIN> values, Context context)
      throws IOException, InterruptedException;

  /**
   * Called once at the end of the task.
   */
  protected void cleanup(Context context) throws IOException, InterruptedException {
    // Default implementation does nothing
  }

  /**
   * Advanced application writers can use the run method to exert greater
   * control on map processing.
   */
  public void run(Context context) throws IOException, InterruptedException {
    setup(context);
    try {
      while (context.nextKey()) {
        reduce(context.getCurrentKey(), context.getValues(), context);
        Iterator<VALUEIN> iter = context.getValues().iterator();
        if(iter instanceof ReduceContext.ValueIterator) {
          ((ReduceContext.ValueIterator<VALUEIN>)iter).resetBackupStore();        
        }
      }
    } finally {
      cleanup(context);
    }
  }
}
```

**Map函数特点**：
- 处理输入数据的每一个键值对
- 可以产生零个、一个或多个输出键值对
- 输出的键值对类型可以与输入不同
- 支持setup和cleanup方法进行初始化和清理

**Reduce函数特点**：
- 处理具有相同键的所有值
- 接收一个键和该键对应的所有值的迭代器
- 通常产生零个或一个输出键值对
- 支持setup和cleanup方法

### 11.2.2 数据流和执行模型

MapReduce的数据流遵循固定的模式：

```java
public class MapReduceDataFlow {
  
  /**
   * Input data is split into fixed-size pieces called input splits
   */
  public List<InputSplit> createInputSplits(JobContext job) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    
    for (FileStatus file : files) {
      Path path = file.getPath();
      long length = file.getLen();
      
      if (length != 0) {
        BlockLocation[] blkLocations = getFileBlockLocations(file, 0, length);
        
        long splitSize = computeSplitSize(length, minSize, maxSize);
        long bytesRemaining = length;
        
        while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
          splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                             blkLocations[blkIndex].getHosts(),
                             blkLocations[blkIndex].getCachedHosts()));
          bytesRemaining -= splitSize;
        }
        
        if (bytesRemaining != 0) {
          int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
          splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                             blkLocations[blkIndex].getHosts(),
                             blkLocations[blkIndex].getCachedHosts()));
        }
      }
    }
    
    return splits;
  }
  
  /**
   * Map phase: process each input split
   */
  public void executeMapPhase(InputSplit split, Mapper mapper, Context context) 
      throws IOException, InterruptedException {
    
    RecordReader<KEYIN, VALUEIN> input = createRecordReader(split, context);
    input.initialize(split, context);
    
    mapper.run(context);
    
    input.close();
  }
  
  /**
   * Shuffle phase: sort and partition map outputs
   */
  public void executeShufflePhase(MapOutputCollector collector) 
      throws IOException, InterruptedException {
    
    // Sort map outputs by key
    collector.flush();
    
    // Partition outputs for different reducers
    for (int partition = 0; partition < numReduceTasks; partition++) {
      writePartitionedOutput(partition, collector.getSpillFiles());
    }
  }
  
  /**
   * Reduce phase: process grouped data
   */
  public void executeReducePhase(int partition, Reducer reducer, Context context)
      throws IOException, InterruptedException {
    
    // Fetch map outputs for this partition
    ShuffleConsumerPlugin.Context shuffleContext = 
        new ShuffleConsumerPlugin.Context(taskAttemptID, job, fs, umbilical, 
                                         lDirAlloc, reporter, codec, 
                                         combinerClass, combineCollector, 
                                         spilledRecordsCounter, 
                                         reduceCombineInputCounter,
                                         shuffledMapsCounter,
                                         reduceShuffleBytes, failedShuffleCounter,
                                         mergedMapOutputsCounter,
                                         taskStatus, copyPhase, sortPhase, this,
                                         mapOutputFile, localMapFiles);
    
    shuffleConsumerPlugin.init(shuffleContext);
    shuffleConsumerPlugin.run();
    
    // Run reduce function
    reducer.run(context);
    
    shuffleConsumerPlugin.close();
  }
}
```

**执行阶段**：
1. **输入分片**：将输入数据分割为固定大小的分片
2. **Map阶段**：并行处理每个输入分片
3. **Shuffle阶段**：对Map输出进行排序和分区
4. **Reduce阶段**：处理分组后的数据

### 11.2.3 作业配置和提交

MapReduce作业通过Job类进行配置和提交：

```java
public class Job extends JobContextImpl implements JobContext {
  
  public static Job getInstance() throws IOException {
    return getInstance(new Configuration());
  }
  
  public static Job getInstance(Configuration conf) throws IOException {
    JobConf jobConf = new JobConf(conf);
    return new Job(jobConf);
  }
  
  /**
   * Set the Mapper for the job.
   */
  public void setMapperClass(Class<? extends Mapper> cls) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(MAP_CLASS_ATTR, cls, Mapper.class);
  }
  
  /**
   * Set the Reducer for the job.
   */
  public void setReducerClass(Class<? extends Reducer> cls) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setClass(REDUCE_CLASS_ATTR, cls, Reducer.class);
  }
  
  /**
   * Set the number of reduce tasks for the job.
   */
  public void setNumReduceTasks(int tasks) throws IllegalStateException {
    ensureState(JobState.DEFINE);
    conf.setInt(NUM_REDUCES, tasks);
  }
  
  /**
   * Submit the job to the cluster and return immediately.
   */
  public void submit() throws IOException, InterruptedException, ClassNotFoundException {
    ensureState(JobState.DEFINE);
    setUseNewAPI();
    connect();
    final JobSubmitter submitter = getJobSubmitter(cluster.getFileSystem(), cluster.getClient());
    status = ugi.doAs(new PrivilegedExceptionAction<JobStatus>() {
      public JobStatus run() throws IOException, InterruptedException, ClassNotFoundException {
        return submitter.submitJobInternal(Job.this, cluster);
      }
    });
    state = JobState.RUNNING;
  }
  
  /**
   * Submit the job to the cluster and wait for it to finish.
   */
  public boolean waitForCompletion(boolean verbose) throws IOException, InterruptedException,
                                                           ClassNotFoundException {
    if (state == JobState.DEFINE) {
      submit();
    }
    if (verbose) {
      monitorAndPrintJob();
    } else {
      int completionPollIntervalMillis = 
          Job.getCompletionPollInterval(cluster.getConf());
      while (!isComplete()) {
        try {
          Thread.sleep(completionPollIntervalMillis);
        } catch (InterruptedException ie) {
          // ignore
        }
      }
    }
    return isSuccessful();
  }
}
```

**作业配置要素**：
- Mapper和Reducer类
- 输入和输出路径
- 输入和输出格式
- 分区器和组合器
- 资源需求配置

## 11.3 MapReduce作业执行流程

### 11.3.1 作业提交过程

MapReduce作业的提交过程涉及多个步骤：

```java
public class JobSubmitter {
  
  JobStatus submitJobInternal(Job job, Cluster cluster) 
      throws ClassNotFoundException, InterruptedException, IOException {
    
    // Validate job configuration
    checkSpecs(job);
    
    Configuration conf = job.getConfiguration();
    addMRFrameworkToDistributedCache(conf);
    
    // Get staging directory
    Path jobStagingArea = JobSubmissionFiles.getStagingDir(cluster, conf);
    
    // Get job ID
    JobID jobId = submitClient.getNewJobID();
    job.setJobID(jobId);
    Path submitJobDir = new Path(jobStagingArea, jobId.toString());
    JobStatus status = null;
    
    try {
      conf.set(MRJobConfig.USER_NAME, UserGroupInformation.getCurrentUser().getShortUserName());
      conf.set("hadoop.http.filter.initializers", 
          "org.apache.hadoop.yarn.server.webproxy.amfilter.AmFilterInitializer");
      conf.set(MRJobConfig.MAPREDUCE_JOB_DIR, submitJobDir.toString());
      
      // Create job directory
      FileSystem fs = submitJobDir.getFileSystem(conf);
      fs.mkdirs(submitJobDir);
      
      // Copy job resources to staging directory
      copyAndConfigureFiles(job, submitJobDir);
      
      // Compute input splits
      int maps = writeSplits(job, submitJobDir);
      conf.setInt(MRJobConfig.NUM_MAPS, maps);
      
      // Write job configuration
      writeConf(conf, submitJobDir);
      
      // Submit job to ResourceManager
      status = submitClient.submitJob(jobId, submitJobDir.toString(), 
                                     job.getCredentials());
      
      if (status != null) {
        return status;
      } else {
        throw new IOException("Could not launch job");
      }
    } finally {
      if (status == null) {
        LOG.info("Cleaning up the staging area " + submitJobDir);
        if (fs != null && submitJobDir != null) {
          fs.delete(submitJobDir, true);
        }
      }
    }
  }
  
  private int writeSplits(JobContext job, Path jobSubmitDir) throws IOException,
      InterruptedException, ClassNotFoundException {
    Configuration conf = job.getConfiguration();
    InputFormat<?, ?> input = ReflectionUtils.newInstance(job.getInputFormatClass(), conf);
    
    List<InputSplit> splits = input.getSplits(job);
    T[] array = (T[]) splits.toArray(new InputSplit[splits.size()]);
    
    // Sort splits by size, largest first
    Arrays.sort(array, new SplitComparator());
    JobSplitWriter.createSplitFiles(jobSubmitDir, conf, 
                                   jobSubmitDir.getFileSystem(conf), array);
    return array.length;
  }
}
```

**提交步骤**：
1. **作业验证**：检查作业配置的有效性
2. **资源准备**：将作业相关文件复制到HDFS
3. **输入分片**：计算输入数据的分片信息
4. **作业提交**：向ResourceManager提交作业

### 11.3.2 MRAppMaster：MapReduce ApplicationMaster

MRAppMaster是MapReduce在YARN上的ApplicationMaster实现：

```java
public class MRAppMaster extends CompositeService {

  private Clock clock;
  private final long startTime;
  private final long appSubmitTime;
  private String appName;
  private final ApplicationAttemptId appAttemptID;
  private final ContainerId containerID;
  private final String nmHost;
  private final int nmPort;
  private final int nmHttpPort;
  protected final MRAppMetrics metrics;
  private Map<TaskId, TaskInfo> completedTasksFromPreviousRun;
  private List<AMInfo> amInfos;
  private AppContext context;
  private Dispatcher dispatcher;
  private ClientService clientService;
  private ContainerAllocator containerAllocator;
  private ContainerLauncher containerLauncher;
  private TaskCleaner taskCleaner;
  private Speculator speculator;
  private TaskAttemptListener taskAttemptListener;
  private JobTokenSecretManager jobTokenSecretManager;
  private JobId jobId;
  private boolean newApiCommitter;
  private OutputCommitter committer;
  private JobEventDispatcher jobEventDispatcher;
  private JobHistoryEventHandler jobHistoryEventHandler;
  private SpeculatorEventDispatcher speculatorEventDispatcher;

  public MRAppMaster(ApplicationAttemptId applicationAttemptId, 
                     ContainerId containerId, String nmHost, int nmPort, 
                     int nmHttpPort, long appSubmitTime) {
    this(applicationAttemptId, containerId, nmHost, nmPort, nmHttpPort,
         new SystemClock(), appSubmitTime);
  }

  @Override
  protected void serviceInit(Configuration conf) throws Exception {
    conf.setBoolean(Dispatcher.DISPATCHER_EXIT_ON_ERROR_KEY, true);

    context = new RunningAppContext(conf);

    // Job name is the same as the app name util we support DAG of jobs
    // for an app later
    appName = conf.get(MRJobConfig.JOB_NAME, "<missing app name>");

    conf.setInt(MRJobConfig.APPLICATION_ATTEMPT_ID, appAttemptID.getAttemptId());

    newApiCommitter = false;
    jobId = MRBuilderUtils.newJobId(appAttemptID.getApplicationId(), 
                                   appAttemptID.getApplicationId().getId());
    int numReduceTasks = conf.getInt(MRJobConfig.NUM_REDUCES, 0);
    if ((numReduceTasks > 0 && 
         conf.getBoolean("mapred.reducer.new-api", false)) ||
          (numReduceTasks == 0 && 
           conf.getBoolean("mapred.mapper.new-api", false)))  {
      newApiCommitter = true;
      LOG.info("Using mapred newApiCommitter.");
    }

    committer = createOutputCommitter(conf);
    boolean recoveryEnabled = conf.getBoolean(
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE,
        MRJobConfig.MR_AM_JOB_RECOVERY_ENABLE_DEFAULT);
    boolean recoverySupportedByCommitter = committer.isRecoverySupported();
    
    if (recoveryEnabled && recoverySupportedByCommitter
        && appAttemptID.getAttemptId() > 1) {
      LOG.info("Recovery is enabled. "
          + "Will try to recover from previous life on best effort basis.");
      recoveryServ = new RecoveryService(appAttemptID, clock);
      addIfService(recoveryServ);
      dispatcher = recoveryServ.getDispatcher();
      clock = recoveryServ.getClock();
      completedTasksFromPreviousRun = recoveryServ.getCompletedTasks();
      amInfos = recoveryServ.getAMInfos();
    } else {
      LOG.info("Not starting RecoveryService: recoveryEnabled: "
          + recoveryEnabled + " recoverySupportedByCommitter: "
          + recoverySupportedByCommitter + " ApplicationAttemptID: "
          + appAttemptID.getAttemptId());
      dispatcher = new AsyncDispatcher();
      addIfService(dispatcher);
    }

    //service to handle requests to TaskUmbilicalProtocol
    taskAttemptListener = createTaskAttemptListener(context, taskHeartbeatHandler,
        containerHeartbeatHandler);
    addIfService(taskAttemptListener);

    //service to handle requests from JobClient
    clientService = createClientService(context);
    addIfService(clientService);

    //service to handle the actual allocation of containers
    containerAllocator = createContainerAllocator(clientService, context);
    addIfService(containerAllocator);
    dispatcher.register(ContainerAllocator.EventType.class, containerAllocator);

    //service to handle the launching/stopping of containers
    containerLauncher = createContainerLauncher(context);
    addIfService(containerLauncher);
    dispatcher.register(ContainerLauncher.EventType.class, containerLauncher);

    //service to handle completed containers
    taskCleaner = createTaskCleaner(context);
    addIfService(taskCleaner);
    dispatcher.register(TaskCleaner.EventType.class, taskCleaner);

    //service to handle speculation
    speculator = createSpeculator(conf, context);
    addIfService(speculator);

    speculatorEventDispatcher = new SpeculatorEventDispatcher(conf);
    dispatcher.register(Speculator.EventType.class, speculatorEventDispatcher);

    // service to log job history events
    EventHandler<JobHistoryEvent> historyService = 
        createJobHistoryHandler(context);
    dispatcher.register(org.apache.hadoop.mapreduce.jobhistory.EventType.class,
        historyService);

    this.jobEventDispatcher = new JobEventDispatcher();

    //register the event dispatchers
    dispatcher.register(JobEventType.class, jobEventDispatcher);
    dispatcher.register(TaskEventType.class, new TaskEventDispatcher());
    dispatcher.register(TaskAttemptEventType.class, 
        new TaskAttemptEventDispatcher());

    if (conf.getBoolean(MRJobConfig.MAP_SPECULATIVE, false)
        || conf.getBoolean(MRJobConfig.REDUCE_SPECULATIVE, false)) {
      //optional service to speculate on task attempts' progress
      addIfService(speculator);
    }

    speculatorEventDispatcher.init(conf);

    // Add the JobHistoryEventHandler last so that it is properly stopped first.
    // This will guarantee that all history-events are flushed before AM goes
    // down.
    addIfService(historyService);
    super.serviceInit(conf);
  }
}
```

**MRAppMaster核心组件**：
- **ContainerAllocator**：负责向ResourceManager申请容器
- **ContainerLauncher**：负责启动和停止容器
- **TaskAttemptListener**：处理任务的心跳和状态更新
- **ClientService**：处理客户端请求
- **Speculator**：处理推测执行
- **JobHistoryEventHandler**：记录作业历史事件

## 11.4 Map任务执行机制

### 11.4.1 MapTask的核心实现

MapTask负责执行Map阶段的处理逻辑：

```java
public class MapTask extends Task {
  
  private TaskSplitIndex splitMetaInfo = new TaskSplitIndex();
  private final static int APPROX_HEADER_LENGTH = 150;

  @Override
  public void run(final JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, ClassNotFoundException, InterruptedException {
    
    this.umbilical = umbilical;

    if (isMapTask()) {
      // If there are no reducers then there won't be any sort. Hence the map 
      // phase will govern the entire attempt's progress.
      if (conf.getNumReduceTasks() == 0) {
        mapPhase = getProgress().addPhase("map", 1.0f);
      } else {
        // If there are reducers then the entire attempt's progress will be 
        // split between the map phase (67%) and the sort phase (33%).
        mapPhase = getProgress().addPhase("map", 0.667f);
        sortPhase  = getProgress().addPhase("sort", 0.333f);
      }
    }
    
    TaskReporter reporter = startReporter(umbilical);
 
    boolean useNewApi = job.getUseNewMapper();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    if (useNewApi) {
      runNewMapper(job, splitMetaInfo, umbilical, reporter);
    } else {
      runOldMapper(job, splitMetaInfo, umbilical, reporter);
    }
    done(umbilical, reporter);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewMapper(final JobConf job,
                    final TaskSplitIndex splitIndex,
                    final TaskUmbilicalProtocol umbilical,
                    TaskReporter reporter
                    ) throws IOException, ClassNotFoundException,
                             InterruptedException {
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, 
                                                                  getTaskID(),
                                                                  reporter);

    // make a mapper
    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE> mapper =
      (org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getMapperClass(), job);

    // make the input format
    org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE> inputFormat =
      (org.apache.hadoop.mapreduce.InputFormat<INKEY,INVALUE>)
        ReflectionUtils.newInstance(taskContext.getInputFormatClass(), job);

    // rebuild the input split
    org.apache.hadoop.mapreduce.InputSplit split = null;
    split = getSplitDetails(new Path(splitIndex.getSplitLocation()),
        splitIndex.getStartOffset());

    LOG.info("Processing split: " + split);

    org.apache.hadoop.mapreduce.RecordReader<INKEY,INVALUE> input =
      new NewTrackingRecordReader<INKEY,INVALUE>
        (split, inputFormat, reporter, taskContext);
    
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    org.apache.hadoop.mapreduce.RecordWriter output = null;
    
    // get an output object
    if (job.getNumReduceTasks() == 0) {
      output = 
        new NewDirectOutputCollector(taskContext, job, umbilical, reporter);
    } else {
      output = new NewOutputCollector(taskContext, job, umbilical, reporter);
    }

    org.apache.hadoop.mapreduce.MapContext<INKEY, INVALUE, OUTKEY, OUTVALUE> 
    mapContext = 
      new MapContextImpl<INKEY, INVALUE, OUTKEY, OUTVALUE>(job, getTaskID(), 
          input, output, 
          committer, 
          reporter, split);

    org.apache.hadoop.mapreduce.Mapper<INKEY,INVALUE,OUTKEY,OUTVALUE>.Context 
        mapperContext = 
          new WrappedMapper<INKEY, INVALUE, OUTKEY, OUTVALUE>().getMapContext(
              mapContext);

    try {
      input.initialize(split, mapperContext);
      mapper.run(mapperContext);
      mapPhase.complete();
      setPhase(TaskStatus.Phase.SORT);
      statusUpdate(umbilical);
      input.close();
      input = null;
      output.close(mapperContext);
      output = null;
    } finally {
      closeQuietly(input);
      closeQuietly(output, mapperContext);
    }
  }
}
```

**Map任务执行步骤**：
1. **初始化**：设置任务环境和进度报告
2. **输入分片处理**：读取分配给该任务的输入分片
3. **记录读取**：使用RecordReader逐条读取输入记录
4. **Map函数执行**：对每条记录调用用户定义的map函数
5. **输出收集**：收集map函数的输出结果
6. **排序和分区**：对输出进行排序和分区（如果有Reduce任务）

### 11.4.2 输入分片和记录读取

InputFormat负责定义如何读取输入数据：

```java
public abstract class FileInputFormat<K, V> extends InputFormat<K, V> {
  
  public List<InputSplit> getSplits(JobContext job) throws IOException {
    StopWatch sw = new StopWatch().start();
    long minSize = Math.max(getFormatMinSplitSize(), getMinSplitSize(job));
    long maxSize = getMaxSplitSize(job);

    // generate splits
    List<InputSplit> splits = new ArrayList<InputSplit>();
    List<FileStatus> files = listStatus(job);
    for (FileStatus file: files) {
      Path path = file.getPath();
      long length = file.getLen();
      if (length != 0) {
        BlockLocation[] blkLocations;
        if (file instanceof LocatedFileStatus) {
          blkLocations = ((LocatedFileStatus) file).getBlockLocations();
        } else {
          FileSystem fs = path.getFileSystem(job.getConfiguration());
          blkLocations = fs.getFileBlockLocations(file, 0, length);
        }
        if (isSplitable(job, path)) {
          long blockSize = file.getBlockSize();
          long splitSize = computeSplitSize(blockSize, minSize, maxSize);

          long bytesRemaining = length;
          while (((double) bytesRemaining)/splitSize > SPLIT_SLOP) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, splitSize,
                        blkLocations[blkIndex].getHosts(),
                        blkLocations[blkIndex].getCachedHosts()));
            bytesRemaining -= splitSize;
          }

          if (bytesRemaining != 0) {
            int blkIndex = getBlockIndex(blkLocations, length-bytesRemaining);
            splits.add(makeSplit(path, length-bytesRemaining, bytesRemaining,
                       blkLocations[blkIndex].getHosts(),
                       blkLocations[blkIndex].getCachedHosts()));
          }
        } else { // not splitable
          splits.add(makeSplit(path, 0, length, blkLocations[0].getHosts(),
                      blkLocations[0].getCachedHosts()));
        }
      } else { 
        //Create empty hosts array for zero length files
        splits.add(makeSplit(path, 0, length, new String[0]));
      }
    }
    
    // Save the number of input files for metrics/loadgen
    job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());
    sw.stop();
    if (LOG.isDebugEnabled()) {
      LOG.debug("Total # of splits generated by getSplits: " + splits.size()
          + ", TimeTaken: " + sw.now(TimeUnit.MILLISECONDS));
    }
    return splits;
  }

  protected long computeSplitSize(long blockSize, long minSize, long maxSize) {
    return Math.max(minSize, Math.min(maxSize, blockSize));
  }

  protected int getBlockIndex(BlockLocation[] blkLocations, long offset) {
    for (int i = 0 ; i < blkLocations.length; i++) {
      // is the offset inside this block?
      if ((blkLocations[i].getOffset() <= offset) &&
          (offset < blkLocations[i].getOffset() + blkLocations[i].getLength())){
        return i;
      }
    }
    BlockLocation last = blkLocations[blkLocations.length-1];
    long fileLength = last.getOffset() + last.getLength() - 1;
    throw new IllegalArgumentException("Offset " + offset + 
                                       " is outside of file (0.." +
                                       fileLength + ")");
  }
}
```

**分片策略**：
- 考虑HDFS块大小和数据本地性
- 支持最小和最大分片大小配置
- 避免产生过小的分片（SPLIT_SLOP机制）
- 记录分片的主机位置信息

### 11.4.3 Map输出收集和缓冲

MapOutputCollector负责收集Map函数的输出：

```java
public class MapOutputBuffer<K extends Object, V extends Object> 
  implements MapOutputCollector<K, V>, IndexedSortable {
  
  private final int partitions;
  private final JobConf job;
  private final TaskReporter reporter;
  private final Class<K> keyClass;
  private final Class<V> valClass;
  private final RawComparator<K> comparator;
  private final SerializationFactory serializationFactory;
  private final Serializer<K> keySerializer;
  private final Serializer<V> valSerializer;
  private final CombinerRunner<K,V> combinerRunner;
  private final CombineOutputCollector<K,V> combineCollector;

  // Compression for map-outputs
  private final CompressionCodec codec;

  // k/v accounting
  private final IntBuffer kvmeta; // metadata overlay on backing store
  int kvstart;            // marks origin of spill metadata
  int kvend;              // marks end of spill metadata
  int kvindex;            // marks end of fully serialized records

  int equator;            // marks origin of meta/serialization
  int bufstart;           // marks beginning of spill
  int bufend;             // marks beginning of collectable
  int bufmark;            // marks end of record
  int bufindex;           // marks end of collected
  int bufvoid;            // marks the point where we should stop
                          // reading at the end of the buffer

  byte[] kvbuffer;        // main output buffer
  private final byte[] b0 = new byte[0];

  private static final int INDEX = 0;            // index offset in acct
  private static final int KEYSTART = 1;         // key offset in acct
  private static final int VALSTART = 2;         // val offset in acct
  private static final int VALLEN = 3;           // val len in acct
  private static final int NMETA = 4;            // num meta ints
  private static final int METASIZE = NMETA * 4; // size in bytes

  // spill accounting
  private int numSpills = 0;
  private final int minSpillsForCombine;
  private final IndexedSorter sorter;
  private final ReentrantLock spillLock = new ReentrantLock();
  private final Condition spillDone = spillLock.newCondition();
  private final Condition spillReady = spillLock.newCondition();
  private volatile boolean spillInProgress;
  private volatile Throwable sortSpillException = null;

  private final SpillThread spillThread = new SpillThread();

  public synchronized void collect(K key, V value, final int partition
                                   ) throws IOException {
    reporter.progress();
    if (key.getClass() != keyClass) {
      throw new IOException("Type mismatch in key from map: expected "
                            + keyClass.getName() + ", received "
                            + key.getClass().getName());
    }
    if (value.getClass() != valClass) {
      throw new IOException("Type mismatch in value from map: expected "
                            + valClass.getName() + ", received "
                            + value.getClass().getName());
    }
    if (partition < 0 || partition >= partitions) {
      throw new IOException("Illegal partition for " + key + " (" +
          partition + ")");
    }
    checkSpillException();
    bufferPercent = kvbuffer.length;
    if (bufferPercent < SPILL_PERCENT) {
      startSpill();
      final int kvnext = (kvindex + NMETA) % kvmeta.capacity();
      spillLock.lock();
      try {
        boolean kvfull;
        do {
          if (sortSpillException != null) {
            throw new IOException("Spill failed", sortSpillException);
          }
          // sufficient acct space
          kvfull = kvnext == kvstart;
          final boolean kvsoftlimit = ((kvnext > kvend)
              ? kvnext - kvend > softRecordLimit
              : kvend - kvnext < kvmeta.capacity() - softRecordLimit);
          if (kvstart == kvend && kvsoftlimit) {
            LOG.info("Spilling map output");
            LOG.info("bufstart = " + bufstart + "; bufend = " + bufmark +
                     "; bufvoid = " + bufvoid);
            LOG.info("kvstart = " + kvstart + "(" + (kvstart * 4) +
                     "); kvend = " + kvend + "(" + (kvend * 4) +
                     "); length = " + (distanceTo(kvend, kvstart,
                           kvmeta.capacity()) + 1) + "/" + maxRec);
            startSpill();
            try {
              spillReady.await();
            } catch (InterruptedException e) {
                throw new IOException("Interrupted while waiting for the writer", e);
            }
          }
        } while (kvfull);
      } finally {
        spillLock.unlock();
      }
    }

    try {
      // serialize key bytes into buffer
      int keystart = bufindex;
      keySerializer.serialize(key);
      if (bufindex < keystart) {
        // wrapped the key; must make contiguous
        bb.shiftBufferedKey();
        keystart = 0;
      }
      // serialize value bytes into buffer
      final int valstart = bufindex;
      valSerializer.serialize(value);
      // It's possible for records to have zero length, i.e. the serializer
      // will perform no writes. To ensure that the boundary conditions are
      // checked and that the kvindex invariant is maintained, perform a
      // zero-length write into the buffer. The logic monitoring this could be
      // moved into collect, but this is cleaner and inexpensive. For now, it
      // is acceptable.
      bb.write(b0, 0, 0);
      if (bufindex < valstart) {
        // wrapped the value; must make contiguous
        bb.shiftBufferedValue();
        valstart = bufindex;
      }

      // update accounting info
      int ind = kvindex * NMETA;
      kvmeta.put(ind + PARTITION, partition);
      kvmeta.put(ind + KEYSTART, keystart);
      kvmeta.put(ind + VALSTART, valstart);
      kvmeta.put(ind + VALLEN, distanceTo(bufindex, valstart, bufvoid));
      // advance kvindex
      kvindex = kvnext;
    } catch (MapBufferTooSmallException e) {
      LOG.info("Record too large for in-memory buffer: " + e.getMessage());
      spillSingleRecord(key, value, partition);
      mapContext.getCounter("Map-Reduce Framework", 
          "Spilled Records").increment(1);
      return;
    }
  }
}
```

**输出缓冲机制**：
- 使用环形缓冲区存储键值对
- 当缓冲区达到阈值时触发溢写
- 支持键值对的序列化和压缩
- 实现了高效的内存管理策略

## 11.5 Shuffle机制深度分析

### 11.5.1 Shuffle概述

Shuffle是MapReduce框架中最复杂也是最关键的机制，它负责将Map任务的输出传输给Reduce任务。Shuffle过程包括Map端的排序、分区、溢写，以及Reduce端的拉取、合并、排序等步骤。

```java
public class ShuffleSchedulerImpl<K,V> implements ShuffleScheduler<K,V> {

  private final TaskAttemptID reduceId;
  private final JobConf jobConf;
  private final TaskStatus status;
  private final ExceptionReporter reporter;
  private final SecretKey shuffleKey;

  private final int numMaps;
  private Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

  private final Random random = new Random();
  private final DelayQueue<Penalty> penalties = new DelayQueue<Penalty>();
  private final Referee referee = new Referee();
  private final Map<String, MapHost> mapLocations = new HashMap<String, MapHost>();
  private final Set<MapHost> pendingHosts = new HashSet<MapHost>();
  private final Set<TaskAttemptID> obsoleteMaps = new HashSet<TaskAttemptID>();

  private final int maxMapRuntime = 0;
  private final int maxFailedUniqueFetches;
  private final int maxFetchFailuresBeforeReporting;
  private final long startTime;
  private long lastProgressTime;

  @Override
  public synchronized MapHost getHost() throws InterruptedException {
    while(pendingHosts.isEmpty()) {
      wait();
    }

    MapHost host = null;
    Iterator<MapHost> iter = pendingHosts.iterator();
    int numToPick = random.nextInt(pendingHosts.size());
    for (int i=0; i <= numToPick; ++i) {
      host = iter.next();
    }

    pendingHosts.remove(host);
    host.markBusy();

    LOG.info("Assigning " + host + " with " + host.getNumKnownMapOutputs() +
             " to " + Thread.currentThread().getName());
    shuffleStart.set(System.currentTimeMillis());

    return host;
  }

  @Override
  public synchronized void freeHost(MapHost host) {
    if (host.getState() != MapHost.State.PENALIZED) {
      if (host.markAvailable() == MapHost.State.PENDING) {
        pendingHosts.add(host);
        notifyAll();
      }
    }
    LOG.info(host + " freed by " + Thread.currentThread().getName() + " in " +
             (System.currentTimeMillis()-shuffleStart.get()) + "ms");
  }

  @Override
  public synchronized void putBackKnownMapOutput(MapHost host,
                                                TaskAttemptID mapId) {
    host.addKnownMapOutput(mapId);
  }

  @Override
  public synchronized void reportLocalError(IOException ioe) {
    reporter.reportException(ioe);
  }

  @Override
  public synchronized void reportRemoteError(TaskAttemptID mapId,
                                           MapHost host, IOException ioe) {
    host.penalize();
    try {
      penalties.put(new Penalty(host, System.currentTimeMillis() + PENALTY_GROWTH_RATE));
    } catch (InterruptedException ie) {
      Thread.currentThread().interrupt();
    }
  }
}
```

### 11.5.2 Map端Shuffle实现

Map端的Shuffle主要包括分区、排序和溢写：

```java
public class MapOutputBuffer<K extends Object, V extends Object> {

  protected class SpillThread extends Thread {

    @Override
    public void run() {
      spillLock.lock();
      spillThreadRunning = true;
      try {
        while (true) {
          spillDone.signal();
          while (!spillInProgress) {
            spillReady.await();
          }
          try {
            spillLock.unlock();
            sortAndSpill();
          } catch (Throwable e) {
            sortSpillException = e;
          } finally {
            spillLock.lock();
            if (bufend < bufstart) {
              bufvoid = kvbuffer.length;
            }
            kvstart = kvend;
            bufstart = bufend;
            spillInProgress = false;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
      } finally {
        spillLock.unlock();
        spillThreadRunning = false;
      }
    }
  }

  @SuppressWarnings("unchecked")
  private void sortAndSpill() throws IOException, ClassNotFoundException,
                                     InterruptedException {
    //approximate the length of the output file to be the length of the
    //buffer + header lengths for the partitions
    final long size = distanceTo(bufstart, bufend, bufvoid) +
                      partitions * APPROX_HEADER_LENGTH;
    FSDataOutputStream out = null;
    try {
      // create spill file
      final SpillRecord spillRec = new SpillRecord(partitions);
      final Path filename =
          mapOutputFile.getSpillFileForWrite(numSpills, size);
      out = rfs.create(filename);

      final int mstart = kvend;
      final int mend = 1 + // kvend is a valid record
        (kvstart >= kvend ? kvstart : kvmeta.capacity() + kvstart);
      sorter.sort(MapOutputBuffer.this, mstart, mend, reporter);
      int spindex = mstart;
      final IndexRecord rec = new IndexRecord();
      final InMemValBytes value = new InMemValBytes();
      for (int i = 0; i < partitions; ++i) {
        IFile.Writer<K, V> writer = null;
        try {
          long segmentStart = out.getPos();
          FSDataOutputStream partitionOut = CryptoUtils.wrapIfNecessary(job, out);
          writer = new IFile.Writer<K, V>(job, partitionOut, keyClass, valClass, codec,
                                          spilledRecordsCounter, true);
          if (combinerRunner == null) {
            // spill directly
            DataInputBuffer key = new DataInputBuffer();
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
              final int kvoff = offsetFor(spindex % maxRec);
              int keystart = kvmeta.get(kvoff + KEYSTART);
              int valstart = kvmeta.get(kvoff + VALSTART);
              key.reset(kvbuffer, keystart, valstart - keystart);
              getVBytesForOffset(kvoff, value);
              writer.append(key, value);
              ++spindex;
            }
          } else {
            int spstart = spindex;
            while (spindex < mend &&
                kvmeta.get(offsetFor(spindex % maxRec) + PARTITION) == i) {
              ++spindex;
            }
            // Note: we would like to avoid the combiner if we've fewer
            // than some threshold of records for a partition
            if (spstart != spindex) {
              combineCollector.setWriter(writer);
              RawKeyValueIterator kvIter =
                new MRResultIterator(spstart, spindex);
              combinerRunner.combine(kvIter, combineCollector);
            }
          }

          // close the writer
          writer.close();

          // record offsets
          rec.startOffset = segmentStart;
          rec.rawLength = writer.getRawLength() + CryptoUtils.cryptoPadding(job);
          rec.partLength = writer.getCompressedLength() + CryptoUtils.cryptoPadding(job);
          spillRec.putIndex(rec, i);

          writer = null;
        } finally {
          if (null != writer) writer.close();
        }
      }

      if (totalIndexCacheMemory >= indexCacheMemoryLimit) {
        // create spill index file
        Path indexFilename =
            mapOutputFile.getSpillIndexFileForWrite(numSpills, partitions
                * MAP_OUTPUT_INDEX_RECORD_LENGTH);
        spillRec.writeToFile(indexFilename, job);
      } else {
        indexCacheList.add(spillRec);
        totalIndexCacheMemory +=
          spillRec.size() * MAP_OUTPUT_INDEX_RECORD_LENGTH;
      }
      LOG.info("Finished spill " + numSpills);
      ++numSpills;
    } finally {
      if (out != null) out.close();
    }
  }
}
```

**Map端Shuffle步骤**：
1. **内存缓冲**：Map输出首先写入内存缓冲区
2. **分区排序**：按照分区号和键进行排序
3. **溢写磁盘**：将排序后的数据写入临时文件
4. **合并溢写文件**：将多个溢写文件合并为最终输出

### 11.5.3 Reduce端Shuffle实现

Reduce端的Shuffle包括拉取、合并和排序：

```java
public class Fetcher<K,V> extends Thread {

  private final TaskAttemptID reduceId;
  private final ShuffleSchedulerImpl<K,V> scheduler;
  private final MergeManager<K,V> merger;
  private final ShuffleClientMetrics metrics;
  private final ExceptionReporter reporter;
  private final JobConf jobConf;
  private final CompressionCodec codec;
  private final SecretKey shuffleKey;
  private volatile boolean stopped = false;

  private static boolean sslShuffle;
  private static SSLFactory sslFactory;

  public Fetcher(JobConf job, TaskAttemptID reduceId,
                 ShuffleSchedulerImpl<K,V> scheduler, MergeManager<K,V> merger,
                 Reporter reporter, ShuffleClientMetrics metrics,
                 ExceptionReporter exceptionReporter, SecretKey shuffleKey) {
    this.jobConf = job;
    this.reduceId = reduceId;
    this.scheduler = scheduler;
    this.merger = merger;
    this.reporter = reporter;
    this.metrics = metrics;
    this.exceptionReporter = exceptionReporter;
    this.shuffleKey = shuffleKey;

    this.codec = CodecUtil.getCodec(job);
    this.connectionTimeout =
      job.getInt(MRJobConfig.SHUFFLE_CONNECT_TIMEOUT,
                 MRJobConfig.DEFAULT_SHUFFLE_CONNECT_TIMEOUT);
    this.readTimeout =
      job.getInt(MRJobConfig.SHUFFLE_READ_TIMEOUT,
                 MRJobConfig.DEFAULT_SHUFFLE_READ_TIMEOUT);

    setName("fetcher#" + id);
    setDaemon(true);
  }

  @Override
  public void run() {
    try {
      while (!stopped && !Thread.currentThread().isInterrupted()) {
        MapHost host = null;
        try {
          // If merge is on, block
          merger.waitForResource();

          // Get a host to shuffle from
          host = scheduler.getHost();
          metrics.threadBusy();

          // Shuffle
          copyFromHost(host);
        } finally {
          if (host != null) {
            scheduler.freeHost(host);
            metrics.threadFree();
          }
        }
      }
    } catch (InterruptedException ie) {
      return;
    } catch (Throwable t) {
      exceptionReporter.reportException(t);
    }
  }

  private void copyFromHost(MapHost host) throws IOException {
    // Get completed maps on 'host'
    List<TaskAttemptID> maps = scheduler.getMapsForHost(host);

    // Sanity check to catch hosts with only 'OBSOLETE' maps,
    // especially at the tail of large jobs
    if (maps.size() == 0) {
      return;
    }

    LOG.debug("Fetcher " + id + " going to fetch from " + host + " for: "
      + maps);

    // List of maps to be fetched yet
    Set<TaskAttemptID> remaining = new HashSet<TaskAttemptID>(maps);

    // Construct the url and connect
    URL url = getMapOutputURL(host, maps);
    HttpURLConnection connection = openConnection(url);

    // Validate header
    validateConnectionHeader(connection);

    // Get the size of the compressed data
    int compressedLength = connection.getContentLength();
    int decompressedLength = Integer.parseInt(
      connection.getHeaderField(ShuffleHeader.HTTP_HEADER_NAME));

    // Get the data
    InputStream input = connection.getInputStream();

    try {
      input = CryptoUtils.wrapIfNecessary(jobConf, input, compressedLength);

      // Process each map output
      for (TaskAttemptID mapId : maps) {
        if (!remaining.contains(mapId)) {
          continue;
        }

        LOG.debug("Fetcher " + id + " about to shuffle output of map " +
                  mapId + " decomp: " + decompressedLength + " len: " +
                  compressedLength + " to " + reduceId);

        // Read the shuffle header
        ShuffleHeader header = new ShuffleHeader();
        header.readFields(input);

        // Get the location for the map output - either in-memory or on-disk
        MapOutput<K,V> mapOutput = null;
        try {
          mapOutput = merger.reserve(mapId, header.getDecompressedLength(),
                                     id);
        } catch (IOException ioe) {
          // kill the reduce attempt
          ioErrs.increment(1);
          scheduler.reportLocalError(ioe);
          return;
        }

        // Check if we can shuffle *now* ...
        if (mapOutput == null) {
          LOG.info("fetcher#" + id + " - MergeManager returned status WAIT ...");
          // Not an error but wait to process data.
          return;
        }

        // The codec for lz0,lz4,snappy,bz2,etc. throw java.lang.InternalError
        // on decompression failures. Catching and re-throwing as IOException
        // to allow fetch failure logic to be processed
        try {
          // Go!
          LOG.info("fetcher#" + id + " about to shuffle output of map " +
                   mapOutput.getMapId() + " decomp: " +
                   header.getDecompressedLength() + " len: " +
                   header.getCompressedLength() + " to " + reduceId);

          mapOutput.shuffle(host, input, header.getCompressedLength(),
                           header.getDecompressedLength(), metrics, reporter);
        } catch (java.lang.InternalError | Exception e) {
          LOG.warn("Failed to shuffle output of " + mapId +
                   " from " + host.getHostName(), e);

          // Inform the shuffle-scheduler
          mapOutput.abort();
          metrics.failedFetch();
          return;
        }

        // Successful copy
        remaining.remove(mapId);
        metrics.successFetch();

        // Inform the shuffle scheduler
        long bytes = mapOutput.getSize();
        scheduler.copySucceeded(mapId, host, bytes, System.currentTimeMillis(),
                               mapOutput);
      }

      // Sanity check
      if (remaining.size() > 0) {
        throw new IOException("Incomplete fetch (" + remaining.size() +
                              " pending) from " + host.getHostName());
      }

      LOG.info("fetcher#" + id + " done fetching from " + host.getHostName());

    } finally {
      // Close connection
      connection.disconnect();
    }
  }
}
```

**Reduce端Shuffle步骤**：
1. **Map输出定位**：确定需要拉取的Map输出位置
2. **并行拉取**：启动多个Fetcher线程并行拉取数据
3. **内存管理**：根据内存情况决定数据存储位置
4. **合并排序**：将拉取的数据进行合并和排序

## 11.6 Reduce任务执行机制

### 11.6.1 ReduceTask的核心实现

ReduceTask负责执行Reduce阶段的处理逻辑：

```java
public class ReduceTask extends Task {

  private int numMaps;
  private ReduceCopier reduceCopier;

  @Override
  public void run(JobConf job, final TaskUmbilicalProtocol umbilical)
    throws IOException, InterruptedException, ClassNotFoundException {

    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());

    if (isMapOrReduce()) {
      copyPhase = getProgress().addPhase("copy");
      sortPhase  = getProgress().addPhase("sort");
      reducePhase = getProgress().addPhase("reduce");
    }

    // start thread that will handle communication with parent
    TaskReporter reporter = startReporter(umbilical);

    boolean useNewApi = job.getUseNewReducer();
    initialize(job, getJobID(), reporter, useNewApi);

    // check if it is a cleanupJobTask
    if (jobCleanup) {
      runJobCleanupTask(umbilical, reporter);
      return;
    }
    if (jobSetup) {
      runJobSetupTask(umbilical, reporter);
      return;
    }
    if (taskCleanup) {
      runTaskCleanupTask(umbilical, reporter);
      return;
    }

    // Initialize the codec
    codec = initCodec();
    RawKeyValueIterator rIter = null;
    ShuffleConsumerPlugin shuffleConsumerPlugin = null;

    Class combinerClass = conf.getCombinerClass();
    CombineOutputCollector combineCollector =
      (null != combinerClass) ?
       new CombineOutputCollector(reduceCombineOutputCounter, reporter, conf) : null;

    Class<? extends ShuffleConsumerPlugin> clazz =
          job.getClass(MRConfig.SHUFFLE_CONSUMER_PLUGIN, Shuffle.class, ShuffleConsumerPlugin.class);

    shuffleConsumerPlugin = ReflectionUtils.newInstance(clazz, job);
    LOG.info("Using ShuffleConsumerPlugin: " + shuffleConsumerPlugin);

    ShuffleConsumerPlugin.Context shuffleContext =
      new ShuffleConsumerPlugin.Context(getTaskID(), job, FileSystem.getLocal(job), umbilical,
                  super.lDirAlloc, reporter, codec,
                  combinerClass, combineCollector,
                  spilledRecordsCounter, reduceCombineInputCounter,
                  shuffledMapsCounter,
                  reduceShuffleBytes, failedShuffleCounter,
                  mergedMapOutputsCounter,
                  taskStatus, copyPhase, sortPhase, this,
                  mapOutputFile, localMapFiles);
    shuffleConsumerPlugin.init(shuffleContext);
    rIter = shuffleConsumerPlugin.run();

    // free up the data structures
    mapOutputFilesOnDisk.clear();

    sortPhase.complete();                         // sort is complete
    setPhase(TaskStatus.Phase.REDUCE);
    statusUpdate(umbilical);
    Class keyClass = job.getMapOutputKeyClass();
    Class valueClass = job.getMapOutputValueClass();
    RawComparator comparator = job.getOutputValueGroupingComparator();

    if (useNewApi) {
      runNewReducer(job, umbilical, reporter, rIter, comparator,
                    keyClass, valueClass);
    } else {
      runOldReducer(job, umbilical, reporter, rIter, comparator,
                    keyClass, valueClass);
    }

    shuffleConsumerPlugin.close();
    done(umbilical, reporter);
  }

  @SuppressWarnings("unchecked")
  private <INKEY,INVALUE,OUTKEY,OUTVALUE>
  void runNewReducer(JobConf job,
                     final TaskUmbilicalProtocol umbilical,
                     final TaskReporter reporter,
                     RawKeyValueIterator rIter,
                     RawComparator<INKEY> comparator,
                     Class<INKEY> keyClass,
                     Class<INVALUE> valueClass
                     ) throws IOException,InterruptedException,
                              ClassNotFoundException {
    // wrap value iterator to report progress.
    final RawKeyValueIterator rawIter = rIter;
    rIter = new RawKeyValueIterator() {
      public void close() throws IOException {
        rawIter.close();
      }
      public DataInputBuffer getKey() throws IOException {
        return rawIter.getKey();
      }
      public Progress getProgress() {
        return rawIter.getProgress();
      }
      public DataInputBuffer getValue() throws IOException {
        return rawIter.getValue();
      }
      public boolean next() throws IOException {
        boolean ret = rawIter.next();
        reporter.setProgress(rawIter.getProgress().getProgress());
        return ret;
      }
    };
    // make a task context so we can get the classes
    org.apache.hadoop.mapreduce.TaskAttemptContext taskContext =
      new org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl(job, getTaskID(), reporter);

    // make a reducer
    org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE> reducer =
      (org.apache.hadoop.mapreduce.Reducer<INKEY,INVALUE,OUTKEY,OUTVALUE>)
        ReflectionUtils.newInstance(taskContext.getReducerClass(), job);
    org.apache.hadoop.mapreduce.RecordWriter<OUTKEY,OUTVALUE> trackedRW =
      new NewTrackingRecordWriter<OUTKEY, OUTVALUE>(this, taskContext);
    job.setBoolean("mapred.skip.on", isSkipping());
    job.setBoolean(JobContext.SKIP_RECORDS, isSkipping());
    org.apache.hadoop.mapreduce.Reducer.Context
         reducerContext = createReduceContext(reducer, job, getTaskID(),
                                               rIter, reduceInputKeyCounter,
                                               reduceInputValueCounter,
                                               trackedRW,
                                               committer,
                                               reporter, comparator, keyClass,
                                               valueClass);
    try {
      reducer.run(reducerContext);
    } finally {
      trackedRW.close(reducerContext);
    }
  }
}
```

**Reduce任务执行阶段**：
1. **Shuffle阶段**：拉取和合并Map输出
2. **Sort阶段**：对合并后的数据进行排序
3. **Reduce阶段**：执行用户定义的Reduce函数
4. **输出阶段**：将结果写入最终输出位置

### 11.6.2 数据合并和排序

MergeManager负责管理Reduce端的数据合并：

```java
public class MergeManagerImpl<K, V> implements MergeManager<K, V> {

  private final int memoryLimit;
  private final long maxSingleShuffleLimit;

  private final int memToMemMergeOutputsThreshold;
  private final long mergeThreshold;

  private final Set<MapOutput<K,V>> inMemoryMergedMapOutputs =
    new TreeSet<MapOutput<K,V>>(new MapOutput.MapOutputComparator<K,V>());
  private final IntermediateMemoryToMemoryMerger memToMemMerger;

  private final Set<MapOutput<K,V>> inMemoryMapOutputs =
    new TreeSet<MapOutput<K,V>>(new MapOutput.MapOutputComparator<K,V>());
  private final InMemoryMerger inMemoryMerger;

  private final Set<Path> onDiskMapOutputs = new TreeSet<Path>();
  private final OnDiskMerger onDiskMerger;

  private final long memoryAvailable = maxMemory - reservedMemory;
  private long usedMemory = 0;
  private long commitMemory = 0;
  private final long maxSingleShuffleLimit =
    (long)(memoryLimit * maxSingleShuffleInputPercent);
  private final int memToMemMergeOutputsThreshold =
    (ioSortFactor > mapOutputsFilesInMemory) ? mapOutputsFilesInMemory : ioSortFactor;
  private final long mergeThreshold = (long)(this.memoryLimit * shuffleMergePercent);

  boolean canShuffleToMemory(long requestedSize) {
    return (requestedSize < maxSingleShuffleLimit);
  }

  @Override
  public synchronized MapOutput<K,V> reserve(TaskAttemptID mapId,
                                             long requestedSize,
                                             int fetcher
                                             ) throws IOException {
    if (!canShuffleToMemory(requestedSize)) {
      LOG.info(mapId + ": Shuffling to disk since " + requestedSize +
               " is greater than maxSingleShuffleLimit (" +
               maxSingleShuffleLimit + ")");
      return new MapOutput<K,V>(mapId, this, requestedSize, conf,
                                localDirAllocator, fetcher, true,
                                fs, mapOutputFile);
    }

    // Stall shuffle if we are above the memory limit

    // It is possible that all threads could just be stalling and not make
    // progress at all. This could happen when:
    //
    // requested size is causing the used memory to go above the limit &&
    // requested size < singleShuffleLimit &&
    // current used size < mergeThreshold (merge will not get triggered)
    //
    // To avoid this from happening, we allow exactly one thread to go past
    // the memory limit. We check (usedMemory > memoryLimit) and not
    // (usedMemory + requestedSize > memoryLimit). When this thread is done
    // fetching, this will automatically trigger a merge thereby unlocking
    // all the stalled threads

    if (usedMemory > memoryLimit) {
      LOG.debug(mapId + ": Stalling shuffle since usedMemory (" + usedMemory
          + ") is greater than memoryLimit (" + memoryLimit + ")." +
          " CommitMemory is (" + commitMemory + ")");
      return stallShuffle;
    }

    // Allow the in-memory shuffle
    usedMemory += requestedSize;
    commitMemory += requestedSize;
    if (LOG.isDebugEnabled()) {
      LOG.debug(mapId + ": Proceeding with shuffle since usedMemory ("
          + usedMemory + ") is lesser than memoryLimit (" + memoryLimit + ")."
          + "CommitMemory is (" + commitMemory + ")");
    }
    return new MapOutput<K,V>(mapId, this, requestedSize, conf,
                              localDirAllocator, fetcher, false,
                              null, mapOutputFile);
  }

  @Override
  public synchronized void closeInMemoryFile(MapOutput<K,V> mapOutput) {
    inMemoryMapOutputs.add(mapOutput);
    LOG.info("closeInMemoryFile -> map-output of size: " + mapOutput.getSize()
        + ", inMemoryMapOutputs.size() -> " + inMemoryMapOutputs.size()
        + ", commitMemory -> " + commitMemory + ", usedMemory ->" + usedMemory);

    commitMemory -= mapOutput.getSize();

    // Can hang if mergeThreshold is really low.
    if (commitMemory < mergeThreshold) {
      LOG.info("Starting inMemoryMerger's merge since commitMemory=" +
          commitMemory + " < mergeThreshold=" + mergeThreshold +
          ". Current inMemoryMapOutputs.size()=" + inMemoryMapOutputs.size());
      inMemoryMerger.startMerge(inMemoryMapOutputs);
    }
  }

  private class InMemoryMerger extends MergeThread<MapOutput<K,V>, K, V> {

    public InMemoryMerger(MergeManagerImpl<K, V> manager) {
      super(manager, Integer.MAX_VALUE, exceptionReporter);
      setName("InMemoryMerger - Thread to merge in-memory shuffled map-outputs");
      setDaemon(true);
    }

    @Override
    public void merge(List<MapOutput<K,V>> inputs) throws IOException {
      if (inputs == null || inputs.size() == 0) {
        return;
      }

      TaskAttemptID dummyMapId = inputs.get(0).getMapId();
      List<Segment<K, V>> inMemorySegments = new ArrayList<Segment<K, V>>();
      long mergeOutputSize =
        createInMemorySegments(inputs, inMemorySegments, 0);
      int noInMemorySegments = inMemorySegments.size();

      MapOutput<K, V> mergedMapOutput =
        unconditionalReserve(dummyMapId, mergeOutputSize, false);

      Writer<K, V> writer =
        new InMemoryWriter<K, V>(mergedMapOutput.getArrayStream());

      LOG.info("Initiating in-memory merge with " + noInMemorySegments +
               " segments...");

      RawKeyValueIterator rIter =
        Merger.merge(conf, fs,
                     keyClass, valueClass,
                     inMemorySegments, inMemorySegments.size(),
                     new Path(reduceId.toString()),
                     conf.getOutputKeyComparator(), reporter, spilledRecordsCounter,
                     null, null);
      Merger.writeFile(rIter, writer, reporter, conf);
      writer.close();

      LOG.info(reduceId +
          " Merge of the " + noInMemorySegments +
          " files in-memory complete." +
          " Local file is " + mergedMapOutput.getDescription() +
          " of size " + mergedMapOutput.getSize());

      // Note the output of the merge
      closeInMemoryMergedFile(mergedMapOutput);
    }
  }
}
```

**合并策略**：
- **内存合并**：优先在内存中合并小文件
- **磁盘合并**：大文件直接在磁盘上合并
- **多级合并**：支持多轮合并以控制文件数量
- **压缩优化**：支持数据压缩以减少I/O

## 11.7 MapReduce容错处理

### 11.7.1 任务级容错机制

MapReduce实现了完善的任务级容错处理：

```java
public class TaskAttemptImpl implements TaskAttempt,
    EventHandler<TaskAttemptEvent> {

  private final TaskAttemptId attemptId;
  private final EventHandler eventHandler;
  private final TaskAttemptListener taskAttemptListener;
  private final Path jobFile;
  private final int partition;
  private final JobConf conf;
  private final List<String> diagnostics = new ArrayList<String>();
  private final Lock readLock;
  private final Lock writeLock;
  private final MRAppMetrics metrics;
  private final AppContext appContext;

  private static final StateMachineFactory
               <TaskAttemptImpl, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
           stateMachineFactory
         = new StateMachineFactory<TaskAttemptImpl, TaskAttemptState, TaskAttemptEventType, TaskAttemptEvent>
              (TaskAttemptState.NEW)

    // Transitions from NEW state
    .addTransition(TaskAttemptState.NEW, TaskAttemptState.UNASSIGNED,
        TaskAttemptEventType.TA_SCHEDULE, new RequestContainerTransition(false))
    .addTransition(TaskAttemptState.NEW, TaskAttemptState.UNASSIGNED,
        TaskAttemptEventType.TA_RESCHEDULE, new RequestContainerTransition(true))
    .addTransition(TaskAttemptState.NEW, TaskAttemptState.KILLED,
        TaskAttemptEventType.TA_KILL, new KilledTransition())
    .addTransition(TaskAttemptState.NEW, TaskAttemptState.FAILED,
        TaskAttemptEventType.TA_FAILMSG, new FailedTransition())

    // Transitions from UNASSIGNED state
    .addTransition(TaskAttemptState.UNASSIGNED, TaskAttemptState.ASSIGNED,
        TaskAttemptEventType.TA_ASSIGNED, new ContainerAssignedTransition())
    .addTransition(TaskAttemptState.UNASSIGNED, TaskAttemptState.KILLED,
        TaskAttemptEventType.TA_KILL, new DeallocateContainerTransition(
            TaskAttemptState.KILLED, true))
    .addTransition(TaskAttemptState.UNASSIGNED, TaskAttemptState.FAILED,
        TaskAttemptEventType.TA_FAILMSG, new DeallocateContainerTransition(
            TaskAttemptState.FAILED, true))

    // Transitions from ASSIGNED state
    .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.RUNNING,
        TaskAttemptEventType.TA_STARTED, new LaunchedContainerTransition())
    .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.FAILED,
        TaskAttemptEventType.TA_CONTAINER_LAUNCH_FAILED,
        new DeallocateContainerTransition(TaskAttemptState.FAILED, false))
    .addTransition(TaskAttemptState.ASSIGNED, TaskAttemptState.KILLED,
        TaskAttemptEventType.TA_KILL, new DeallocateContainerTransition(
            TaskAttemptState.KILLED, true))

    // Transitions from RUNNING state
    .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.SUCCESS,
        TaskAttemptEventType.TA_DONE, new SucceededTransition())
    .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAILED,
        TaskAttemptEventType.TA_FAILMSG, new FailedTransition())
    .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.FAILED,
        TaskAttemptEventType.TA_TIMED_OUT, new FailedTransition())
    .addTransition(TaskAttemptState.RUNNING, TaskAttemptState.KILLED,
        TaskAttemptEventType.TA_KILL, new KilledTransition())

    .installTopology();

  @Override
  public void handle(TaskAttemptEvent event) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Processing " + event.getTaskAttemptID() + " of type "
          + event.getType());
    }
    writeLock.lock();
    try {
      final TaskAttemptState oldState = getState();
      try {
        stateMachine.doTransition(event.getType(), event);
      } catch (InvalidStateTransitonException e) {
        LOG.error("Can't handle this event at current state for "
            + this.attemptId, e);
        internalError(event.getType());
      }
      if (oldState != getState()) {
        LOG.info(attemptId + " TaskAttempt Transitioned from " + oldState
            + " to " + getState());
      }
    } finally {
      writeLock.unlock();
    }
  }

  private static class FailedTransition implements
      SingleArcTransition<TaskAttemptImpl, TaskAttemptEvent> {

    @Override
    public void transition(TaskAttemptImpl taskAttempt,
        TaskAttemptEvent event) {

      // set the finish time
      taskAttempt.setFinishTime();

      if (event instanceof TaskAttemptFailEvent) {
        TaskAttemptFailEvent castEvent = (TaskAttemptFailEvent) event;
        taskAttempt.addDiagnosticInfo(castEvent.getDiagnosticInfo());
      } else if (event instanceof TaskAttemptKillEvent) {
        taskAttempt.addDiagnosticInfo("TaskAttempt killed before it completed successfully");
      }

      // Send out events to the Task - indicating TaskAttemptFailed
      taskAttempt.eventHandler.handle(new TaskTAttemptEvent(
          taskAttempt.attemptId, TaskEventType.T_ATTEMPT_FAILED));

      taskAttempt.eventHandler.handle(new JobCounterUpdateEvent
          (taskAttempt.attemptId.getTaskId().getJobId())
          .addCounterUpdate(JobCounter.NUM_FAILED_MAPS, 1));

      LOG.info("TaskAttempt " + taskAttempt.attemptId + " transitioned to FAILED");
    }
  }
}
```

**容错特性**：
- **任务重试**：失败的任务自动在其他节点重试
- **推测执行**：对运行缓慢的任务启动备份任务
- **节点黑名单**：避免在频繁失败的节点上调度任务
- **检查点机制**：支持任务的中间状态保存

### 11.7.2 推测执行机制

Speculator负责检测和处理运行缓慢的任务：

```java
public class DefaultSpeculator extends AbstractService implements
    Speculator {

  private final Map<TaskId, Boolean> runningTasks;
  private final Map<TaskAttemptId, Boolean> runningTaskAttempts;
  private final Map<TaskId, TaskRuntimeEstimator> estimators;
  private final SpeculatorEvent poisonPill = new SpeculatorEvent(null, -1L);
  private final BlockingQueue<SpeculatorEvent> eventQueue =
      new LinkedBlockingQueue<SpeculatorEvent>();
  private final Thread speculationBackgroundThread = new Thread() {
      @Override
      public void run() {
        while (!Thread.currentThread().isInterrupted()) {
          processSpeculatorEvent();
        }
      }
    };
  private final Clock clock;
  private final EventHandler<Event> eventHandler;
  private final Configuration conf;
  private AppContext context;
  private int soonestRetryAfterNoSpeculate;
  private int soonestRetryAfterSpeculate;
  private double proportionRunningTasksSpeculatable;
  private double proportionTotalTasksSpeculatable;
  private int minimumAllowedSpeculativeTasks;

  @Override
  protected void serviceStart() throws Exception {
    Preconditions.checkNotNull(conf);
    this.context = getContext();
    this.speculationBackgroundThread.start();
    super.serviceStart();
  }

  @Override
  protected void serviceStop() throws Exception{
    // this could be called before serviceStart() finishes
    if (speculationBackgroundThread != null) {
      speculationBackgroundThread.interrupt();
    }
    super.serviceStop();
  }

  @Override
  public void handleAttempt(TaskAttemptStatus status) {
    long timestamp = clock.getTime();
    statusUpdate(status, timestamp);
  }

  // This section is not part of the Speculator interface; it's used only for
  // testing
  public boolean eventQueueEmpty() {
    return eventQueue.isEmpty();
  }

  // This interface is intended to be used only for test cases.
  public void scanForSpeculations() {
    LOG.info("We got asked to run a debug speculation scan.");
    // debug
    System.out.println("We got asked to run a debug speculation scan.");
    System.out.println("There are " + runningTasks.size()
        + " running tasks");

    long now = clock.getTime();
    int speculations = computeSpeculations(now);
  }

  private int computeSpeculations(long now) {
    // We'll try to issue one speculation per job per round, at most
    //  This is to prevent runaway speculation
    //  Note that this intentionally doesn't prevent speculation if there are
    //   multiple jobs running.

    int successes = 0;

    long soonestLaunchTime = Long.MIN_VALUE;

    for (Job job : context.getAllJobs().values()) {
      int speculations = computeSpeculations(job, now);
      successes += speculations;
      long jobSoonestLaunchTime = context.getJob(job.getID()).getLaunchTime();
      if (jobSoonestLaunchTime > soonestLaunchTime) {
        soonestLaunchTime = jobSoonestLaunchTime;
      }
    }

    long speculationWaitTime = Math.max(
      proportionTotalTasksSpeculatable * (now - soonestLaunchTime),
      minimumAllowedSpeculativeTasks);

    if (successes == 0) {
      long nextSpeculationTime = now + soonestRetryAfterNoSpeculate;
      eventQueue.add(new SpeculatorEvent(null, nextSpeculationTime));
    } else {
      long nextSpeculationTime = now + soonestRetryAfterSpeculate;
      eventQueue.add(new SpeculatorEvent(null, nextSpeculationTime));
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("We launched " + successes + " speculations.  Sleeping "
          + soonestRetryAfterSpeculate + " milliseconds.");
    }

    return successes;
  }

  private int computeSpeculations(Job job, long now) {
    int successes = 0;

    int numberSpeculationsAlready = 0;
    int numberRunningTasks = 0;

    // loop through the tasks of the kind
    Map<TaskId, Task> tasks = job.getTasks(TaskType.MAP);
    for (Task task : tasks.values()) {
      if (shouldConsiderTaskForSpeculation(task)) {
        numberRunningTasks++;
        if (task.getAttempts().size() > 1) {
          numberSpeculationsAlready++;
        }
        if (canSpeculateForTask(task) && shouldSpeculateForTask(task, now)) {
          successes++;
          eventHandler.handle(new TaskEvent(task.getID(), TaskEventType.T_ADD_SPEC_ATTEMPT));
        }
      }
    }

    tasks = job.getTasks(TaskType.REDUCE);
    for (Task task : tasks.values()) {
      if (shouldConsiderTaskForSpeculation(task)) {
        numberRunningTasks++;
        if (task.getAttempts().size() > 1) {
          numberSpeculationsAlready++;
        }
        if (canSpeculateForTask(task) && shouldSpeculateForTask(task, now)) {
          successes++;
          eventHandler.handle(new TaskEvent(task.getID(), TaskEventType.T_ADD_SPEC_ATTEMPT));
        }
      }
    }

    if (numberSpeculationsAlready >= numberRunningTasks * proportionRunningTasksSpeculatable) {
      return 0;
    }

    return successes;
  }
}
```

**推测执行策略**：
- **运行时间估算**：基于历史数据估算任务完成时间
- **异常检测**：识别运行异常缓慢的任务
- **资源控制**：限制推测任务的数量和比例
- **效果评估**：评估推测执行的效果和收益

## 11.8 本章小结

本章深入分析了MapReduce框架的核心设计和实现机制。MapReduce作为Hadoop生态系统的经典计算框架，通过简单而强大的编程模型，为大规模数据处理提供了可靠的解决方案。

**核心要点**：

**编程模型**：Map和Reduce两阶段处理模式，简化了并行编程的复杂性。

**作业执行**：从作业提交到完成的完整流程，包括资源申请、任务调度、容错处理等。

**MRAppMaster**：作为MapReduce的ApplicationMaster，负责协调整个作业的执行。

**Map任务**：处理输入分片，执行用户定义的Map函数，收集和缓冲输出结果。

**输出管理**：通过MapOutputBuffer实现高效的输出收集和溢写机制。

MapReduce框架的设计体现了分布式计算的核心原则：数据本地性、容错性、可扩展性。虽然在某些场景下MapReduce的性能可能不如新一代的计算框架，但其简单性和可靠性使其在大数据处理领域仍然占有重要地位。理解MapReduce的实现原理，对于深入掌握分布式计算技术具有重要意义。
