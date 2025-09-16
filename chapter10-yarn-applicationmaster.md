# 第10章：YARN ApplicationMaster框架

## 10.1 引言

ApplicationMaster（AM）是YARN架构中的核心组件之一，它代表着运行在YARN集群上的单个应用程序。如果说ResourceManager是YARN的"调度中心"，NodeManager是"执行单元"，那么ApplicationMaster就是"应用代理"，负责协调和管理特定应用的所有任务。

ApplicationMaster的设计体现了YARN"一个集群，多种计算框架"的核心理念。不同的计算框架（如MapReduce、Spark、Storm等）可以实现自己的ApplicationMaster，从而在同一个YARN集群上运行不同类型的应用。这种设计不仅提高了集群的资源利用率，还为大数据生态系统的发展提供了统一的资源管理平台。

ApplicationMaster的核心职责包括资源申请与释放、任务调度与监控、容错处理与恢复等。它需要与ResourceManager协商获取资源，与NodeManager通信启动和监控容器，同时还要处理各种异常情况，确保应用的正确执行。本章将深入分析ApplicationMaster的设计模式、实现机制以及与YARN其他组件的交互过程。

## 10.2 ApplicationMaster架构概述

### 10.2.1 ApplicationMaster的核心职责

ApplicationMaster在YARN生态系统中扮演着应用程序的"大脑"角色：

```java
/**
 * The ApplicationMaster is responsible for negotiating resources from 
 * the ResourceManager and working with the NodeManager(s) to execute 
 * and monitor the tasks.
 */
public abstract class ApplicationMaster {
  
  protected AMRMClientAsync<ContainerRequest> amRMClient;
  protected NMClientAsync nmClientAsync;
  protected Configuration conf;
  protected ApplicationAttemptId appAttemptID;
  protected String appMasterHostname = "";
  protected int appMasterRpcPort = -1;
  protected String appMasterTrackingUrl = "";
  protected volatile boolean done = false;
  protected volatile boolean success = false;
  protected ByteBuffer allTokens;

  /**
   * Main run function for the application master
   */
  public abstract void run() throws YarnException, IOException;

  /**
   * Called when containers are completed
   */
  public abstract void onContainersCompleted(List<ContainerStatus> completedContainers);

  /**
   * Called when containers are allocated
   */
  public abstract void onContainersAllocated(List<Container> allocatedContainers);

  /**
   * Called when nodes are updated
   */
  public abstract void onNodesUpdated(List<NodeReport> updatedNodes);

  /**
   * Called when there is an error in the RM communication
   */
  public abstract void onError(Throwable e);

  /**
   * Called when the RM wants the AM to shutdown
   */
  public abstract void onShutdownRequest();
}
```

**资源管理职责**：
- 根据应用需求向ResourceManager申请容器资源
- 监控资源使用情况，动态调整资源需求
- 释放不再需要的容器资源
- 处理资源分配失败和抢占情况

**任务调度职责**：
- 将应用逻辑分解为可执行的任务
- 根据数据本地性和资源可用性调度任务
- 监控任务执行状态和进度
- 处理任务失败和重试逻辑

**容错处理职责**：
- 检测和处理容器失败
- 实现任务级别的容错恢复
- 处理节点失效对应用的影响
- 维护应用状态的一致性

### 10.2.2 ApplicationMaster的生命周期

ApplicationMaster的生命周期与应用程序的执行过程紧密相关：

```java
public enum ApplicationMasterState {
  NEW,
  INITED,
  STARTED,
  RUNNING,
  FINAL_SAVING,
  FINISHED,
  FAILED,
  KILLED
}

public class ApplicationMasterService extends AbstractService {
  
  private ApplicationMasterState currentState = ApplicationMasterState.NEW;
  private final StateMachine<ApplicationMasterState, ApplicationMasterEventType, 
      ApplicationMasterEvent> stateMachine;

  public ApplicationMasterService() {
    super(ApplicationMasterService.class.getName());
    this.stateMachine = createStateMachine();
  }

  private StateMachine<ApplicationMasterState, ApplicationMasterEventType, 
      ApplicationMasterEvent> createStateMachine() {
    return StateMachineFactory.<ApplicationMasterService, ApplicationMasterState,
        ApplicationMasterEventType, ApplicationMasterEvent>newStateMachineFactory(
            ApplicationMasterState.NEW)

        // Transitions from NEW state
        .addTransition(ApplicationMasterState.NEW, ApplicationMasterState.INITED,
            ApplicationMasterEventType.INIT, new InitTransition())

        // Transitions from INITED state  
        .addTransition(ApplicationMasterState.INITED, ApplicationMasterState.STARTED,
            ApplicationMasterEventType.START, new StartTransition())

        // Transitions from STARTED state
        .addTransition(ApplicationMasterState.STARTED, ApplicationMasterState.RUNNING,
            ApplicationMasterEventType.REGISTERED, new RegisteredTransition())

        // Transitions from RUNNING state
        .addTransition(ApplicationMasterState.RUNNING, ApplicationMasterState.FINAL_SAVING,
            ApplicationMasterEventType.FINISH, new FinishTransition())

        .addTransition(ApplicationMasterState.RUNNING, ApplicationMasterState.KILLED,
            ApplicationMasterEventType.KILL, new KillTransition())

        // Transitions to FINISHED state
        .addTransition(ApplicationMasterState.FINAL_SAVING, ApplicationMasterState.FINISHED,
            ApplicationMasterEventType.ATTEMPT_FINISHED, new AttemptFinishedTransition())

        .installTopology();
  }
}
```

**生命周期阶段**：

**NEW → INITED**：ApplicationMaster初始化，加载配置和设置环境。

**INITED → STARTED**：启动各种服务组件，准备与RM和NM通信。

**STARTED → RUNNING**：向ResourceManager注册，开始正常的应用执行。

**RUNNING → FINAL_SAVING**：应用完成，保存最终状态。

**FINAL_SAVING → FINISHED**：清理资源，向ResourceManager注销。

### 10.2.3 ApplicationMaster与其他组件的交互

ApplicationMaster需要与YARN的多个组件进行交互：

```java
public class ApplicationMasterProtocolImpl implements ApplicationMasterProtocol {

  @Override
  public RegisterApplicationMasterResponse registerApplicationMaster(
      RegisterApplicationMasterRequest request) throws YarnException, IOException {
    
    ApplicationAttemptId applicationAttemptId = request.getApplicationAttemptId();
    ApplicationId appId = applicationAttemptId.getApplicationId();
    
    LOG.info("Registering app attempt : " + applicationAttemptId);
    
    RMApp app = this.rmContext.getRMApps().get(appId);
    ApplicationAttempt attempt = app.getCurrentAppAttempt();
    
    RegisterApplicationMasterResponse response = 
        RegisterApplicationMasterResponse.newInstance(
            this.rmContext.getConfigurationProvider()
                .getConfiguration(this.rmContext.getYarnConfiguration(), appId),
            this.rmContext.getQueueInfo(app.getQueue(), false, false),
            this.rmContext.getApplicationACLs().get(appId),
            this.rmContext.getClientToAMTokenSecretManager()
                .getMasterKey(applicationAttemptId).getEncoded(),
            this.rmContext.getContainerTokenSecretManager().getCurrentKey(),
            this.rmContext.getNMTokenSecretManager().getCurrentKey(),
            app.getApplicationSubmissionContext().getResource());

    return response;
  }

  @Override
  public AllocateResponse allocate(AllocateRequest request) 
      throws YarnException, IOException {
    
    ApplicationAttemptId appAttemptId = request.getApplicationAttemptId();
    
    // Process resource requests
    List<ResourceRequest> ask = request.getAskList();
    List<ContainerId> release = request.getReleaseList();
    ResourceBlacklistRequest blacklistRequest = request.getResourceBlacklistRequest();
    
    // Update application progress
    float progress = request.getProgress();
    
    // Allocate containers based on requests
    Allocation allocation = this.rScheduler.allocate(appAttemptId, ask, release,
        blacklistRequest.getBlacklistAdditions(), 
        blacklistRequest.getBlacklistRemovals());

    AllocateResponse allocateResponse = AllocateResponse.newInstance(
        allocation.getResponseId(),
        allocation.getContainers(), 
        allocation.getCompletedContainersStatuses(),
        allocation.getUpdatedNodes(),
        allocation.getResource(),
        allocation.getNMTokens(),
        allocation.getNumClusterNodes(),
        allocation.getPreemptionMessage(),
        allocation.getAMRMToken());

    return allocateResponse;
  }
}
```

**与ResourceManager的交互**：
- 注册ApplicationMaster实例
- 申请和释放容器资源
- 报告应用进度和状态
- 处理资源抢占请求

**与NodeManager的交互**：
- 启动和停止容器
- 监控容器执行状态
- 获取容器日志和诊断信息
- 处理容器失败事件

## 10.3 资源申请与分配机制

### 10.3.1 AMRMClient：与ResourceManager通信

AMRMClient是ApplicationMaster与ResourceManager通信的客户端：

```java
public abstract class AMRMClient<T extends ContainerRequest> extends AbstractService {

  /**
   * Register the application master. This must be called before any 
   * other interaction
   */
  public abstract RegisterApplicationMasterResponse registerApplicationMaster(
      String appHostName, int appHostPort, String appTrackingUrl)
      throws YarnException, IOException;

  /**
   * Request additional containers and receive new container allocations.
   * Requests made via <code>addContainerRequest</code> are sent to the
   * <code>ResourceManager</code>. New containers assigned to the master are
   * retrieved. Status of completed containers and node health updates are
   * also retrieved.
   */
  public abstract AllocateResponse allocate(float progressIndicator)
      throws YarnException, IOException;

  /**
   * Unregister the application master. This must be called in the end.
   */
  public abstract void unregisterApplicationMaster(
      FinalApplicationStatus appStatus, String appMessage, String appTrackingUrl)
      throws YarnException, IOException;

  /**
   * Request containers for resources before calling <code>allocate</code>
   */
  public abstract void addContainerRequest(T req);

  /**
   * Remove previous container request. The previous container request may have 
   * already been sent to the ResourceManager. So even after the remove request 
   * the app must be prepared to receive an allocation for the previous request 
   * even after the remove request
   */
  public abstract void removeContainerRequest(T req);

  /**
   * Release containers assigned by the Resource Manager. If the app cannot use
   * the container or wants to give up the container then it can release them.
   * The app needs to make new requests for the released resource capability if
   * it still needs it.
   */
  public abstract void releaseAssignedContainer(ContainerId containerId);
}
```

**资源请求流程**：
1. **注册阶段**：向ResourceManager注册ApplicationMaster
2. **请求阶段**：提交容器资源请求
3. **分配阶段**：接收ResourceManager分配的容器
4. **释放阶段**：释放不再需要的容器
5. **注销阶段**：应用完成后注销ApplicationMaster

### 10.3.2 ContainerRequest：容器资源请求

ContainerRequest封装了对容器资源的具体需求：

```java
public class ContainerRequest {
  private final Resource capability;
  private final String[] nodes;
  private final String[] racks;
  private final Priority priority;
  private final boolean relaxLocality;
  private final String nodeLabelsExpression;

  public ContainerRequest(Resource capability, String[] nodes, String[] racks,
      Priority priority) {
    this(capability, nodes, racks, priority, true, null);
  }

  public ContainerRequest(Resource capability, String[] nodes, String[] racks,
      Priority priority, boolean relaxLocality, String nodeLabelsExpression) {
    this.capability = capability;
    this.nodes = (nodes != null ? ImmutableList.copyOf(nodes) : null);
    this.racks = (racks != null ? ImmutableList.copyOf(racks) : null);
    this.priority = priority;
    this.relaxLocality = relaxLocality;
    this.nodeLabelsExpression = nodeLabelsExpression;
  }

  public Resource getCapability() {
    return capability;
  }

  public List<String> getNodes() {
    return nodes;
  }

  public List<String> getRacks() {
    return racks;
  }

  public Priority getPriority() {
    return priority;
  }

  public boolean getRelaxLocality() {
    return relaxLocality;
  }

  public String getNodeLabelsExpression() {
    return nodeLabelsExpression;
  }
}
```

**请求参数说明**：
- **capability**：所需的资源量（CPU、内存等）
- **nodes**：首选的节点列表（数据本地性）
- **racks**：首选的机架列表（机架本地性）
- **priority**：请求的优先级
- **relaxLocality**：是否允许放松本地性约束
- **nodeLabelsExpression**：节点标签表达式

### 10.3.3 资源调度策略

ApplicationMaster需要实现智能的资源调度策略：

```java
public class ResourceScheduler {
  
  private final Map<Priority, Map<String, TreeMap<Resource, 
      ResourceRequestInfo>>> remoteRequestsTable = 
      new HashMap<Priority, Map<String, TreeMap<Resource, ResourceRequestInfo>>>();

  public synchronized void addResourceRequest(Priority priority, 
      String resourceName, Resource capability, int numContainers,
      boolean relaxLocality, String labelExpression) {
    
    Map<String, TreeMap<Resource, ResourceRequestInfo>> remoteRequests = 
        this.remoteRequestsTable.get(priority);
    if (remoteRequests == null) {
      remoteRequests = new HashMap<String, TreeMap<Resource, ResourceRequestInfo>>();
      this.remoteRequestsTable.put(priority, remoteRequests);
    }
    
    TreeMap<Resource, ResourceRequestInfo> reqMap = remoteRequests.get(resourceName);
    if (reqMap == null) {
      reqMap = new TreeMap<Resource, ResourceRequestInfo>(new ResourceComparator());
      remoteRequests.put(resourceName, reqMap);
    }
    
    ResourceRequestInfo resourceRequestInfo = reqMap.get(capability);
    if (resourceRequestInfo == null) {
      resourceRequestInfo = new ResourceRequestInfo(priority, resourceName, 
          capability, relaxLocality);
      reqMap.put(capability, resourceRequestInfo);
    }
    
    resourceRequestInfo.remoteRequest.setNumContainers(
        resourceRequestInfo.remoteRequest.getNumContainers() + numContainers);
    
    if (relaxLocality) {
      resourceRequestInfo.remoteRequest.setRelaxLocality(relaxLocality);
    }
    
    if (ResourceRequest.ANY.equals(resourceName)) {
      resourceRequestInfo.remoteRequest.setNodeLabelExpression(labelExpression);
    }
  }

  public synchronized List<ResourceRequest> getResourceRequests() {
    List<ResourceRequest> ret = new ArrayList<ResourceRequest>();
    for (Map<String, TreeMap<Resource, ResourceRequestInfo>> remoteRequestMap : 
         remoteRequestsTable.values()) {
      for (TreeMap<Resource, ResourceRequestInfo> reqMap : remoteRequestMap.values()) {
        for (ResourceRequestInfo resourceRequestInfo : reqMap.values()) {
          ResourceRequest remoteRequest = resourceRequestInfo.remoteRequest;
          if (remoteRequest.getNumContainers() > 0) {
            ret.add(cloneResourceRequest(remoteRequest));
          }
        }
      }
    }
    return ret;
  }
}
```

**调度策略要点**：
- **优先级管理**：根据任务重要性设置不同优先级
- **本地性优化**：优先申请数据本地性好的节点
- **资源聚合**：合并相同类型的资源请求
- **动态调整**：根据执行情况动态调整资源需求

## 10.4 任务调度与监控

### 10.4.1 任务分解与调度

ApplicationMaster需要将应用逻辑分解为可执行的任务：

```java
public abstract class TaskScheduler {
  
  protected final ApplicationMasterService appMasterService;
  protected final Map<TaskId, Task> tasks = new ConcurrentHashMap<>();
  protected final Map<ContainerId, TaskAttempt> runningTaskAttempts = 
      new ConcurrentHashMap<>();
  
  /**
   * Schedule tasks based on available resources and data locality
   */
  public abstract void scheduleTasks(List<Container> allocatedContainers);
  
  /**
   * Handle task completion events
   */
  public abstract void handleTaskCompletion(TaskAttempt taskAttempt, 
      TaskAttemptCompletionEvent event);
  
  /**
   * Handle container allocation events
   */
  public void handleContainerAllocation(List<Container> allocatedContainers) {
    for (Container container : allocatedContainers) {
      // Find the best task for this container
      Task task = selectTaskForContainer(container);
      if (task != null) {
        // Create task attempt
        TaskAttempt taskAttempt = task.createAttempt();
        runningTaskAttempts.put(container.getId(), taskAttempt);
        
        // Launch task in container
        launchTaskInContainer(taskAttempt, container);
      } else {
        // No suitable task found, release container
        appMasterService.releaseContainer(container.getId());
      }
    }
  }
  
  private Task selectTaskForContainer(Container container) {
    // Implement task selection logic based on:
    // 1. Data locality (prefer tasks with data on the same node)
    // 2. Resource requirements (match task needs with container capacity)
    // 3. Task priority and dependencies
    // 4. Load balancing across nodes
    
    String containerNode = container.getNodeId().getHost();
    
    // First, try to find tasks with node-local data
    for (Task task : getUnscheduledTasks()) {
      if (task.hasDataOnNode(containerNode) && 
          task.getResourceRequirement().fitsIn(container.getResource())) {
        return task;
      }
    }
    
    // Second, try to find tasks with rack-local data
    String containerRack = getRackForNode(containerNode);
    for (Task task : getUnscheduledTasks()) {
      if (task.hasDataOnRack(containerRack) && 
          task.getResourceRequirement().fitsIn(container.getResource())) {
        return task;
      }
    }
    
    // Finally, select any compatible task
    for (Task task : getUnscheduledTasks()) {
      if (task.getResourceRequirement().fitsIn(container.getResource())) {
        return task;
      }
    }
    
    return null;
  }
}
```

**任务调度考虑因素**：
- **数据本地性**：优先调度数据本地的任务
- **资源匹配**：确保容器资源满足任务需求
- **负载均衡**：避免某些节点过载
- **任务依赖**：处理任务间的依赖关系

### 10.4.2 容器启动与监控

ApplicationMaster通过NMClient与NodeManager交互启动容器：

```java
public class ContainerLauncher implements NMClientAsync.CallbackHandler {
  
  private final NMClientAsync nmClientAsync;
  private final Map<ContainerId, Container> containers = new ConcurrentHashMap<>();
  
  public void launchContainer(Container container, ContainerLaunchContext ctx) {
    containers.put(container.getId(), container);
    nmClientAsync.startContainerAsync(container, ctx);
  }
  
  @Override
  public void onContainerStarted(ContainerId containerId, 
      Map<String, ByteBuffer> allServiceResponse) {
    LOG.info("Container " + containerId + " started successfully");
    
    Container container = containers.get(containerId);
    if (container != null) {
      // Notify task scheduler that container is running
      taskScheduler.onContainerStarted(container);
    }
  }
  
  @Override
  public void onContainerStatusReceived(ContainerId containerId, 
      ContainerStatus containerStatus) {
    LOG.info("Container " + containerId + " status: " + containerStatus.getState());
    
    // Update task status based on container status
    TaskAttempt taskAttempt = runningTaskAttempts.get(containerId);
    if (taskAttempt != null) {
      taskAttempt.updateStatus(containerStatus);
    }
  }
  
  @Override
  public void onContainerStopped(ContainerId containerId) {
    LOG.info("Container " + containerId + " stopped");
    
    Container container = containers.remove(containerId);
    TaskAttempt taskAttempt = runningTaskAttempts.remove(containerId);
    
    if (taskAttempt != null) {
      // Handle task completion or failure
      handleTaskAttemptCompletion(taskAttempt, container);
    }
  }
  
  @Override
  public void onStartContainerError(ContainerId containerId, Throwable t) {
    LOG.error("Failed to start container " + containerId, t);
    
    // Handle container start failure
    Container container = containers.remove(containerId);
    TaskAttempt taskAttempt = runningTaskAttempts.remove(containerId);
    
    if (taskAttempt != null) {
      taskAttempt.markAsFailed(t);
      // Retry task on different container if possible
      retryTaskAttempt(taskAttempt);
    }
  }
  
  private void handleTaskAttemptCompletion(TaskAttempt taskAttempt, Container container) {
    if (taskAttempt.isSuccessful()) {
      // Task completed successfully
      taskScheduler.onTaskCompleted(taskAttempt);
    } else {
      // Task failed, decide whether to retry
      if (taskAttempt.shouldRetry()) {
        retryTaskAttempt(taskAttempt);
      } else {
        taskScheduler.onTaskFailed(taskAttempt);
      }
    }
    
    // Release container resources
    amRMClient.releaseAssignedContainer(container.getId());
  }
}
```

**容器监控功能**：
- 跟踪容器启动状态
- 监控容器执行进度
- 处理容器异常和失败
- 收集容器性能指标

### 10.4.3 进度报告与状态更新

ApplicationMaster需要定期向ResourceManager报告应用进度：

```java
public class ProgressReporter extends Thread {
  
  private final AMRMClientAsync<ContainerRequest> amRMClient;
  private final ApplicationMaster applicationMaster;
  private volatile boolean stopped = false;
  private final int reportInterval;
  
  public ProgressReporter(AMRMClientAsync<ContainerRequest> amRMClient,
      ApplicationMaster applicationMaster, int reportInterval) {
    this.amRMClient = amRMClient;
    this.applicationMaster = applicationMaster;
    this.reportInterval = reportInterval;
    setDaemon(true);
    setName("ProgressReporter");
  }
  
  @Override
  public void run() {
    while (!stopped && !Thread.currentThread().isInterrupted()) {
      try {
        // Calculate application progress
        float progress = calculateProgress();
        
        // Send heartbeat to ResourceManager
        amRMClient.allocate(progress);
        
        // Sleep until next report interval
        Thread.sleep(reportInterval);
        
      } catch (InterruptedException e) {
        LOG.info("ProgressReporter interrupted");
        break;
      } catch (Exception e) {
        LOG.error("Error in progress reporting", e);
      }
    }
  }
  
  private float calculateProgress() {
    int totalTasks = applicationMaster.getTotalTaskCount();
    int completedTasks = applicationMaster.getCompletedTaskCount();
    
    if (totalTasks == 0) {
      return 0.0f;
    }
    
    return (float) completedTasks / totalTasks;
  }
  
  public void stop() {
    stopped = true;
    interrupt();
  }
}
```

**进度计算方式**：
- 基于已完成任务数量的比例
- 考虑任务的权重和重要性
- 包含数据处理进度信息
- 反映资源使用效率

## 10.5 容错处理与恢复机制

### 10.5.1 任务级容错处理

ApplicationMaster需要处理各种任务失败情况：

```java
public class FaultTolerantTaskManager {
  
  private final Map<TaskId, TaskInfo> taskInfoMap = new ConcurrentHashMap<>();
  private final int maxTaskAttempts;
  private final long taskTimeoutMs;
  
  public class TaskInfo {
    private final TaskId taskId;
    private final List<TaskAttempt> attempts = new ArrayList<>();
    private TaskState state = TaskState.NEW;
    private int failureCount = 0;
    
    public boolean shouldRetry() {
      return failureCount < maxTaskAttempts && 
             state != TaskState.SUCCEEDED &&
             state != TaskState.KILLED;
    }
    
    public TaskAttempt createNewAttempt() {
      TaskAttemptId attemptId = new TaskAttemptId(taskId, attempts.size());
      TaskAttempt attempt = new TaskAttempt(attemptId, this);
      attempts.add(attempt);
      return attempt;
    }
  }
  
  public void handleTaskAttemptFailure(TaskAttempt failedAttempt, Throwable cause) {
    TaskInfo taskInfo = taskInfoMap.get(failedAttempt.getTaskId());
    taskInfo.failureCount++;
    
    LOG.warn("Task attempt " + failedAttempt.getAttemptId() + " failed", cause);
    
    // Analyze failure cause
    FailureType failureType = analyzeFailure(cause);
    
    switch (failureType) {
      case CONTAINER_FAILURE:
        // Container failed, retry on different node
        if (taskInfo.shouldRetry()) {
          retryTaskOnDifferentNode(taskInfo);
        } else {
          markTaskAsFailed(taskInfo, "Too many container failures");
        }
        break;
        
      case APPLICATION_ERROR:
        // Application logic error, don't retry
        markTaskAsFailed(taskInfo, "Application error: " + cause.getMessage());
        break;
        
      case RESOURCE_SHORTAGE:
        // Temporary resource issue, retry with backoff
        if (taskInfo.shouldRetry()) {
          scheduleRetryWithBackoff(taskInfo);
        } else {
          markTaskAsFailed(taskInfo, "Persistent resource shortage");
        }
        break;
        
      case NETWORK_ERROR:
        // Network issue, retry immediately
        if (taskInfo.shouldRetry()) {
          retryTaskImmediately(taskInfo);
        } else {
          markTaskAsFailed(taskInfo, "Persistent network errors");
        }
        break;
    }
  }
  
  private FailureType analyzeFailure(Throwable cause) {
    if (cause instanceof ContainerExitException) {
      ContainerExitException cee = (ContainerExitException) cause;
      if (cee.getExitCode() == ContainerExitStatus.ABORTED) {
        return FailureType.CONTAINER_FAILURE;
      } else if (cee.getExitCode() > 0) {
        return FailureType.APPLICATION_ERROR;
      }
    } else if (cause instanceof IOException) {
      return FailureType.NETWORK_ERROR;
    } else if (cause instanceof ResourceException) {
      return FailureType.RESOURCE_SHORTAGE;
    }
    
    return FailureType.UNKNOWN;
  }
  
  private void retryTaskOnDifferentNode(TaskInfo taskInfo) {
    // Add failed node to blacklist for this task
    String failedNode = getFailedNode(taskInfo.getLastAttempt());
    taskInfo.addBlacklistedNode(failedNode);
    
    // Request new container excluding blacklisted nodes
    requestContainerForTask(taskInfo, taskInfo.getBlacklistedNodes());
  }
  
  private void scheduleRetryWithBackoff(TaskInfo taskInfo) {
    long backoffDelay = calculateBackoffDelay(taskInfo.failureCount);
    
    // Schedule retry after backoff delay
    Timer timer = new Timer();
    timer.schedule(new TimerTask() {
      @Override
      public void run() {
        requestContainerForTask(taskInfo, null);
      }
    }, backoffDelay);
  }
}
```

**容错策略**：
- **重试机制**：失败任务自动重试，最大重试次数限制
- **节点黑名单**：避免在频繁失败的节点上重试
- **退避策略**：重试间隔逐渐增加，避免系统过载
- **失败分析**：根据失败原因采取不同的处理策略

### 10.5.2 ApplicationMaster容错

ApplicationMaster本身也需要容错机制：

```java
public class ApplicationMasterRecovery {
  
  private final StateStore stateStore;
  private final ApplicationAttemptId appAttemptId;
  
  public void saveState(ApplicationState state) throws IOException {
    // Persist application state to reliable storage
    stateStore.storeApplicationState(appAttemptId, state);
  }
  
  public ApplicationState recoverState() throws IOException {
    // Recover application state from storage
    return stateStore.loadApplicationState(appAttemptId);
  }
  
  public void recoverFromPreviousAttempt() throws YarnException, IOException {
    LOG.info("Recovering ApplicationMaster from previous attempt");
    
    // Load previous state
    ApplicationState previousState = recoverState();
    if (previousState == null) {
      LOG.info("No previous state found, starting fresh");
      return;
    }
    
    // Recover task states
    recoverTasks(previousState.getTaskStates());
    
    // Recover container allocations
    recoverContainers(previousState.getContainerStates());
    
    // Recover resource requests
    recoverResourceRequests(previousState.getResourceRequests());
    
    LOG.info("ApplicationMaster recovery completed");
  }
  
  private void recoverTasks(Map<TaskId, TaskState> taskStates) {
    for (Map.Entry<TaskId, TaskState> entry : taskStates.entrySet()) {
      TaskId taskId = entry.getKey();
      TaskState state = entry.getValue();
      
      Task task = createTask(taskId);
      task.setState(state);
      
      if (state == TaskState.RUNNING) {
        // Task was running, need to check if still alive
        checkTaskStatus(task);
      } else if (state == TaskState.NEW || state == TaskState.SCHEDULED) {
        // Task needs to be rescheduled
        rescheduleTask(task);
      }
    }
  }
  
  private void recoverContainers(Map<ContainerId, ContainerState> containerStates) {
    for (Map.Entry<ContainerId, ContainerState> entry : containerStates.entrySet()) {
      ContainerId containerId = entry.getKey();
      ContainerState state = entry.getValue();
      
      if (state == ContainerState.RUNNING) {
        // Verify container is still running
        verifyContainerStatus(containerId);
      }
    }
  }
}
```

**AM容错特性**：
- **状态持久化**：关键状态信息持久化到可靠存储
- **自动重启**：AM失败后自动在其他节点重启
- **状态恢复**：新AM实例恢复之前的执行状态
- **容器恢复**：重新连接到仍在运行的容器

## 10.6 本章小结

本章深入分析了YARN ApplicationMaster的架构设计和实现机制。ApplicationMaster作为应用程序在YARN集群中的代理，承担着资源协商、任务调度、容错处理等关键职责。

**核心要点**：

**架构设计**：ApplicationMaster采用事件驱动的架构，通过状态机管理生命周期，与RM和NM异步通信。

**资源管理**：通过AMRMClient与ResourceManager协商资源，实现智能的资源申请和释放策略。

**任务调度**：将应用逻辑分解为任务，基于数据本地性和资源可用性进行调度。

**容错机制**：实现了任务级和AM级的容错处理，包括重试、恢复、状态持久化等。

**监控报告**：定期向ResourceManager报告进度，监控任务执行状态。

ApplicationMaster的设计体现了YARN"一个集群，多种计算框架"的核心理念，为不同类型的大数据应用提供了统一的资源管理接口。理解ApplicationMaster的实现原理，对于开发高效的YARN应用具有重要指导意义。
