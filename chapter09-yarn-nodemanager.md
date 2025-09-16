# 第9章：YARN NodeManager与容器管理

## 9.1 引言

NodeManager是YARN架构中的工作节点守护进程，负责管理单个节点上的资源和容器。如果说ResourceManager是YARN的"大脑"，那么NodeManager就是YARN的"手脚"，它直接执行具体的任务，管理容器的生命周期，监控资源使用情况，并与ResourceManager保持通信。

NodeManager的设计体现了分布式系统中"分而治之"的核心思想。每个NodeManager只需要关注本节点的资源管理，而不需要了解整个集群的全局状态，这种设计大大简化了系统的复杂性，提高了系统的可扩展性和容错能力。

NodeManager的核心职责包括容器生命周期管理、资源监控、日志聚合、安全隔离等。它需要在保证资源利用率的同时，确保不同应用之间的隔离性和安全性。本章将深入分析NodeManager的架构设计、容器管理机制、资源监控策略以及安全隔离实现，帮助读者理解YARN节点级资源管理的核心原理。

## 9.2 NodeManager架构概述

### 9.2.1 NodeManager的核心职责

NodeManager在YARN架构中承担着多重关键职责：

```java
/**
 * The NodeManager is responsible for launching and managing containers
 * on a node. It also monitors the health of the node and reports back
 * to the ResourceManager.
 */
public class NodeManager extends CompositeService
    implements EventHandler<NodeManagerEvent> {

  private Context context;
  private AsyncDispatcher dispatcher;
  private ContainerManagerImpl containerManager;
  private NodeStatusUpdater nodeStatusUpdater;
  private NodeResourceMonitor nodeResourceMonitor;
  private LocalDirsHandlerService dirsHandler;
  private WebServer webServer;
  private LogAggregationService logAggregationService;
  private NodeHealthCheckerService nodeHealthChecker;
  private LocalizationService localizationService;
  private ContainersLauncher containersLauncher;
  private ContainersMonitor containersMonitor;
  private ApplicationACLsManager aclsManager;
  private NMTokenSecretManagerInNM nmTokenSecretManager;
}
```

**容器管理**：
- 接收ResourceManager的容器启动请求
- 管理容器的完整生命周期
- 监控容器的资源使用情况
- 处理容器的停止和清理

**资源监控**：
- 监控节点的CPU、内存、磁盘、网络使用情况
- 跟踪容器的资源消耗
- 检测资源超限并采取相应措施

**节点健康管理**：
- 定期检查节点健康状况
- 监控磁盘空间和系统负载
- 向ResourceManager报告节点状态

**日志管理**：
- 收集容器的日志信息
- 实现日志聚合和上传
- 提供日志查询接口

### 9.2.2 NodeManager的组件架构

NodeManager采用了事件驱动的组件化架构：

```java
protected void serviceInit(Configuration conf) throws Exception {
  Context context = new NMContext(new NMContainerTokenSecretManager(
      conf), new NMTokenSecretManagerInNM(), null,
      new ApplicationACLsManager(conf), new NMStateStoreService(), false);

  this.context = context;
  this.aclsManager = context.getApplicationACLsManager();

  // Create the async-dispatcher.
  this.dispatcher = createNMDispatcher();

  nodeHealthChecker =
      new NodeHealthCheckerService(
          getNodeHealthScriptRunner(conf), getNodeHealthChecker(conf));
  addService(nodeHealthChecker);

  dirsHandler = nodeHealthChecker.getDiskHandler();

  this.nodeResourceMonitor = createNodeResourceMonitor();
  addService(nodeResourceMonitor);

  containerManager = createContainerManager(context, exec, del,
      nodeStatusUpdater, this.aclsManager, dirsHandler);
  addService(containerManager);
  ((NMContext) context).setContainerManager(containerManager);

  WebServer webServer = createWebServer(context, containerManager
      .getContainersMonitor(), this.aclsManager, dirsHandler);
  addService(webServer);
  ((NMContext) context).setWebServer(webServer);

  nodeStatusUpdater =
      createNodeStatusUpdater(context, dispatcher, nodeHealthChecker);
  addService(nodeStatusUpdater);

  nodeLabelsProvider = createNodeLabelsProvider(conf);
  if (nodeLabelsProvider != null) {
    addService(nodeLabelsProvider);
  }

  // StatusUpdater should be added last so that it get started last
  // so that we make sure everything is up before registering with RM.
  addIfService(nodeStatusUpdater);
  ((NMContext) context).setNodeStatusUpdater(nodeStatusUpdater);
}
```

**核心服务组件**：

**ContainerManager**：容器管理器，负责容器的启动、停止和监控。

**NodeStatusUpdater**：节点状态更新器，与ResourceManager通信。

**NodeResourceMonitor**：节点资源监控器，监控系统资源使用情况。

**NodeHealthChecker**：节点健康检查器，检查节点健康状况。

**支持服务组件**：

**LocalizationService**：本地化服务，负责下载和管理应用资源。

**LogAggregationService**：日志聚合服务，收集和上传容器日志。

**ContainersLauncher**：容器启动器，负责实际启动容器进程。

**ContainersMonitor**：容器监控器，监控容器资源使用情况。

### 9.2.3 事件驱动架构

NodeManager使用事件驱动模式来协调各个组件：

```java
public enum NodeManagerEventType {
  SHUTDOWN,
  RESYNC
}

public class NodeManagerEvent extends AbstractEvent<NodeManagerEventType> {
  public NodeManagerEvent(NodeManagerEventType type) {
    super(type);
  }
}

@Override
public void handle(NodeManagerEvent event) {
  switch (event.getType()) {
  case SHUTDOWN:
    shutDown();
    break;
  case RESYNC:
    resyncWithRM();
    break;
  default:
    LOG.warn("Invalid event " + event.getType() + ". Ignoring.");
  }
}
```

**事件类型**：
- **容器事件**：容器启动、停止、完成等
- **应用事件**：应用初始化、完成、清理等
- **本地化事件**：资源下载、本地化完成等
- **监控事件**：资源超限、健康检查等

**事件流转**：
- 事件通过AsyncDispatcher进行异步分发
- 各组件注册感兴趣的事件类型
- 事件处理器负责具体的业务逻辑

## 9.3 容器生命周期管理

### 9.3.1 ContainerManager：容器管理核心

ContainerManager是NodeManager中负责容器管理的核心组件：

```java
public class ContainerManagerImpl extends CompositeService implements
    ContainerManager {

  private final Context context;
  private final ContainersMonitor containersMonitor;
  private final LogHandler logHandler;
  private final LogAggregationService logAggregationService;
  private final ResourceLocalizationService rsrcLocalizationService;
  private final ContainersLauncher containersLauncher;
  private final AuxServices auxServices;
  private final NodeManagerMetrics metrics;

  private final NodeStatusUpdater nodeStatusUpdater;
  private final AsyncDispatcher dispatcher;

  @Override
  public StartContainersResponse startContainers(
      StartContainersRequest requests) throws YarnException, IOException {

    if (blockNewContainerRequests.get()) {
      throw new NMNotYetReadyException(
        "Rejecting new containers as NodeManager has not"
            + " yet connected with ResourceManager");
    }
    UserGroupInformation remoteUgi = getRemoteUgi();
    NMTokenIdentifier nmTokenIdentifier = selectNMTokenIdentifier(remoteUgi);
    authorizeUser(remoteUgi, nmTokenIdentifier);
    List<ContainerId> succeededContainers = new ArrayList<ContainerId>();
    Map<ContainerId, SerializedException> failedContainers =
        new HashMap<ContainerId, SerializedException>();

    for (StartContainerRequest request : requests.getStartContainerRequests()) {
      ContainerId containerId = null;
      try {
        ContainerTokenIdentifier containerTokenIdentifier =
            BuilderUtils.newContainerTokenIdentifier(request.getContainerToken());
        verifyAndGetContainerTokenIdentifier(request.getContainerToken(),
            containerTokenIdentifier);
        containerId = containerTokenIdentifier.getContainerID();
        startContainerInternal(nmTokenIdentifier, containerTokenIdentifier,
            request);
        succeededContainers.add(containerId);
      } catch (YarnException e) {
        failedContainers.put(containerId, SerializedException.newInstance(e));
      } catch (InvalidToken ie) {
        failedContainers.put(containerId, SerializedException.newInstance(ie));
      } catch (IOException e) {
        throw RPCUtil.getRemoteException(e);
      }
    }

    return StartContainersResponse.newInstance(getAuxServiceMetaData(),
        succeededContainers, failedContainers);
  }
}
```

**容器启动流程**：
1. **权限验证**：验证请求者的身份和权限
2. **Token验证**：验证容器Token的有效性
3. **资源检查**：检查节点是否有足够资源
4. **容器初始化**：创建容器对象并初始化
5. **资源本地化**：下载应用所需的资源文件
6. **容器启动**：启动容器进程

### 9.3.2 容器状态机

NodeManager使用状态机模式管理容器生命周期：

```java
public enum ContainerState {
  NEW,
  LOCALIZING,
  LOCALIZATION_FAILED,
  LOCALIZED,
  RUNNING,
  EXITED_WITH_SUCCESS,
  EXITED_WITH_FAILURE,
  KILLING,
  CONTAINER_CLEANEDUP_AFTER_KILL,
  CONTAINER_RESOURCES_CLEANINGUP,
  DONE
}

public enum ContainerEventType {
  INIT_CONTAINER,
  CONTAINER_LAUNCHED,
  CONTAINER_EXITED,
  CONTAINER_KILLED_ON_REQUEST,
  KILL_CONTAINER,
  CONTAINER_DONE,
  CONTAINER_RESOURCES_CLEANEDUP,
  UPDATE_DIAGNOSTICS_MSG,
  CONTAINER_INITED,
  RESOURCE_LOCALIZED,
  RESOURCE_FAILED
}
```

**状态转换逻辑**：

**NEW → LOCALIZING**：开始资源本地化过程。

**LOCALIZING → LOCALIZED**：资源本地化完成。

**LOCALIZED → RUNNING**：容器成功启动。

**RUNNING → EXITED_WITH_SUCCESS/FAILURE**：容器执行完成。

**任意状态 → KILLING**：接收到杀死容器的请求。

### 9.3.3 容器启动器

ContainersLauncher负责实际启动容器进程：

```java
public class ContainersLauncher extends AbstractService
    implements EventHandler<ContainersLauncherEvent> {

  private final Context context;
  private final ContainerExecutor exec;
  private final Dispatcher dispatcher;
  private final ContainerManager containerManager;

  private final ExecutorService containerLauncher =
    Executors.newCachedThreadPool(
        new ThreadFactoryBuilder()
          .setNameFormat("ContainersLauncher #%d")
          .build());
  private final Map<ContainerId,RunningContainer> running =
    new ConcurrentHashMap<ContainerId,RunningContainer>();

  @Override
  public void handle(ContainersLauncherEvent event) {
    // TODO: validations
    ContainerId containerId = event.getContainerID();
    switch (event.getType()) {
      case LAUNCH_CONTAINER:
        Application app = context.getApplications().get(
            containerId.getApplicationAttemptId().getApplicationId());

        ContainerLaunch launch =
            new ContainerLaunch(context, getConfig(), dispatcher, exec, app,
              event.getContainer(), dirsHandler, containerManager);
        containerLauncher.submit(launch);
        running.put(containerId, launch);
        break;
      case RECOVER_CONTAINER:
        app = context.getApplications().get(
            containerId.getApplicationAttemptId().getApplicationId());
        launch = new RecoveredContainerLaunch(context, getConfig(), dispatcher,
            exec, app, event.getContainer(), dirsHandler, containerManager);
        containerLauncher.submit(launch);
        running.put(containerId, launch);
        break;
      case CLEANUP_CONTAINER:
        ContainerCleanup cleanup = new ContainerCleanup(context, getConfig(),
            containerId, dirsHandler, nodeManagerMetrics);
        containerLauncher.submit(cleanup);
        break;
    }
  }
}
```

**启动机制**：
- 使用线程池异步处理容器启动请求
- 每个容器启动任务在独立线程中执行
- 支持容器恢复和清理操作

## 9.4 资源监控与管理

### 9.4.1 NodeResourceMonitor：节点资源监控

NodeResourceMonitor负责监控节点的整体资源使用情况：

```java
public class NodeResourceMonitorImpl extends AbstractService implements
    NodeResourceMonitor {

  private float vmemRatio;
  private float pmemRatio;
  private float vcoresRatio;
  private long monitoringInterval;
  private MonitoringThread monitoringThread;

  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private ResourceCalculatorProcessTree resourceCalculatorProcessTree;

  @Override
  protected void serviceStart() throws Exception {
    this.monitoringThread = new MonitoringThread();
    this.monitoringThread.start();
    super.serviceStart();
  }

  private class MonitoringThread extends Thread {
    MonitoringThread() {
      super("Node Resource Monitor");
    }

    @Override
    public void run() {
      while (!this.isInterrupted()) {
        // Get node resource usage
        long vmemBytes = resourceCalculatorPlugin.getVirtualMemorySize();
        long pmemBytes = resourceCalculatorPlugin.getPhysicalMemorySize();
        long vcores = resourceCalculatorPlugin.getNumProcessors();

        float vmemUsageRatio = (float)vmemBytes /
            resourceCalculatorPlugin.getVirtualMemorySize();
        float pmemUsageRatio = (float)pmemBytes /
            resourceCalculatorPlugin.getPhysicalMemorySize();
        float vcoresUsageRatio = (float)vcores /
            resourceCalculatorPlugin.getNumProcessors();

        // Check if resource usage exceeds thresholds
        if (vmemUsageRatio > vmemRatio) {
          LOG.warn("Virtual memory usage " + vmemUsageRatio +
              " has crossed the threshold " + vmemRatio);
        }

        if (pmemUsageRatio > pmemRatio) {
          LOG.warn("Physical memory usage " + pmemUsageRatio +
              " has crossed the threshold " + pmemRatio);
        }

        try {
          Thread.sleep(monitoringInterval);
        } catch (InterruptedException e) {
          LOG.warn(NodeResourceMonitorImpl.class.getName()
              + " is interrupted. Exiting.");
          break;
        }
      }
    }
  }
}
```

**监控指标**：
- **内存使用率**：虚拟内存和物理内存使用情况
- **CPU使用率**：CPU核心使用情况
- **磁盘使用率**：磁盘空间和I/O使用情况
- **网络使用率**：网络带宽使用情况

**监控策略**：
- 定期采样资源使用数据
- 与配置的阈值进行比较
- 超限时触发告警或保护措施

### 9.4.2 ContainersMonitor：容器资源监控

ContainersMonitor专门监控容器的资源使用情况：

```java
public class ContainersMonitorImpl extends AbstractService implements
    ContainersMonitor {

  private final static Log LOG = LogFactory
      .getLog(ContainersMonitorImpl.class);

  private long monitoringInterval;
  private MonitoringThread monitoringThread;
  private boolean containerMetricsEnabled;
  private long containerMetricsPeriodMs;
  private long containerMetricsUnregisterDelayMs;

  final Map<ContainerId, ProcessTreeInfo> trackingContainers =
      new HashMap<ContainerId, ProcessTreeInfo>();

  final ContainerExecutor containerExecutor;
  private final Dispatcher dispatcher;
  private final Context context;
  private ResourceCalculatorPlugin resourceCalculatorPlugin;
  private Configuration conf;
  private Class<? extends ResourceCalculatorProcessTree> processTreeClass;

  private long maxVmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;
  private long maxPmemAllottedForContainers = UNKNOWN_MEMORY_LIMIT;

  private boolean pmemCheckEnabled;
  private boolean vmemCheckEnabled;

  @Override
  public void handle(ContainersMonitorEvent monitoringEvent) {

    ContainerId containerId = monitoringEvent.getContainerId();
    switch (monitoringEvent.getType()) {
    case START_MONITORING_CONTAINER:
      onStartMonitoringContainer(monitoringEvent);
      break;
    case STOP_MONITORING_CONTAINER:
      onStopMonitoringContainer(monitoringEvent);
      break;
    case CHANGE_MONITORING_CONTAINER_RESOURCE:
      onChangeMonitoringContainerResource(monitoringEvent);
      break;
    }
  }

  private void onStartMonitoringContainer(
      ContainersMonitorEvent monitoringEvent) {
    ContainerId containerId = monitoringEvent.getContainerId();
    LOG.info("Starting resource-monitoring for " + containerId);

    trackingContainers.put(containerId,
        new ProcessTreeInfo(containerId, null, null,
            monitoringEvent.getLaunchContext().getResource()));
  }
}
```

**监控功能**：
- 跟踪每个容器的进程树
- 监控容器的内存和CPU使用情况
- 检测资源超限并采取措施
- 收集容器性能指标

**资源限制执行**：
- 内存超限时杀死容器
- CPU使用率控制
- 磁盘I/O限制

## 9.5 本地化服务

### 9.5.1 ResourceLocalizationService：资源本地化

ResourceLocalizationService负责将应用所需的资源文件下载到本地：

```java
public class ResourceLocalizationService extends CompositeService
    implements EventHandler<LocalizationEvent>, LocalizationProtocol {

  private Server server;
  private InetSocketAddress localizationServerAddress;
  private long cacheTargetSize;
  private long cacheCleanupPeriod;
  private final ContainerExecutor exec;
  private final Dispatcher dispatcher;
  private final DeletionService delService;
  private LocalResourcesTracker publicRsrc;

  private LocalDirsHandlerService dirsHandler;
  private Context nmContext;

  private final Map<String, LocalResourcesTracker> privateRsrc =
      new ConcurrentHashMap<String, LocalResourcesTracker>();
  private final Map<String, LocalResourcesTracker> appRsrc =
      new ConcurrentHashMap<String, LocalResourcesTracker>();

  @Override
  public void handle(LocalizationEvent event) {
    // TODO: create log dir as $logdir/$user/$appId
    switch (event.getType()) {
    case INIT_APPLICATION_RESOURCES:
      handleInitApplicationResources(
          ((ApplicationLocalizationEvent)event));
      break;
    case INIT_CONTAINER_RESOURCES:
      handleInitContainerResources((ContainerLocalizationRequestEvent) event);
      break;
    case CACHE_CLEANUP:
      handleCacheCleanup((LocalizationEvent)event);
      break;
    case CLEANUP_CONTAINER_RESOURCES:
      handleCleanupContainerResources(
          (ContainerLocalizationCleanupEvent)event);
      break;
    case DESTROY_APPLICATION_RESOURCES:
      handleDestroyApplicationResources(
          ((ApplicationLocalizationEvent)event));
      break;
    default:
      throw new YarnRuntimeException("Unknown localization event type: "
          + event.getType());
    }
  }

  private void handleInitContainerResources(
      ContainerLocalizationRequestEvent rsrcReqs) {
    Container c = rsrcReqs.getContainer();
    LocalizerContext ctxt = new LocalizerContext(
        c.getUser(), c.getContainerId(), c.getCredentials());
    Map<LocalResourceVisibility, Collection<LocalResourceRequest>> rsrcs =
      rsrcReqs.getRequestedResources();
    for (Map.Entry<LocalResourceVisibility, Collection<LocalResourceRequest>> e :
         rsrcs.entrySet()) {
      LocalResourceVisibility vis = e.getKey();
      Collection<LocalResourceRequest> reqs = e.getValue();
      switch (vis) {
      case PUBLIC:
        publicRsrc.handle(new ResourceRequestEvent(reqs, ctxt));
        break;
      case PRIVATE:
        privateRsrc.get(ctxt.getUser())
          .handle(new ResourceRequestEvent(reqs, ctxt));
        break;
      case APPLICATION:
        appRsrc.get(
            ConverterUtils.toString(
              c.getContainerId().getApplicationAttemptId().getApplicationId()))
          .handle(new ResourceRequestEvent(reqs, ctxt));
        break;
      }
    }
  }
}
```

**本地化类型**：
- **PUBLIC**：所有用户共享的资源
- **PRIVATE**：用户私有资源
- **APPLICATION**：应用级别的资源

**本地化流程**：
1. 解析容器的资源需求
2. 根据资源可见性分类处理
3. 下载远程资源到本地缓存
4. 创建符号链接到容器工作目录

### 9.5.2 LocalResourcesTracker：本地资源跟踪

LocalResourcesTracker跟踪和管理本地化的资源：

```java
class LocalResourcesTrackerImpl implements LocalResourcesTracker {

  private final String user;
  private final ApplicationId appId;
  private final Dispatcher dispatcher;
  private final Map<LocalResourceRequest, LocalizedResource> localrsrc;
  private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
  private final Lock readLock = readWriteLock.readLock();
  private final Lock writeLock = readWriteLock.writeLock();

  @Override
  public void handle(ResourceEvent event) {
    LocalResourceRequest req = event.getLocalResourceRequest();
    LocalizedResource rsrc = localrsrc.get(req);
    switch (event.getType()) {
    case REQUEST:
      if (rsrc != null && (!isResourcePresent(rsrc))) {
        LOG.info("Resource " + rsrc.getLocalPath()
            + " is missing, localizing it again");
        removeResource(req);
        rsrc = null;
      }
      if (null == rsrc) {
        rsrc = new LocalizedResource(req, dispatcher);
        localrsrc.put(req, rsrc);
      }
      break;
    case LOCALIZED:
      if (ResourceState.LOCALIZED == rsrc.getState()) {
        LOG.warn("Resource " + rsrc + " is already localized");
        return;
      }
      rsrc.handle(event);
      if (ResourceState.LOCALIZED == rsrc.getState()) {
        updateResource(req, rsrc);
      }
      break;
    case RELEASE:
      if (null == rsrc) {
        LOG.warn("Release unknown resource " + req);
        return;
      }
      rsrc.handle(event);
      break;
    case RECOVERED:
      if (rsrc != null) {
        LOG.warn("Resource " + rsrc + " is already present");
        return;
      }
      rsrc = recoverResource(req, (ResourceRecoveredEvent) event);
      localrsrc.put(req, rsrc);
      break;
    }
  }
}
```

**资源状态管理**：
- **INIT**：资源初始化状态
- **DOWNLOADING**：正在下载资源
- **LOCALIZED**：资源本地化完成
- **FAILED**：本地化失败

## 9.6 日志聚合服务

### 9.6.1 LogAggregationService：日志聚合

LogAggregationService负责收集和聚合容器日志：

```java
public class LogAggregationService extends AbstractService implements
    LogAggregationContext {

  private final Context context;
  private final DeletionService deletionService;
  private final Dispatcher dispatcher;

  private LocalDirsHandlerService dirsHandler;
  private NodeId nodeId;
  private final ConcurrentMap<ApplicationId, AppLogAggregator> appLogAggregators;
  private final ExecutorService threadPool;

  @Override
  public void handle(LogHandlerEvent event) {
    switch (event.getType()) {
      case APPLICATION_STARTED:
        LogHandlerAppStartedEvent appStartEvent =
            (LogHandlerAppStartedEvent) event;
        initApp(appStartEvent.getApplicationId(), appStartEvent.getUser(),
            appStartEvent.getCredentials(),
            appStartEvent.getApplicationAcls(),
            appStartEvent.getLogAggregationContext(),
            appStartEvent.getRecoveredAppLogInitedTime());
        break;
      case CONTAINER_FINISHED:
        LogHandlerContainerFinishedEvent containerFinishEvent =
            (LogHandlerContainerFinishedEvent) event;
        stopContainer(containerFinishEvent.getContainerId(),
            containerFinishEvent.getExitCode());
        break;
      case APPLICATION_FINISHED:
        LogHandlerAppFinishedEvent appFinishEvent =
            (LogHandlerAppFinishedEvent) event;
        stopApp(appFinishEvent.getApplicationId());
        break;
      default:
        ; // Ignore
    }
  }

  private void initApp(final ApplicationId appId, String user,
      Credentials credentials, Map<ApplicationAccessType, String> appAcls,
      LogAggregationContext logAggregationContext,
      long recoveredLogInitedTime) {
    ApplicationEvent eventResponse;
    try {
      verifyAndCreateRemoteLogDir(getConfig());
      initAppAggregator(appId, user, credentials, appAcls,
          logAggregationContext, recoveredLogInitedTime);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_INITED);
    } catch (YarnRuntimeException e) {
      LOG.warn("Application failed to init aggregation", e);
      eventResponse = new ApplicationEvent(appId,
          ApplicationEventType.APPLICATION_LOG_HANDLING_FAILED);
    }
    this.dispatcher.getEventHandler().handle(eventResponse);
  }
}
```

**日志聚合流程**：
1. 应用启动时初始化日志聚合器
2. 容器完成时收集容器日志
3. 应用完成时上传聚合日志
4. 清理本地日志文件

### 9.6.2 AppLogAggregator：应用日志聚合器

AppLogAggregator负责单个应用的日志聚合：

```java
public class AppLogAggregatorImpl implements AppLogAggregator {

  private final LocalDirsHandlerService dirsHandler;
  private final Dispatcher dispatcher;
  private final ApplicationId appId;
  private final String user;
  private final NodeId nodeId;
  private final ContainerExecutor exec;
  private final ReentrantReadWriteLock rwLock = new ReentrantReadWriteLock();
  private final Map<ContainerId, ContainerLogAggregator> containerLogAggregators;

  private final ExecutorService threadPool;
  private final FileSystem remoteFS;
  private final Path remoteAppLogDir;
  private final Path remoteAppLogFile;

  @Override
  public void startContainerLogAggregation(ContainerId containerId,
      boolean shouldUpload) {
    if (shouldUpload) {
      LOG.info("Starting log aggregation for container " + containerId);
      ContainerLogAggregator aggregator = new ContainerLogAggregator(
          containerId);
      if (containerLogAggregators.putIfAbsent(containerId, aggregator) != null) {
        LOG.warn("Log aggregation for container " + containerId
            + " has already started");
      } else {
        threadPool.execute(aggregator);
      }
    }
  }

  @Override
  public void finishLogAggregation() {
    LOG.info("Application just finished : " + this.appId);
    this.appFinishing.set(true);
    for (ContainerLogAggregator aggregator : containerLogAggregators.values()) {
      aggregator.finishLogAggregation();
    }
    doAppLogAggregation();
  }

  private void doAppLogAggregation() {
    ContainerLogsRetentionPolicy retentionPolicy =
        this.context.getContainerLogsRetentionPolicy();
    if (retentionPolicy == null) {
      LOG.warn("Container logs retention policy is null. Log aggregation is disabled.");
      return;
    }

    try {
      Path appLogDir = LogAggregationUtils.getRemoteAppLogDir(
          this.remoteFS, this.appId, this.user, this.remoteRootLogDir,
          this.remoteRootLogDirSuffix);

      if (this.remoteFS.exists(appLogDir)) {
        // Upload aggregated logs
        uploadAggregatedLogs(appLogDir);
      }
    } catch (Exception e) {
      LOG.error("Failed to aggregate logs for application " + this.appId, e);
    }
  }
}
```

**日志处理特性**：
- 异步日志收集和上传
- 支持日志压缩和加密
- 可配置的日志保留策略
- 容器日志的增量聚合

## 9.7 安全隔离机制

### 9.7.1 ContainerExecutor：容器执行器

ContainerExecutor提供容器的安全执行环境：

```java
public abstract class ContainerExecutor implements Configurable {

  public static final String DIRECTORY_CONTENTS = "directory.info";
  public static final String CONTAINER_SCRIPT = "launch_container.sh";

  /**
   * Prepare the environment for containers in this application to execute.
   * @param user user name under which the container is launched
   * @param appId application Id
   * @param nmPrivateContainerScriptPath the path for launch script
   * @param nmPrivateTokensPath the path for tokens for the application
   * @param nmPrivateKeytabPath the path for keytab for the application
   * @throws IOException
   */
  public abstract void prepareContainer(ContainerPrepareContext ctx)
      throws IOException;

  /**
   * Launch the container on the node. This is a blocking call and returns only
   * when the container exits.
   * @param ctx Encapsulates information necessary for launching containers.
   * @return the return status of the launch
   * @throws IOException
   */
  public abstract int launchContainer(ContainerLaunchContext ctx)
      throws IOException;

  /**
   * Signal container with the specified signal.
   * @param user the user name of the container
   * @param pid the pid of the container
   * @param signal the signal to send to the container
   * @return true if the signaling is successful
   * @throws IOException
   */
  public abstract boolean signalContainer(ContainerSignalContext ctx)
      throws IOException;

  /**
   * Delete specified directories as a given user.
   * @param user user name under which the container is launched
   * @param subDirs sub directories to be deleted
   * @param baseDirs base directories which contains the subDirs
   * @throws IOException
   */
  public abstract void deleteAsUser(DeletionAsUserContext ctx)
      throws IOException;
}
```

**执行器类型**：

**DefaultContainerExecutor**：默认执行器，以NodeManager进程用户身份运行容器。

**LinuxContainerExecutor**：Linux容器执行器，支持用户隔离和资源限制。

**DockerContainerExecutor**：Docker容器执行器，提供容器化运行环境。

### 9.7.2 LinuxContainerExecutor：Linux容器执行

LinuxContainerExecutor提供基于Linux的安全隔离：

```java
public class LinuxContainerExecutor extends ContainerExecutor {

  private static final Log LOG = LogFactory
      .getLog(LinuxContainerExecutor.class);

  private String nonsecureLocalUser;
  private Pattern nonsecureLocalUserPattern;
  private LCEResourcesHandler resourcesHandler;
  private boolean containerSchedPriorityIsSet = false;
  private int containerSchedPriorityAdjustment = 0;
  private boolean containerLimitUsers;

  @Override
  public void prepareContainer(ContainerPrepareContext ctx) throws IOException {
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();

    createUserLocalDirs(localDirs, user);
    createUserCacheDirs(localDirs, user);
    createAppDirs(localDirs, user, appId);
    createAppLogDirs(appId, logDirs, user);

    // Copy debugging information if required.
    copyDebugInformation(containerWorkDir, user);
    copyTokens(ctx.getNmPrivateTokensPath(), user, appId);
  }

  @Override
  public int launchContainer(ContainerLaunchContext ctx) throws IOException {
    String user = ctx.getUser();
    String appId = ctx.getAppId();
    String containerId = ctx.getContainerId();
    Path containerWorkDir = ctx.getContainerWorkDir();
    List<String> localDirs = ctx.getLocalDirs();
    List<String> logDirs = ctx.getLogDirs();
    List<String> filecacheDirs = ctx.getFilecacheDirs();
    List<String> userLocalDirs = ctx.getUserLocalDirs();

    String runAsUser = getRunAsUser(user);
    ContainerRuntimeContext.Builder builder = new ContainerRuntimeContext
        .Builder(ctx.getContainer());

    builder.setExecutionAttribute(RUN_AS_USER, runAsUser)
        .setExecutionAttribute(USER, user)
        .setExecutionAttribute(APPID, appId)
        .setExecutionAttribute(CONTAINER_ID_STR, containerId)
        .setExecutionAttribute(CONTAINER_WORK_DIR, containerWorkDir)
        .setExecutionAttribute(LOCAL_DIRS, localDirs)
        .setExecutionAttribute(LOG_DIRS, logDirs)
        .setExecutionAttribute(FILECACHE_DIRS, filecacheDirs)
        .setExecutionAttribute(USER_LOCAL_DIRS, userLocalDirs)
        .setExecutionAttribute(CONTAINER_LOCAL_DIRS, ctx.getContainerLocalDirs())
        .setExecutionAttribute(CONTAINER_LOG_DIRS, ctx.getContainerLogDirs());

    ContainerRuntimeContext runtimeContext = builder.build();

    return containerRuntime.launchContainer(runtimeContext);
  }
}
```

**安全特性**：
- 用户身份隔离
- 文件系统权限控制
- 资源使用限制
- 进程组管理

## 9.8 NodeManager配置与优化

### 9.8.1 关键配置参数

NodeManager的性能和行为可以通过多个配置参数进行调优：

```xml
<!-- 节点资源配置 -->
<property>
  <name>yarn.nodemanager.resource.memory-mb</name>
  <value>8192</value>
  <description>节点可用内存总量</description>
</property>

<property>
  <name>yarn.nodemanager.resource.cpu-vcores</name>
  <value>8</value>
  <description>节点可用CPU核心数</description>
</property>

<!-- 容器监控配置 -->
<property>
  <name>yarn.nodemanager.container-monitor.interval-ms</name>
  <value>3000</value>
  <description>容器监控间隔</description>
</property>

<property>
  <name>yarn.nodemanager.vmem-pmem-ratio</name>
  <value>2.1</value>
  <description>虚拟内存与物理内存比率</description>
</property>

<!-- 本地化配置 -->
<property>
  <name>yarn.nodemanager.localizer.cache.target-size-mb</name>
  <value>10240</value>
  <description>本地化缓存目标大小</description>
</property>

<!-- 日志聚合配置 -->
<property>
  <name>yarn.log-aggregation-enable</name>
  <value>true</value>
  <description>启用日志聚合</description>
</property>
```

### 9.8.2 性能优化建议

**内存管理优化**：
- 合理设置虚拟内存比率
- 启用内存检查和限制
- 配置适当的堆内存大小

**磁盘I/O优化**：
- 使用多个本地目录分散I/O
- 配置SSD作为临时目录
- 启用磁盘健康检查

**网络优化**：
- 调整网络缓冲区大小
- 优化RPC连接池配置
- 启用数据压缩传输

**监控优化**：
- 调整监控间隔平衡性能和及时性
- 启用JMX监控接口
- 配置适当的日志级别

## 9.9 故障处理与恢复

### 9.9.1 容器故障处理

NodeManager具备完善的容器故障检测和处理机制：

**内存超限处理**：
- 监控容器内存使用情况
- 超限时发送SIGTERM信号
- 超时后发送SIGKILL强制终止

**进程异常处理**：
- 检测容器进程状态
- 处理僵尸进程
- 清理容器资源

**磁盘故障处理**：
- 监控磁盘健康状况
- 自动切换到健康磁盘
- 报告磁盘故障状态

### 9.9.2 NodeManager恢复机制

NodeManager支持状态恢复以提高可用性：

**状态持久化**：
- 保存容器状态信息
- 记录应用和本地化状态
- 持久化配置和令牌信息

**恢复流程**：
- 启动时读取持久化状态
- 重建内存中的状态信息
- 恢复正在运行的容器
- 重新注册到ResourceManager

## 9.10 本章小结

本章深入分析了YARN NodeManager的架构设计和实现机制。NodeManager作为YARN的工作节点，承担着容器管理、资源监控、日志聚合等关键职责。

**核心要点**：

**架构设计**：NodeManager采用事件驱动的组件化架构，各组件职责明确，协调工作。

**容器管理**：通过状态机模式管理容器生命周期，支持容器的启动、监控、停止和清理。

**资源监控**：实现了节点级和容器级的资源监控，确保资源使用的合理性和安全性。

**本地化服务**：提供了灵活的资源本地化机制，支持不同可见性级别的资源管理。

**日志聚合**：实现了完整的日志收集和聚合功能，便于应用调试和运维。

**安全隔离**：通过ContainerExecutor提供了多种安全隔离机制，保证容器间的安全性。

NodeManager的设计体现了分布式系统中"分而治之"的核心思想，通过将复杂的集群资源管理问题分解为节点级的资源管理问题，大大简化了系统的复杂性，提高了系统的可扩展性和可维护性。理解NodeManager的实现原理，对于深入掌握YARN的工作机制具有重要意义。