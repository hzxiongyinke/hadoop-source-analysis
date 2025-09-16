# 第8章：YARN ResourceManager资源管理

## 8.1 引言

YARN（Yet Another Resource Negotiator）是Hadoop 2.0引入的资源管理框架，它将资源管理和作业调度从MapReduce中分离出来，形成了一个通用的资源管理平台。ResourceManager作为YARN的核心组件，承担着整个集群的资源管理和应用调度职责，是YARN架构的"大脑"。

ResourceManager的设计体现了现代分布式系统的核心理念：将资源管理抽象化、标准化，为不同类型的计算框架提供统一的资源服务。它不仅要管理集群中的计算资源（CPU、内存、磁盘、网络），还要协调应用程序的生命周期，处理故障恢复，确保系统的高可用性。

ResourceManager的复杂性在于它需要在多个维度上进行优化：资源利用率、应用响应时间、系统吞吐量、公平性等。同时，它还要处理异构资源、多租户隔离、安全认证等企业级需求。本章将深入分析ResourceManager的架构设计、资源调度算法、应用管理机制以及高可用实现，帮助读者理解YARN资源管理的核心原理。

## 8.2 ResourceManager架构概述

### 8.2.1 ResourceManager的核心职责

ResourceManager在YARN架构中承担着多重关键职责：


```java
/**
 * The ResourceManager is the main class that is a set of components.
 * "I am the ResourceManager. All your resources belong to us..."
 *
 */
@SuppressWarnings("unchecked")
public class ResourceManager extends CompositeService
        implements Recoverable, ResourceManagerMXBean {

  /**
   * Priority of the ResourceManager shutdown hook.
   */
  public static final int SHUTDOWN_HOOK_PRIORITY = 30;

  /**
   * Used for generation of various ids.
   */
  public static final int EPOCH_BIT_SHIFT = 40;
```


ResourceManager的核心职责包括：

**资源管理**：
- 跟踪集群中所有节点的资源状态
- 管理资源的分配和回收
- 监控资源使用情况和性能指标

**应用管理**：
- 接收和验证应用提交请求
- 管理应用的生命周期
- 协调ApplicationMaster的启动和监控

**调度决策**：
- 根据调度策略分配资源
- 处理资源请求和释放
- 实现多租户资源隔离

**集群监控**：
- 监控NodeManager的健康状况
- 处理节点的加入和离开
- 维护集群拓扑信息

### 8.2.2 ResourceManager的组件架构

ResourceManager采用了模块化的组件架构：


```java
protected ResourceScheduler scheduler;
protected ReservationSystem reservationSystem;
private ClientRMService clientRM;
protected ApplicationMasterService masterService;
protected NMLivelinessMonitor nmLivelinessMonitor;
protected NodesListManager nodesListManager;
protected RMAppManager rmAppManager;
protected ApplicationACLsManager applicationACLsManager;
protected QueueACLsManager queueACLsManager;
private FederationStateStoreService federationStateStoreService;
private ProxyCAManager proxyCAManager;
private WebApp webApp;
private AppReportFetcher fetcher = null;
protected ResourceTrackerService resourceTracker;
private JvmMetrics jvmMetrics;
private boolean curatorEnabled = false;
private ZKCuratorManager zkManager;
```


**核心服务组件**：

**ResourceScheduler**：资源调度器，负责资源分配决策。

**ClientRMService**：客户端服务，处理应用提交和查询请求。

**ApplicationMasterService**：AM服务，处理ApplicationMaster的注册和资源请求。

**ResourceTrackerService**：资源跟踪服务，处理NodeManager的注册和心跳。

**管理组件**：

**RMAppManager**：应用管理器，管理应用的生命周期。

**NodesListManager**：节点列表管理器，维护集群节点信息。

**NMLivelinessMonitor**：节点活跃性监控器，监控NodeManager状态。

**安全组件**：

**ApplicationACLsManager**：应用访问控制管理器。

**QueueACLsManager**：队列访问控制管理器。

**ProxyCAManager**：代理证书管理器。

### 8.2.3 服务初始化流程

ResourceManager的初始化过程体现了复杂分布式系统的启动模式：


```java
masterService = createApplicationMasterService();
createAndRegisterOpportunisticDispatcher(masterService);
addService(masterService) ;
rmContext.setApplicationMasterService(masterService);


applicationACLsManager = new ApplicationACLsManager(conf);

queueACLsManager = createQueueACLsManager(scheduler, conf);

rmAppManager = createRMAppManager();
// Register event handler for RMAppManagerEvents
rmDispatcher.register(RMAppManagerEventType.class, rmAppManager);

clientRM = createClientRMService();
addService(clientRM);
rmContext.setClientRMService(clientRM);

applicationMasterLauncher = createAMLauncher();
rmDispatcher.register(AMLauncherEventType.class,
    applicationMasterLauncher);
```


**初始化顺序**：
1. 创建核心服务组件
2. 注册事件处理器
3. 建立组件间的依赖关系
4. 启动各个服务

**依赖管理**：
- 通过CompositeService管理服务依赖
- 使用事件驱动模型解耦组件
- 通过RMContext共享状态信息

## 8.3 资源调度器架构

### 8.3.1 YarnScheduler接口

YarnScheduler定义了资源调度的核心接口：


```java
/**
 * The main api between the ApplicationMaster and the Scheduler.
 * The ApplicationMaster is updating his future resource requirements
 * and may release containers he doens't need.
 * 
 * @param appAttemptId
 * @param ask
 * @param schedulingRequests
 * @param release
 * @param blacklistAdditions
 * @param blacklistRemovals
 * @param updateRequests     @return the {@link Allocation} for the application
 */
@Public
@Stable
Allocation allocate(ApplicationAttemptId appAttemptId,
    List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
    List<ContainerId> release, List<String> blacklistAdditions,
    List<String> blacklistRemovals, ContainerUpdates updateRequests);
```


**调度接口功能**：

**资源分配**：处理ApplicationMaster的资源请求。

**资源释放**：处理容器的释放请求。

**黑名单管理**：管理不可用节点列表。

**容器更新**：处理容器的更新请求。

### 8.3.2 调度器创建机制

ResourceManager支持可插拔的调度器：


```java
protected ResourceScheduler createScheduler() {
  String schedulerClassName = conf.get(YarnConfiguration.RM_SCHEDULER,
      YarnConfiguration.DEFAULT_RM_SCHEDULER);
  LOG.info("Using Scheduler: " + schedulerClassName);
  try {
    Class<?> schedulerClazz = Class.forName(schedulerClassName);
    if (ResourceScheduler.class.isAssignableFrom(schedulerClazz)) {
      return (ResourceScheduler) ReflectionUtils.newInstance(schedulerClazz,
          this.conf);
    } else {
      throw new YarnRuntimeException("Class: " + schedulerClassName
          + " not instance of " + ResourceScheduler.class.getCanonicalName());
    }
  } catch (ClassNotFoundException e) {
    throw new YarnRuntimeException("Could not instantiate Scheduler: "
        + schedulerClassName, e);
  }
}
```


**调度器类型**：
- **CapacityScheduler**：容量调度器（默认）
- **FairScheduler**：公平调度器
- **FifoScheduler**：先进先出调度器

**可插拔设计**：
- 通过配置指定调度器类型
- 使用反射机制动态加载
- 支持自定义调度器实现

### 8.3.3 CapacityScheduler：容量调度器

CapacityScheduler是YARN的默认调度器，实现了层次化的队列管理：


```java
/**
 * Schedule on all nodes by starting at a random point.
 * @param cs
 */
static void schedule(CapacityScheduler cs) throws InterruptedException{
  // First randomize the start point
  int current = 0;
  Collection<FiCaSchedulerNode> nodes = cs.nodeTracker.getAllNodes();

  // If nodes size is 0 (when there are no node managers registered,
  // we can return from here itself.
  int nodeSize = nodes.size();
  if(nodeSize == 0) {
    return;
  }
  int start = random.nextInt(nodeSize);

  // To avoid too verbose DEBUG logging, only print debug log once for
  // every 10 secs.
  boolean printSkipedNodeLogging = false;
  if (Time.monotonicNow() / 1000 % 10 == 0) {
    printSkipedNodeLogging = (!printedVerboseLoggingForAsyncScheduling);
  } else {
    printedVerboseLoggingForAsyncScheduling = false;
  }
  // ... 调度逻辑
}
```


**调度特点**：

**层次化队列**：支持多级队列结构，实现资源隔离。

**容量保证**：每个队列有最小容量保证。

**弹性共享**：空闲资源可以被其他队列使用。

**抢占机制**：支持资源抢占以保证SLA。

### 8.3.4 资源分配流程

CapacityScheduler的资源分配实现：


```java
@Override
@Lock(Lock.NoLock.class)
public Allocation allocate(ApplicationAttemptId applicationAttemptId,
    List<ResourceRequest> ask, List<SchedulingRequest> schedulingRequests,
    List<ContainerId> release, List<String> blacklistAdditions,
    List<String> blacklistRemovals, ContainerUpdates updateRequests) {
  FiCaSchedulerApp application = getApplicationAttempt(applicationAttemptId);
  if (application == null) {
    LOG.error("Calling allocate on removed or non existent application " +
        applicationAttemptId.getApplicationId());
    return EMPTY_ALLOCATION;
  }
  // ... 分配逻辑
}
```


**分配步骤**：
1. 验证应用存在性
2. 处理资源请求
3. 执行调度算法
4. 返回分配结果

## 8.4 应用管理机制

### 8.4.1 RMAppManager：应用管理器

RMAppManager负责应用的生命周期管理：


```java
@Override
public void handle(RMAppManagerEvent event) {
  ApplicationId applicationId = event.getApplicationId();
  LOG.debug("RMAppManager processing event for {} of type {}",
      applicationId, event.getType());
  switch (event.getType()) {
  case APP_COMPLETED :
    finishApplication(applicationId);
    logApplicationSummary(applicationId);
    checkAppNumCompletedLimit();
    break;
  case APP_MOVE :
    // moveAllApps from scheduler will fire this event for each of
    // those applications which needed to be moved to a new queue.
    // Use the standard move application api to do the same.
    try {
      moveApplicationAcrossQueue(applicationId,
          event.getTargetQueueForMove());
    } catch (YarnException e) {
      LOG.warn("Move Application has failed: " + e.getMessage());
    }
    // ... 其他事件处理
  }
}
```


**事件处理**：
- **APP_COMPLETED**：应用完成处理
- **APP_MOVE**：应用队列迁移
- **APP_KILL**：应用终止处理

**管理功能**：
- 应用提交验证
- 应用状态跟踪
- 应用资源清理

### 8.4.2 应用状态机

YARN使用状态机模式管理应用生命周期：


```java
// Transitions from NEW state
.addTransition(RMAppState.NEW, RMAppState.NEW,
    RMAppEventType.NODE_UPDATE, new RMAppNodeUpdateTransition())
.addTransition(RMAppState.NEW, RMAppState.NEW_SAVING,
    RMAppEventType.START, new RMAppNewlySavingTransition())
.addTransition(RMAppState.NEW, EnumSet.of(RMAppState.SUBMITTED,
        RMAppState.ACCEPTED, RMAppState.FINISHED, RMAppState.FAILED,
        RMAppState.KILLED, RMAppState.FINAL_SAVING),
    RMAppEventType.RECOVER, new RMAppRecoveredTransition())
.addTransition(RMAppState.NEW, RMAppState.KILLED, RMAppEventType.KILL,
    new AppKilledTransition())
.addTransition(RMAppState.NEW, RMAppState.FINAL_SAVING,
    RMAppEventType.APP_REJECTED,
    new FinalSavingTransition(new AppRejectedTransition(),
      RMAppState.FAILED))
```


**状态转换**：

**NEW → NEW_SAVING**：应用开始保存状态。

**NEW_SAVING → SUBMITTED**：应用提交到调度器。

**SUBMITTED → ACCEPTED**：应用被调度器接受。

**ACCEPTED → RUNNING**：ApplicationMaster注册成功。

**RUNNING → FINISHING**：应用开始结束流程。

### 8.4.3 ApplicationMaster协调

ResourceManager通过ApplicationMasterService与AM交互：


```java
@Override
public void allocate(ApplicationAttemptId appAttemptId,
    AllocateRequest request, AllocateResponse response) throws YarnException {

  handleProgress(appAttemptId, request);

  List<ResourceRequest> ask = request.getAskList();
  List<ContainerId> release = request.getReleaseList();

  ResourceBlacklistRequest blacklistRequest =
      request.getResourceBlacklistRequest();
  List<String> blacklistAdditions =
      (blacklistRequest != null) ?
          blacklistRequest.getBlacklistAdditions() : Collections.emptyList();
  List<String> blacklistRemovals =
      (blacklistRequest != null) ?
          blacklistRequest.getBlacklistRemovals() : Collections.emptyList();
  RMApp app =
      getRmContext().getRMApps().get(appAttemptId.getApplicationId());
  // ... 分配处理逻辑
}
```


**协调功能**：
- 处理AM的资源请求
- 管理AM的心跳
- 协调容器分配
- 处理AM的状态更新

## 8.5 高可用机制

### 8.5.1 Active/Standby架构

ResourceManager支持Active/Standby高可用模式：


```java
synchronized void transitionToActive() throws Exception {
  if (rmContext.getHAServiceState() == HAServiceProtocol.HAServiceState.ACTIVE) {
    LOG.info("Already in active state");
    return;
  }
  LOG.info("Transitioning to active state");

  this.rmLoginUGI.doAs(new PrivilegedExceptionAction<Void>() {
    @Override
    public Void run() throws Exception {
      try {
        startActiveServices();
        return null;
      } catch (Exception e) {
        reinitialize(true);
        throw e;
      }
    }
  });

  rmContext.setHAServiceState(HAServiceProtocol.HAServiceState.ACTIVE);
  LOG.info("Transitioned to active state");
}
```


**状态转换**：
- **STANDBY → ACTIVE**：故障转移或手动切换
- **ACTIVE → STANDBY**：维护或故障处理

**服务管理**：
- Active状态启动所有服务
- Standby状态只启动必要服务
- 状态转换时的服务协调

### 8.5.2 ZooKeeper选举机制

ResourceManager使用ZooKeeper进行Leader选举：


```java
@Override
protected void serviceInit(Configuration conf)
    throws Exception {
  conf = conf instanceof YarnConfiguration
      ? conf
      : new YarnConfiguration(conf);

  String zkQuorum = conf.get(YarnConfiguration.RM_ZK_ADDRESS);
  if (zkQuorum == null) {
    throw new YarnRuntimeException("Embedded automatic failover " +
        "is enabled, but " + YarnConfiguration.RM_ZK_ADDRESS +
        " is not set");
  }

  String rmId = HAUtil.getRMHAId(conf);
  String clusterId = YarnConfiguration.getClusterId(conf);
  localActiveNodeInfo = createActiveNodeInfo(clusterId, rmId);

  String zkBasePath = conf.get(YarnConfiguration.AUTO_FAILOVER_ZK_BASE_PATH,
      YarnConfiguration.DEFAULT_AUTO_FAILOVER_ZK_BASE_PATH);
  String electionZNode = zkBasePath + "/" + clusterId;

  zkSessionTimeout = conf.getLong(YarnConfiguration.RM_ZK_TIMEOUT_MS,
      YarnConfiguration.DEFAULT_RM_ZK_TIMEOUT_MS);

  List<ACL> zkAcls = ZKCuratorManager.getZKAcls(conf);
  List<ZKUtil.ZKAuthInfo> zkAuths = ZKCuratorManager.getZKAuths(conf);

  int maxRetryNum =
      conf.getInt(YarnConfiguration.RM_HA_FC_ELECTOR_ZK_RETRIES_KEY, conf
        .getInt(CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_KEY,
          CommonConfigurationKeys.HA_FC_ELECTOR_ZK_OP_RETRIES_DEFAULT));
  elector = new ActiveStandbyElector(zkQuorum, (int) zkSessionTimeout,
      electionZNode, zkAcls, zkAuths, this, maxRetryNum, false);
  // ... 选举器初始化
}
```


**选举机制**：
- 基于ZooKeeper的分布式锁
- 自动故障检测和切换
- 脑裂保护机制

### 8.5.3 状态恢复机制

ResourceManager支持状态恢复：


```java
@Override
public void recover(RMState state) throws Exception {
  // recover RMdelegationTokenSecretManager
  rmContext.getRMDelegationTokenSecretManager().recover(state);

  // recover AMRMTokenSecretManager
  rmContext.getAMRMTokenSecretManager().recover(state);

  // recover reservations
  if (reservationSystem != null) {
    reservationSystem.recover(state);
  }
  // recover applications
  rmAppManager.recover(state);

  // recover ProxyCA
  rmContext.getProxyCAManager().recover(state);

  setSchedulerRecoveryStartAndWaitTime(state, conf);
}
```


**恢复内容**：
- Token管理器状态
- 应用状态信息
- 调度器状态
- 预约系统状态

## 8.6 性能优化与监控

### 8.6.1 异步调度优化

CapacityScheduler支持异步调度以提高性能：

**异步调度特点**：
- 调度决策与心跳处理分离
- 提高系统吞吐量
- 减少调度延迟

**实现机制**：
- 独立的调度线程
- 事件驱动的调度触发
- 批量处理调度请求

### 8.6.2 资源预留机制

调度器实现了资源预留机制：


```java
LeafQueue queue = ((LeafQueue) reservedApplication.getQueue());
CSAssignment assignment = queue.assignContainers(getClusterResource(),
    new SimpleCandidateNodeSet<>(node),
    // TODO, now we only consider limits for parent for non-labeled
    // resources, should consider labeled resources as well.
    new ResourceLimits(labelManager
        .getResourceByLabel(RMNodeLabelsManager.NO_LABEL,
            getClusterResource())),
    SchedulingMode.RESPECT_PARTITION_EXCLUSIVITY);

if (assignment.isFulfilledReservation()) {
  if (withNodeHeartbeat) {
    // Only update SchedulerHealth in sync scheduling, existing
    // Data structure of SchedulerHealth need to be updated for
    // Async mode
    updateSchedulerHealth(lastNodeUpdateTime, node.getNodeID(),
        assignment);
  }
  // ... 预留处理逻辑
}
```


**预留机制**：
- 为大资源请求预留节点
- 避免资源碎片化
- 提高资源利用率

### 8.6.3 监控指标

ResourceManager提供了丰富的监控指标：

**资源指标**：
- 集群总资源和可用资源
- 各队列的资源使用情况
- 节点资源利用率

**应用指标**：
- 应用提交和完成数量
- 应用等待时间和运行时间
- 失败应用统计

**调度指标**：
- 调度延迟和吞吐量
- 资源分配成功率
- 抢占操作统计

## 8.7 本章小结

本章深入分析了YARN ResourceManager的架构设计和核心实现。通过对源码的详细分析，我们可以看到ResourceManager设计的几个重要特点：

**模块化架构**：通过清晰的组件划分和服务化设计，实现了复杂功能的有序管理。

**可插拔调度**：支持多种调度算法，满足不同场景的需求。

**状态机管理**：使用状态机模式管理应用生命周期，确保状态转换的正确性。

**高可用保证**：通过Active/Standby模式和状态恢复机制，确保系统的高可用性。

**性能优化**：通过异步调度、资源预留等机制，提高系统性能和资源利用率。

在下一章中，我们将分析YARN的另一个核心组件——NodeManager，了解节点级别的资源管理和容器执行机制。

---

**本章要点回顾**：
- ResourceManager是YARN的核心，负责集群资源管理和应用调度
- 采用模块化架构，支持可插拔的调度器
- 使用状态机模式管理应用生命周期
- 通过Active/Standby模式实现高可用
- 提供了丰富的性能优化和监控机制
