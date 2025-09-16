# 第2章：Hadoop核心架构设计

## 2.1 引言

在深入分析Hadoop各个组件的源码实现之前，我们需要首先理解Hadoop的整体架构设计理念。Hadoop作为一个复杂的分布式系统，其架构设计体现了多年来分布式系统设计的最佳实践和经验总结。从Master-Slave的经典模式到现代的微服务架构思想，从简单的配置管理到复杂的服务治理，Hadoop的架构演进反映了大数据技术的发展轨迹。

本章将从架构设计的角度分析Hadoop的核心设计原理，包括其采用的架构模式、模块化设计思想、组件间的通信机制、配置管理体系以及服务生命周期管理。通过对这些基础架构的深入理解，读者将为后续的源码分析建立坚实的理论基础。

## 2.2 Master-Slave架构模式

### 2.2.1 架构模式概述

Hadoop采用了经典的Master-Slave架构模式，这种模式在分布式系统中被广泛应用，特别适合于需要集中管理和协调的场景。在Hadoop生态系统中，这种模式体现在多个层面：

**HDFS层面**：
- **NameNode（Master）**：管理文件系统元数据，协调DataNode的工作
- **DataNode（Slave）**：存储实际数据块，接受NameNode的指令

**YARN层面**：
- **ResourceManager（Master）**：管理集群资源，协调应用程序的资源分配
- **NodeManager（Slave）**：管理单个节点的资源，执行容器任务

**MapReduce层面**：
- **ApplicationMaster（Master）**：管理特定应用的任务调度和执行
- **Task（Slave）**：执行具体的Map或Reduce任务

### 2.2.2 Master-Slave模式的优势

**集中化管理**：Master节点提供了统一的管理入口，简化了系统的设计和实现。所有的元数据管理、资源调度、任务协调都通过Master节点进行，避免了分布式一致性的复杂问题。

**简化的一致性模型**：由于Master节点是唯一的决策者，系统可以采用相对简单的一致性模型。例如，HDFS的"一次写入，多次读取"模型就是基于这种集中化管理实现的。

**易于监控和调试**：集中化的架构使得系统状态的监控和问题的调试变得相对简单。管理员可以通过Master节点获取整个集群的状态信息。

### 2.2.3 单点故障问题及解决方案

Master-Slave架构的主要缺点是Master节点的单点故障问题。Hadoop在不同版本中采用了多种策略来解决这个问题：

**Secondary NameNode（Hadoop 1.x）**：
虽然名为"Secondary"，但它并不是NameNode的热备份，而是用于辅助NameNode进行检查点操作，减少NameNode重启时的恢复时间。

**NameNode HA（Hadoop 2.x+）**：
引入了真正的高可用机制，通过Active/Standby模式实现：
- **Active NameNode**：提供正常服务
- **Standby NameNode**：实时同步Active NameNode的状态
- **共享存储**：用于存储EditLog，保证状态同步
- **ZooKeeper**：用于故障检测和自动切换

**Federation（Hadoop 2.x+）**：
通过多个NameNode管理不同的命名空间，实现水平扩展：
- 每个NameNode管理一部分命名空间
- 客户端通过ViewFileSystem统一访问
- 避免了单个NameNode的性能瓶颈

## 2.3 模块化设计与组件解耦

### 2.3.1 分层架构设计

Hadoop采用了清晰的分层架构，每一层都有明确的职责和接口定义：

**基础设施层（hadoop-common）**：
- 提供通用的工具类和基础服务
- RPC通信框架
- 配置管理系统
- 安全认证框架
- 序列化机制

**存储层（hadoop-hdfs）**：
- 分布式文件系统实现
- 数据块管理
- 元数据管理
- 数据复制和恢复

**资源管理层（hadoop-yarn）**：
- 集群资源管理
- 应用程序生命周期管理
- 容器调度和监控

**计算层（hadoop-mapreduce）**：
- 分布式计算框架
- 任务调度和执行
- 数据处理流水线

### 2.3.2 接口抽象与实现分离

Hadoop大量使用了接口抽象来实现组件间的解耦，这种设计使得系统具有良好的可扩展性和可测试性。

**文件系统抽象**：
```java
public abstract class FileSystem extends Configured implements Closeable {
    // 抽象的文件系统操作接口
    public abstract FSDataInputStream open(Path f, int bufferSize);
    public abstract FSDataOutputStream create(Path f, ...);
    public abstract boolean delete(Path f, boolean recursive);
    // ...
}
```

这种抽象使得Hadoop可以支持多种文件系统实现：
- **DistributedFileSystem**：HDFS的实现
- **LocalFileSystem**：本地文件系统
- **S3AFileSystem**：Amazon S3的实现
- **AzureFileSystem**：Azure存储的实现

**RPC引擎抽象**：
```java
public abstract class RpcEngine {
    public abstract <T> ProtocolProxy<T> getProxy(Class<T> protocol, ...);
    public abstract RPC.Server getServer(Class<?> protocol, Object instance, ...);
    // ...
}
```

支持多种RPC实现：
- **WritableRpcEngine**：基于Writable序列化
- **ProtobufRpcEngine2**：基于Protocol Buffers序列化

### 2.3.3 插件化架构

Hadoop采用了插件化的架构设计，允许用户根据需要替换或扩展系统组件：

**调度器插件**：
- **FairScheduler**：公平调度器
- **CapacityScheduler**：容量调度器
- **FifoScheduler**：先进先出调度器

**序列化插件**：
- **WritableSerialization**：Hadoop原生序列化
- **AvroSerialization**：Avro序列化
- **JavaSerialization**：Java原生序列化

**压缩编解码器**：
- **GzipCodec**：Gzip压缩
- **BZip2Codec**：BZip2压缩
- **LzoCodec**：LZO压缩
- **SnappyCodec**：Snappy压缩

## 2.4 服务发现与通信机制

### 2.4.1 配置驱动的服务发现

与现代微服务架构中常见的注册中心模式不同，Hadoop采用了配置驱动的服务发现机制。这种设计简化了系统的复杂性，但也要求运维人员对集群配置有深入的理解。

**核心配置参数**：
- `fs.defaultFS`：默认文件系统的地址
- `yarn.resourcemanager.hostname`：ResourceManager的主机名
- `dfs.nameservices`：NameNode服务的逻辑名称

**配置示例**：
```xml
<configuration>
    <property>
        <name>fs.defaultFS</name>
        <value>hdfs://namenode:9000</value>
    </property>
    <property>
        <name>yarn.resourcemanager.hostname</name>
        <value>resourcemanager</value>
    </property>
</configuration>
```

### 2.4.2 RPC通信框架

Hadoop的组件间通信完全基于自研的RPC框架，这个框架针对大数据场景进行了专门优化：

**协议定义**：
所有的RPC协议都继承自`VersionedProtocol`接口，支持版本管理：
```java
public interface ClientProtocol extends VersionedProtocol {
    long getProtocolVersion(String protocol, long clientVersion);
    LocatedBlocks getBlockLocations(String src, long offset, long length);
    // ...
}
```

**代理机制**：
客户端通过动态代理获取远程服务的本地代理：
```java
ClientProtocol namenode = RPC.getProxy(
    ClientProtocol.class,
    ClientProtocol.versionID,
    nnAddr,
    conf
);
```

**多线程服务器**：
服务端采用基于NIO的多线程Reactor模式，支持高并发请求处理。

### 2.4.3 心跳与健康检查机制

Hadoop通过心跳机制实现节点状态监控和故障检测：

**DataNode心跳**：
- 定期向NameNode发送心跳信息
- 汇报数据块状态和存储使用情况
- 接收NameNode的指令（复制、删除、移动数据块）

**NodeManager心跳**：
- 定期向ResourceManager发送心跳信息
- 汇报节点资源使用情况
- 接收ResourceManager的容器管理指令

**ApplicationMaster心跳**：
- 定期向ResourceManager发送心跳信息
- 汇报应用程序执行进度
- 申请或释放资源

## 2.5 配置管理与服务治理

### 2.5.1 Configuration类设计原理

Hadoop的配置管理系统基于`Configuration`类实现，这个类提供了灵活而强大的配置管理能力：

**配置文件层次结构**：
1. **默认配置**：`core-default.xml`、`hdfs-default.xml`、`yarn-default.xml`
2. **站点配置**：`core-site.xml`、`hdfs-site.xml`、`yarn-site.xml`
3. **程序配置**：通过代码动态设置的配置

**配置加载机制**：
```java
Configuration conf = new Configuration();
// 自动加载默认配置文件
conf.addResource("custom-site.xml");  // 添加自定义配置
String value = conf.get("property.name", "default.value");
```

**配置优先级**：
后加载的配置会覆盖先加载的配置，但标记为`final`的配置不能被覆盖：
```xml
<property>
    <name>important.setting</name>
    <value>production.value</value>
    <final>true</final>
</property>
```

### 2.5.2 服务抽象与生命周期管理

Hadoop定义了统一的服务抽象框架，所有的服务组件都实现`Service`接口：

**服务状态模型**：
```java
public enum STATE {
    NOTINITED(0, "NOTINITED"),
    INITED(1, "INITED"),
    STARTED(2, "STARTED"),
    STOPPED(3, "STOPPED");
}
```

**状态转换规则**：
- `NOTINITED` → `INITED`：通过`init()`方法
- `INITED` → `STARTED`：通过`start()`方法
- 任何状态 → `STOPPED`：通过`stop()`方法

**AbstractService基类**：
```java
public abstract class AbstractService implements Service {
    protected void serviceInit(Configuration conf) throws Exception {
        // 子类实现具体的初始化逻辑
    }
    
    protected void serviceStart() throws Exception {
        // 子类实现具体的启动逻辑
    }
    
    protected void serviceStop() throws Exception {
        // 子类实现具体的停止逻辑
    }
}
```

### 2.5.3 复合服务与依赖管理

Hadoop提供了`CompositeService`类来管理具有依赖关系的多个服务：

**服务组合**：
```java
public class ResourceManager extends CompositeService {
    protected void serviceInit(Configuration conf) throws Exception {
        // 添加子服务
        addService(rmSecretManagerService);
        addService(containerAllocationExpirer);
        addService(amLivelinessMonitor);
        addService(scheduler);
        // ...
        super.serviceInit(conf);  // 初始化所有子服务
    }
}
```

**依赖管理**：
- 初始化时按添加顺序初始化子服务
- 启动时按添加顺序启动子服务
- 停止时按相反顺序停止子服务

## 2.6 设计模式在Hadoop中的应用

### 2.6.1 工厂模式

Hadoop大量使用工厂模式来创建对象，提供了良好的扩展性：

**FileSystem工厂**：
```java
public static FileSystem get(Configuration conf) throws IOException {
    return get(getDefaultUri(conf), conf);
}

public static FileSystem get(URI uri, Configuration conf) throws IOException {
    String scheme = uri.getScheme();
    String authority = uri.getAuthority();
    
    if (scheme == null && authority == null) {
        return getDefaultFileSystem(conf);
    }
    
    if (scheme != null && authority == null) {
        return getFileSystemClass(scheme, conf).newInstance();
    }
    // ...
}
```

**RPC代理工厂**：
```java
public static <T> T getProxy(Class<T> protocol, long clientVersion,
                            InetSocketAddress addr, Configuration conf) {
    return getProtocolEngine(protocol, conf)
        .getProxy(protocol, clientVersion, addr, conf);
}
```

### 2.6.2 观察者模式

Hadoop使用观察者模式实现事件通知和状态监控：

**服务状态监听**：
```java
public interface ServiceStateChangeListener {
    void stateChanged(Service service);
}

// 在AbstractService中
private final ServiceStateChangeListeners listeners = 
    new ServiceStateChangeListeners();

public void registerServiceListener(ServiceStateChangeListener listener) {
    listeners.add(listener);
}
```

**度量系统**：
```java
public abstract class MetricsSource {
    public abstract void getMetrics(MetricsCollector collector, boolean all);
}

// 度量收集器会定期调用所有注册的MetricsSource
```

### 2.6.3 策略模式

Hadoop使用策略模式实现算法的可插拔替换：

**调度策略**：
```java
public abstract class ResourceScheduler extends AbstractService {
    public abstract Allocation allocate(
        ApplicationAttemptId applicationAttemptId,
        List<ResourceRequest> ask,
        List<ContainerId> release,
        List<String> blacklistAdditions,
        List<String> blacklistRemovals);
}
```

不同的调度器实现不同的调度策略：
- `FairScheduler`：公平调度
- `CapacityScheduler`：容量调度
- `FifoScheduler`：先进先出调度

**负载均衡策略**：
```java
public abstract class BlockPlacementPolicy {
    public abstract DatanodeStorageInfo[] chooseTarget(
        String srcPath,
        int numOfReplicas,
        Node writer,
        List<DatanodeStorageInfo> chosenNodes,
        boolean returnChosenNodes,
        Set<Node> excludedNodes,
        long blocksize,
        StorageType storageType);
}
```

### 2.6.4 模板方法模式

Hadoop在服务框架中大量使用模板方法模式：

**AbstractService模板**：
```java
public final void init(Configuration conf) {
    if (conf == null) {
        throw new ServiceStateException("Cannot initialize service: null configuration");
    }
    if (isInState(STATE.INITED)) {
        return;
    }
    synchronized (stateChangeLock) {
        if (enterState(STATE.INITED) != STATE.INITED) {
            setConfig(conf);
            try {
                serviceInit(config);  // 模板方法，由子类实现
                if (isInState(STATE.INITED)) {
                    notifyListeners();
                }
            } catch (Exception e) {
                noteFailure(e);
                ServiceOperations.stopQuietly(LOG, this);
                throw ServiceStateException.convert(e);
            }
        }
    }
}
```

## 2.7 核心抽象类和接口设计

### 2.7.1 Service接口体系

`Service`接口是Hadoop服务框架的核心抽象，定义了所有服务组件必须遵循的生命周期契约：

**接口定义**：
```java
public interface Service extends Closeable {
    enum STATE {
        NOTINITED, INITED, STARTED, STOPPED
    }
    
    void init(Configuration config);
    void start();
    void stop();
    void close() throws IOException;
    
    STATE getServiceState();
    boolean isInState(STATE state);
    Throwable getFailureCause();
    STATE getFailureState();
    
    String getName();
    Configuration getConfig();
    long getStartTime();
}
```

**设计优势**：
- **统一的生命周期管理**：所有服务都遵循相同的启动和停止流程
- **状态可见性**：可以随时查询服务的当前状态
- **错误处理**：提供了统一的错误信息获取机制
- **配置管理**：每个服务都有自己的配置上下文

### 2.7.2 Configured抽象类

`Configured`类提供了配置管理的基础实现：

```java
public class Configured implements Configurable {
    private Configuration conf;
    
    public Configured() {
        this(null);
    }
    
    public Configured(Configuration conf) {
        setConf(conf);
    }
    
    public void setConf(Configuration conf) {
        this.conf = conf;
    }
    
    public Configuration getConf() {
        return conf;
    }
}
```

大多数Hadoop组件都继承自这个类，获得了基础的配置管理能力。

### 2.7.3 VersionedProtocol接口

所有的RPC协议都必须实现`VersionedProtocol`接口：

```java
public interface VersionedProtocol {
    long getProtocolVersion(String protocol, long clientVersion) 
        throws IOException;
}
```

这个接口支持协议版本管理，确保客户端和服务端的兼容性。

## 2.8 组件间的依赖注入机制

### 2.8.1 配置驱动的依赖注入

Hadoop采用配置驱动的依赖注入机制，通过配置文件指定具体的实现类：

**示例配置**：
```xml
<property>
    <name>fs.hdfs.impl</name>
    <value>org.apache.hadoop.hdfs.DistributedFileSystem</value>
</property>

<property>
    <name>yarn.resourcemanager.scheduler.class</name>
    <value>org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FairScheduler</value>
</property>
```

**实现机制**：
```java
public static <T> Class<? extends T> getClass(String name, 
                                              Class<T> defaultValue, 
                                              Class<T> xface) {
    try {
        Class<?> theClass = getClassByName(name);
        if (theClass != null && !xface.isAssignableFrom(theClass))
            throw new RuntimeException(theClass+" not "+xface.getName());
        else if (theClass != null)
            return theClass.asSubclass(xface);
        else
            return defaultValue;
    } catch (Exception e) {
        throw new RuntimeException(e);
    }
}
```

### 2.8.2 服务定位器模式

Hadoop使用服务定位器模式来获取服务实例：

**ResourceManager中的服务定位**：
```java
public class ResourceManager extends CompositeService {
    private RMContext rmContext;
    
    protected void serviceInit(Configuration conf) throws Exception {
        this.rmContext = new RMContextImpl();
        
        // 创建并注册各种服务
        RMSecretManagerService rmSecretManagerService = 
            createRMSecretManagerService();
        addService(rmSecretManagerService);
        rmContext.setRMSecretManagerService(rmSecretManagerService);
        
        ResourceScheduler scheduler = createScheduler();
        addService(scheduler);
        rmContext.setScheduler(scheduler);
        // ...
    }
}
```

**RMContext作为服务注册表**：
```java
public interface RMContext {
    Dispatcher getDispatcher();
    ResourceScheduler getScheduler();
    RMSecretManagerService getRMSecretManagerService();
    // ...
}
```

## 2.9 本章小结

本章从架构设计的角度深入分析了Hadoop的核心设计原理。通过对Master-Slave架构模式、模块化设计、通信机制、配置管理和设计模式的分析，我们可以看到Hadoop架构设计的几个重要特点：

**简洁性与实用性的平衡**：Hadoop选择了相对简单但实用的架构模式，如Master-Slave架构和配置驱动的服务发现，这些设计虽然不是最先进的，但在大数据场景下证明了其有效性。

**可扩展性的重视**：通过接口抽象、插件化架构和工厂模式，Hadoop提供了良好的可扩展性，允许用户根据需要替换或扩展系统组件。

**统一的服务框架**：通过Service接口和AbstractService基类，Hadoop建立了统一的服务管理框架，简化了复杂系统的开发和维护。

**配置驱动的设计理念**：Hadoop大量使用配置文件来控制系统行为，这种设计使得系统具有很好的灵活性，但也要求运维人员具备较高的技术水平。

在接下来的章节中，我们将基于这些架构设计原理，深入分析Hadoop各个组件的具体实现，特别是RPC框架、配置系统等基础设施的源码实现。

---

**本章要点回顾**：
- Hadoop采用Master-Slave架构模式，通过HA和Federation解决单点故障问题
- 分层架构和接口抽象实现了良好的模块化设计和组件解耦
- 配置驱动的服务发现和RPC通信机制简化了系统复杂性
- 统一的服务框架提供了标准化的生命周期管理
- 多种设计模式的应用提高了系统的可扩展性和可维护性
