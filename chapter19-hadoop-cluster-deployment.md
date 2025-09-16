# 第19章：Hadoop集群部署与运维

## 19.1 引言

Hadoop集群的部署与运维是将理论知识转化为实际生产力的关键环节。一个成功的Hadoop部署不仅需要深入理解Hadoop的架构原理和配置参数，还需要掌握分布式系统的运维技巧、性能调优方法和故障处理策略。随着企业数据规模的不断增长和业务需求的日益复杂，Hadoop集群的规模也在不断扩大，从最初的几十个节点发展到数千个节点的超大规模集群。

现代Hadoop集群部署面临着多重挑战：硬件异构性带来的兼容性问题、网络拓扑的复杂性、存储系统的多样性、安全策略的严格性等。同时，云计算和容器化技术的兴起也为Hadoop部署带来了新的机遇和挑战。传统的物理机部署模式正在向虚拟化、容器化和云原生方向演进，这要求运维人员不仅要掌握传统的部署技能，还要了解新兴的部署技术和工具。

运维管理是Hadoop集群生命周期中最重要的阶段，涵盖了监控告警、性能调优、容量规划、故障处理、安全管理、版本升级等多个方面。有效的运维管理不仅能够确保集群的稳定运行，还能够最大化硬件资源的利用率，提升数据处理的效率。随着自动化运维和智能运维技术的发展，传统的手工运维模式正在向自动化、智能化方向转变。

本章将系统性地介绍Hadoop集群部署与运维的最佳实践，从集群规划设计到自动化部署，从日常运维管理到故障应急处理，帮助读者建立完整的Hadoop运维知识体系和实践能力。

## 19.2 集群规划与设计

### 19.2.1 硬件选型与容量规划

合理的硬件选型是Hadoop集群成功部署的基础：

```java
/**
 * Hadoop集群容量规划工具
 */
public class HadoopCapacityPlanner {
  
  private static final Logger LOG = LoggerFactory.getLogger(HadoopCapacityPlanner.class);
  
  /**
   * 集群规格配置
   */
  public static class ClusterSpec {
    private int nodeCount;
    private int coresPerNode;
    private long memoryPerNodeGB;
    private long storagePerNodeTB;
    private String storageType; // HDD, SSD, NVMe
    private String networkBandwidth; // 1Gbps, 10Gbps, 25Gbps
    
    // Constructors, getters, setters
    public ClusterSpec(int nodeCount, int coresPerNode, long memoryPerNodeGB, 
                      long storagePerNodeTB, String storageType, String networkBandwidth) {
      this.nodeCount = nodeCount;
      this.coresPerNode = coresPerNode;
      this.memoryPerNodeGB = memoryPerNodeGB;
      this.storagePerNodeTB = storagePerNodeTB;
      this.storageType = storageType;
      this.networkBandwidth = networkBandwidth;
    }
    
    // Getters
    public int getNodeCount() { return nodeCount; }
    public int getCoresPerNode() { return coresPerNode; }
    public long getMemoryPerNodeGB() { return memoryPerNodeGB; }
    public long getStoragePerNodeTB() { return storagePerNodeTB; }
    public String getStorageType() { return storageType; }
    public String getNetworkBandwidth() { return networkBandwidth; }
  }
  
  /**
   * 工作负载特征
   */
  public static class WorkloadProfile {
    private double avgJobSizeGB;
    private int concurrentJobs;
    private double cpuIntensiveRatio; // 0.0 - 1.0
    private double ioIntensiveRatio;  // 0.0 - 1.0
    private int replicationFactor;
    private double compressionRatio;
    
    public WorkloadProfile(double avgJobSizeGB, int concurrentJobs, 
                          double cpuIntensiveRatio, double ioIntensiveRatio,
                          int replicationFactor, double compressionRatio) {
      this.avgJobSizeGB = avgJobSizeGB;
      this.concurrentJobs = concurrentJobs;
      this.cpuIntensiveRatio = cpuIntensiveRatio;
      this.ioIntensiveRatio = ioIntensiveRatio;
      this.replicationFactor = replicationFactor;
      this.compressionRatio = compressionRatio;
    }
    
    // Getters
    public double getAvgJobSizeGB() { return avgJobSizeGB; }
    public int getConcurrentJobs() { return concurrentJobs; }
    public double getCpuIntensiveRatio() { return cpuIntensiveRatio; }
    public double getIoIntensiveRatio() { return ioIntensiveRatio; }
    public int getReplicationFactor() { return replicationFactor; }
    public double getCompressionRatio() { return compressionRatio; }
  }
  
  /**
   * 容量规划结果
   */
  public static class CapacityPlanResult {
    private ClusterSpec recommendedSpec;
    private double storageUtilization;
    private double memoryUtilization;
    private double cpuUtilization;
    private double networkUtilization;
    private List<String> recommendations;
    private List<String> warnings;
    
    public CapacityPlanResult() {
      this.recommendations = new ArrayList<>();
      this.warnings = new ArrayList<>();
    }
    
    // Getters and setters
    public ClusterSpec getRecommendedSpec() { return recommendedSpec; }
    public void setRecommendedSpec(ClusterSpec recommendedSpec) { this.recommendedSpec = recommendedSpec; }
    
    public double getStorageUtilization() { return storageUtilization; }
    public void setStorageUtilization(double storageUtilization) { this.storageUtilization = storageUtilization; }
    
    public double getMemoryUtilization() { return memoryUtilization; }
    public void setMemoryUtilization(double memoryUtilization) { this.memoryUtilization = memoryUtilization; }
    
    public double getCpuUtilization() { return cpuUtilization; }
    public void setCpuUtilization(double cpuUtilization) { this.cpuUtilization = cpuUtilization; }
    
    public double getNetworkUtilization() { return networkUtilization; }
    public void setNetworkUtilization(double networkUtilization) { this.networkUtilization = networkUtilization; }
    
    public List<String> getRecommendations() { return recommendations; }
    public List<String> getWarnings() { return warnings; }
    
    public void addRecommendation(String recommendation) { recommendations.add(recommendation); }
    public void addWarning(String warning) { warnings.add(warning); }
  }
  
  /**
   * 执行容量规划
   */
  public CapacityPlanResult planCapacity(long totalDataSizeTB, WorkloadProfile workload, 
                                        ClusterSpec currentSpec) {
    
    CapacityPlanResult result = new CapacityPlanResult();
    
    // 计算存储需求
    double effectiveDataSize = totalDataSizeTB * workload.getReplicationFactor() / 
                              workload.getCompressionRatio();
    
    // 计算所需存储容量（考虑80%利用率）
    double requiredStorageCapacity = effectiveDataSize / 0.8;
    
    // 计算内存需求
    double requiredMemoryGB = calculateMemoryRequirement(workload);
    
    // 计算CPU需求
    int requiredCores = calculateCpuRequirement(workload);
    
    // 生成推荐配置
    ClusterSpec recommendedSpec = generateRecommendedSpec(
        requiredStorageCapacity, requiredMemoryGB, requiredCores, workload);
    
    result.setRecommendedSpec(recommendedSpec);
    
    // 计算资源利用率
    calculateUtilization(result, totalDataSizeTB, workload);
    
    // 生成建议和警告
    generateRecommendationsAndWarnings(result, workload);
    
    return result;
  }
  
  private double calculateMemoryRequirement(WorkloadProfile workload) {
    // 基础内存需求：每个并发作业需要2-4GB内存
    double baseMemoryPerJob = 3.0; // GB
    double totalJobMemory = workload.getConcurrentJobs() * baseMemoryPerJob;
    
    // 系统开销：操作系统和Hadoop服务
    double systemOverhead = 8.0; // GB per node
    
    // 缓存需求：基于数据大小和访问模式
    double cacheMemory = workload.getAvgJobSizeGB() * 0.1; // 10%的数据缓存
    
    return totalJobMemory + systemOverhead + cacheMemory;
  }
  
  private int calculateCpuRequirement(WorkloadProfile workload) {
    // 基础CPU需求：每个并发作业需要1-2个核心
    double baseCoresPerJob = 1.5;
    double totalJobCores = workload.getConcurrentJobs() * baseCoresPerJob;
    
    // CPU密集型调整
    if (workload.getCpuIntensiveRatio() > 0.7) {
      totalJobCores *= 1.5;
    }
    
    // 系统开销：预留20%的CPU给系统
    return (int) Math.ceil(totalJobCores * 1.2);
  }
  
  private ClusterSpec generateRecommendedSpec(double requiredStorageCapacity, 
                                            double requiredMemoryGB, 
                                            int requiredCores,
                                            WorkloadProfile workload) {
    
    // 选择节点规格
    NodeSpec nodeSpec = selectOptimalNodeSpec(requiredMemoryGB, requiredCores, workload);
    
    // 计算节点数量
    int nodeCount = calculateNodeCount(requiredStorageCapacity, requiredMemoryGB, 
                                      requiredCores, nodeSpec);
    
    // 选择存储类型
    String storageType = selectStorageType(workload);
    
    // 选择网络带宽
    String networkBandwidth = selectNetworkBandwidth(workload, nodeCount);
    
    return new ClusterSpec(nodeCount, nodeSpec.cores, nodeSpec.memoryGB, 
                          nodeSpec.storageTB, storageType, networkBandwidth);
  }
  
  private static class NodeSpec {
    int cores;
    long memoryGB;
    long storageTB;
    
    NodeSpec(int cores, long memoryGB, long storageTB) {
      this.cores = cores;
      this.memoryGB = memoryGB;
      this.storageTB = storageTB;
    }
  }
  
  private NodeSpec selectOptimalNodeSpec(double requiredMemoryGB, int requiredCores, 
                                        WorkloadProfile workload) {
    
    // 预定义的节点规格选项
    NodeSpec[] nodeOptions = {
        new NodeSpec(16, 64, 12),   // 小型节点
        new NodeSpec(32, 128, 24),  // 中型节点
        new NodeSpec(48, 256, 36),  // 大型节点
        new NodeSpec(64, 512, 48)   // 超大型节点
    };
    
    // 根据工作负载特征选择最适合的节点规格
    for (NodeSpec spec : nodeOptions) {
      if (spec.cores >= requiredCores / 10 && // 假设10个节点
          spec.memoryGB >= requiredMemoryGB / 10) {
        return spec;
      }
    }
    
    // 如果没有合适的预定义规格，返回最大的
    return nodeOptions[nodeOptions.length - 1];
  }
  
  private int calculateNodeCount(double requiredStorageCapacity, double requiredMemoryGB, 
                               int requiredCores, NodeSpec nodeSpec) {
    
    // 基于存储需求计算节点数
    int nodesByStorage = (int) Math.ceil(requiredStorageCapacity / nodeSpec.storageTB);
    
    // 基于内存需求计算节点数
    int nodesByMemory = (int) Math.ceil(requiredMemoryGB / nodeSpec.memoryGB);
    
    // 基于CPU需求计算节点数
    int nodesByCpu = (int) Math.ceil((double) requiredCores / nodeSpec.cores);
    
    // 取最大值，确保满足所有资源需求
    int nodeCount = Math.max(Math.max(nodesByStorage, nodesByMemory), nodesByCpu);
    
    // 最少3个节点（考虑高可用）
    return Math.max(nodeCount, 3);
  }
  
  private String selectStorageType(WorkloadProfile workload) {
    if (workload.getIoIntensiveRatio() > 0.8) {
      return "SSD";
    } else if (workload.getIoIntensiveRatio() > 0.5) {
      return "Hybrid"; // SSD + HDD
    } else {
      return "HDD";
    }
  }
  
  private String selectNetworkBandwidth(WorkloadProfile workload, int nodeCount) {
    // 基于集群规模和工作负载选择网络带宽
    if (nodeCount > 100 || workload.getIoIntensiveRatio() > 0.7) {
      return "25Gbps";
    } else if (nodeCount > 50 || workload.getIoIntensiveRatio() > 0.5) {
      return "10Gbps";
    } else {
      return "1Gbps";
    }
  }
  
  private void calculateUtilization(CapacityPlanResult result, long totalDataSizeTB, 
                                   WorkloadProfile workload) {
    
    ClusterSpec spec = result.getRecommendedSpec();
    
    // 计算存储利用率
    double totalStorageCapacity = spec.getNodeCount() * spec.getStoragePerNodeTB();
    double effectiveDataSize = totalDataSizeTB * workload.getReplicationFactor() / 
                              workload.getCompressionRatio();
    result.setStorageUtilization(effectiveDataSize / totalStorageCapacity);
    
    // 计算内存利用率（简化计算）
    double totalMemory = spec.getNodeCount() * spec.getMemoryPerNodeGB();
    double requiredMemory = calculateMemoryRequirement(workload);
    result.setMemoryUtilization(requiredMemory / totalMemory);
    
    // 计算CPU利用率
    int totalCores = spec.getNodeCount() * spec.getCoresPerNode();
    int requiredCores = calculateCpuRequirement(workload);
    result.setCpuUtilization((double) requiredCores / totalCores);
    
    // 网络利用率（简化估算）
    result.setNetworkUtilization(0.3); // 假设30%的网络利用率
  }
  
  private void generateRecommendationsAndWarnings(CapacityPlanResult result, 
                                                 WorkloadProfile workload) {
    
    // 存储相关建议
    if (result.getStorageUtilization() > 0.9) {
      result.addWarning("存储利用率过高，建议增加存储容量或优化数据压缩");
    } else if (result.getStorageUtilization() < 0.5) {
      result.addRecommendation("存储利用率较低，可以考虑减少节点数量或增加数据量");
    }
    
    // 内存相关建议
    if (result.getMemoryUtilization() > 0.85) {
      result.addWarning("内存利用率过高，可能影响性能，建议增加内存配置");
    }
    
    // CPU相关建议
    if (result.getCpuUtilization() > 0.8) {
      result.addWarning("CPU利用率过高，建议增加CPU核心数或优化作业并发度");
    }
    
    // 工作负载相关建议
    if (workload.getCpuIntensiveRatio() > 0.7) {
      result.addRecommendation("工作负载为CPU密集型，建议选择高频CPU和优化算法");
    }
    
    if (workload.getIoIntensiveRatio() > 0.7) {
      result.addRecommendation("工作负载为IO密集型，建议使用SSD存储和高速网络");
    }
    
    // 高可用建议
    if (result.getRecommendedSpec().getNodeCount() < 5) {
      result.addRecommendation("集群节点数较少，建议启用NameNode HA和ResourceManager HA");
    }
  }
  
  /**
   * 生成部署报告
   */
  public String generateDeploymentReport(CapacityPlanResult result) {
    StringBuilder report = new StringBuilder();
    
    report.append("=== Hadoop集群容量规划报告 ===\n\n");
    
    ClusterSpec spec = result.getRecommendedSpec();
    report.append("推荐集群配置:\n");
    report.append(String.format("- 节点数量: %d\n", spec.getNodeCount()));
    report.append(String.format("- 每节点CPU核心: %d\n", spec.getCoresPerNode()));
    report.append(String.format("- 每节点内存: %d GB\n", spec.getMemoryPerNodeGB()));
    report.append(String.format("- 每节点存储: %d TB\n", spec.getStoragePerNodeTB()));
    report.append(String.format("- 存储类型: %s\n", spec.getStorageType()));
    report.append(String.format("- 网络带宽: %s\n", spec.getNetworkBandwidth()));
    
    report.append("\n资源利用率:\n");
    report.append(String.format("- 存储利用率: %.1f%%\n", result.getStorageUtilization() * 100));
    report.append(String.format("- 内存利用率: %.1f%%\n", result.getMemoryUtilization() * 100));
    report.append(String.format("- CPU利用率: %.1f%%\n", result.getCpuUtilization() * 100));
    report.append(String.format("- 网络利用率: %.1f%%\n", result.getNetworkUtilization() * 100));
    
    if (!result.getRecommendations().isEmpty()) {
      report.append("\n建议:\n");
      for (String recommendation : result.getRecommendations()) {
        report.append("- ").append(recommendation).append("\n");
      }
    }
    
    if (!result.getWarnings().isEmpty()) {
      report.append("\n警告:\n");
      for (String warning : result.getWarnings()) {
        report.append("- ").append(warning).append("\n");
      }
    }
    
    return report.toString();
  }
}
```

### 19.2.2 网络拓扑设计

合理的网络拓扑是Hadoop集群性能的重要保障：

```java
/**
 * Hadoop网络拓扑规划器
 */
public class NetworkTopologyPlanner {
  
  private static final Logger LOG = LoggerFactory.getLogger(NetworkTopologyPlanner.class);
  
  /**
   * 网络拓扑节点
   */
  public static class TopologyNode {
    private String name;
    private String type; // rack, switch, node
    private List<TopologyNode> children;
    private TopologyNode parent;
    private Map<String, Object> properties;
    
    public TopologyNode(String name, String type) {
      this.name = name;
      this.type = type;
      this.children = new ArrayList<>();
      this.properties = new HashMap<>();
    }
    
    public void addChild(TopologyNode child) {
      children.add(child);
      child.parent = this;
    }
    
    public String getPath() {
      if (parent == null) {
        return "/" + name;
      }
      return parent.getPath() + "/" + name;
    }
    
    // Getters and setters
    public String getName() { return name; }
    public String getType() { return type; }
    public List<TopologyNode> getChildren() { return children; }
    public TopologyNode getParent() { return parent; }
    public Map<String, Object> getProperties() { return properties; }
  }
  
  /**
   * 网络拓扑设计
   */
  public static class NetworkTopologyDesign {
    private TopologyNode root;
    private Map<String, TopologyNode> nodeMap;
    private int totalNodes;
    private int racksCount;
    private int nodesPerRack;
    
    public NetworkTopologyDesign() {
      this.nodeMap = new HashMap<>();
      this.root = new TopologyNode("cluster", "cluster");
    }
    
    // Getters and setters
    public TopologyNode getRoot() { return root; }
    public Map<String, TopologyNode> getNodeMap() { return nodeMap; }
    public int getTotalNodes() { return totalNodes; }
    public int getRacksCount() { return racksCount; }
    public int getNodesPerRack() { return nodesPerRack; }
    
    public void setTotalNodes(int totalNodes) { this.totalNodes = totalNodes; }
    public void setRacksCount(int racksCount) { this.racksCount = racksCount; }
    public void setNodesPerRack(int nodesPerRack) { this.nodesPerRack = nodesPerRack; }
  }
  
  /**
   * 设计网络拓扑
   */
  public NetworkTopologyDesign designTopology(int totalNodes, int maxNodesPerRack) {
    NetworkTopologyDesign design = new NetworkTopologyDesign();
    
    // 计算机架数量
    int racksCount = (int) Math.ceil((double) totalNodes / maxNodesPerRack);
    int nodesPerRack = (int) Math.ceil((double) totalNodes / racksCount);
    
    design.setTotalNodes(totalNodes);
    design.setRacksCount(racksCount);
    design.setNodesPerRack(nodesPerRack);
    
    // 创建机架和节点
    createRacksAndNodes(design, racksCount, nodesPerRack, totalNodes);
    
    // 优化拓扑结构
    optimizeTopology(design);
    
    return design;
  }
  
  private void createRacksAndNodes(NetworkTopologyDesign design, int racksCount, 
                                  int nodesPerRack, int totalNodes) {
    
    int nodeIndex = 1;
    
    for (int rackIndex = 1; rackIndex <= racksCount; rackIndex++) {
      // 创建机架
      TopologyNode rack = new TopologyNode("rack" + rackIndex, "rack");
      rack.getProperties().put("bandwidth", "10Gbps");
      rack.getProperties().put("switch_type", "ToR"); // Top of Rack
      
      design.getRoot().addChild(rack);
      design.getNodeMap().put(rack.getName(), rack);
      
      // 计算当前机架的节点数量
      int currentRackNodes = Math.min(nodesPerRack, totalNodes - nodeIndex + 1);
      
      // 创建节点
      for (int i = 0; i < currentRackNodes; i++) {
        TopologyNode node = new TopologyNode("node" + nodeIndex, "node");
        node.getProperties().put("ip", "192.168." + rackIndex + "." + (i + 1));
        node.getProperties().put("hostname", "hadoop-node" + nodeIndex);
        node.getProperties().put("bandwidth", "1Gbps");
        
        rack.addChild(node);
        design.getNodeMap().put(node.getName(), node);
        
        nodeIndex++;
      }
    }
  }
  
  private void optimizeTopology(NetworkTopologyDesign design) {
    // 检查机架平衡性
    balanceRacks(design);
    
    // 优化网络带宽配置
    optimizeBandwidth(design);
    
    // 添加冗余路径
    addRedundancy(design);
  }
  
  private void balanceRacks(NetworkTopologyDesign design) {
    List<TopologyNode> racks = design.getRoot().getChildren();
    
    // 计算节点分布
    Map<TopologyNode, Integer> rackNodeCounts = new HashMap<>();
    for (TopologyNode rack : racks) {
      rackNodeCounts.put(rack, rack.getChildren().size());
    }
    
    // 检查是否需要重新平衡
    int minNodes = rackNodeCounts.values().stream().mapToInt(Integer::intValue).min().orElse(0);
    int maxNodes = rackNodeCounts.values().stream().mapToInt(Integer::intValue).max().orElse(0);
    
    if (maxNodes - minNodes > 1) {
      LOG.info("Rack imbalance detected: min={}, max={}", minNodes, maxNodes);
      // 这里可以实现重新平衡逻辑
    }
  }
  
  private void optimizeBandwidth(NetworkTopologyDesign design) {
    // 根据集群规模调整网络带宽
    int totalNodes = design.getTotalNodes();
    
    String rackBandwidth;
    String nodeBandwidth;
    
    if (totalNodes > 1000) {
      rackBandwidth = "100Gbps";
      nodeBandwidth = "25Gbps";
    } else if (totalNodes > 100) {
      rackBandwidth = "40Gbps";
      nodeBandwidth = "10Gbps";
    } else {
      rackBandwidth = "10Gbps";
      nodeBandwidth = "1Gbps";
    }
    
    // 更新带宽配置
    for (TopologyNode rack : design.getRoot().getChildren()) {
      rack.getProperties().put("bandwidth", rackBandwidth);
      
      for (TopologyNode node : rack.getChildren()) {
        node.getProperties().put("bandwidth", nodeBandwidth);
      }
    }
  }
  
  private void addRedundancy(NetworkTopologyDesign design) {
    // 为大型集群添加冗余交换机
    if (design.getTotalNodes() > 500) {
      for (TopologyNode rack : design.getRoot().getChildren()) {
        rack.getProperties().put("redundant_switch", true);
        rack.getProperties().put("switch_count", 2);
      }
    }
  }
  
  /**
   * 生成Hadoop网络拓扑脚本
   */
  public String generateTopologyScript(NetworkTopologyDesign design) {
    StringBuilder script = new StringBuilder();
    
    script.append("#!/bin/bash\n");
    script.append("# Hadoop Network Topology Script\n");
    script.append("# Auto-generated by NetworkTopologyPlanner\n\n");
    
    script.append("# Function to get rack for a given node\n");
    script.append("get_rack() {\n");
    script.append("  local node=$1\n");
    script.append("  case $node in\n");
    
    // 生成节点到机架的映射
    for (TopologyNode rack : design.getRoot().getChildren()) {
      for (TopologyNode node : rack.getChildren()) {
        String hostname = (String) node.getProperties().get("hostname");
        String ip = (String) node.getProperties().get("ip");
        
        script.append(String.format("    %s|%s)\n", hostname, ip));
        script.append(String.format("      echo \"%s\"\n", rack.getPath()));
        script.append("      ;;\n");
      }
    }
    
    script.append("    *)\n");
    script.append("      echo \"/default-rack\"\n");
    script.append("      ;;\n");
    script.append("  esac\n");
    script.append("}\n\n");
    
    script.append("# Main execution\n");
    script.append("if [ $# -eq 0 ]; then\n");
    script.append("  echo \"Usage: $0 <node1> [node2] ...\"\n");
    script.append("  exit 1\n");
    script.append("fi\n\n");
    
    script.append("for node in \"$@\"; do\n");
    script.append("  rack=$(get_rack \"$node\")\n");
    script.append("  echo \"$rack\"\n");
    script.append("done\n");
    
    return script.toString();
  }
  
  /**
   * 生成网络配置文档
   */
  public String generateNetworkConfigDoc(NetworkTopologyDesign design) {
    StringBuilder doc = new StringBuilder();
    
    doc.append("# Hadoop集群网络配置文档\n\n");
    
    doc.append("## 集群概览\n");
    doc.append(String.format("- 总节点数: %d\n", design.getTotalNodes()));
    doc.append(String.format("- 机架数量: %d\n", design.getRacksCount()));
    doc.append(String.format("- 每机架节点数: %d\n", design.getNodesPerRack()));
    doc.append("\n");
    
    doc.append("## 网络拓扑结构\n");
    generateTopologyTree(doc, design.getRoot(), 0);
    doc.append("\n");
    
    doc.append("## 节点清单\n");
    doc.append("| 节点名称 | IP地址 | 机架位置 | 网络带宽 |\n");
    doc.append("|---------|--------|----------|----------|\n");
    
    for (TopologyNode rack : design.getRoot().getChildren()) {
      for (TopologyNode node : rack.getChildren()) {
        String hostname = (String) node.getProperties().get("hostname");
        String ip = (String) node.getProperties().get("ip");
        String bandwidth = (String) node.getProperties().get("bandwidth");
        
        doc.append(String.format("| %s | %s | %s | %s |\n", 
                                hostname, ip, rack.getPath(), bandwidth));
      }
    }
    
    doc.append("\n## 配置建议\n");
    generateConfigRecommendations(doc, design);
    
    return doc.toString();
  }
  
  private void generateTopologyTree(StringBuilder doc, TopologyNode node, int level) {
    String indent = "  ".repeat(level);
    doc.append(String.format("%s- %s (%s)\n", indent, node.getName(), node.getType()));
    
    for (TopologyNode child : node.getChildren()) {
      generateTopologyTree(doc, child, level + 1);
    }
  }
  
  private void generateConfigRecommendations(StringBuilder doc, NetworkTopologyDesign design) {
    doc.append("### core-site.xml配置\n");
    doc.append("```xml\n");
    doc.append("<property>\n");
    doc.append("  <name>net.topology.script.file.name</name>\n");
    doc.append("  <value>/etc/hadoop/conf/topology.sh</value>\n");
    doc.append("</property>\n");
    doc.append("```\n\n");
    
    doc.append("### hdfs-site.xml配置\n");
    doc.append("```xml\n");
    doc.append("<property>\n");
    doc.append("  <name>dfs.replication</name>\n");
    doc.append("  <value>3</value>\n");
    doc.append("</property>\n");
    doc.append("<property>\n");
    doc.append("  <name>dfs.namenode.replication.considerLoad</name>\n");
    doc.append("  <value>true</value>\n");
    doc.append("</property>\n");
    doc.append("```\n\n");
    
    if (design.getRacksCount() > 1) {
      doc.append("### 机架感知配置建议\n");
      doc.append("- 启用机架感知以优化数据本地性\n");
      doc.append("- 配置副本放置策略以提高容错能力\n");
      doc.append("- 监控跨机架网络流量\n\n");
    }
    
    if (design.getTotalNodes() > 100) {
      doc.append("### 大规模集群优化建议\n");
      doc.append("- 使用高速网络交换机\n");
      doc.append("- 配置网络QoS策略\n");
      doc.append("- 实施网络监控和告警\n");
      doc.append("- 考虑使用InfiniBand或高速以太网\n");
    }
  }
}
```

## 19.3 本章小结

本章深入分析了Hadoop集群部署与运维的关键技术和最佳实践。合理的集群规划和设计是成功部署的基础，需要综合考虑硬件选型、容量规划、网络拓扑等多个因素。

**核心要点**：

**容量规划**：基于工作负载特征进行科学的容量规划，确保资源配置的合理性。

**网络拓扑**：设计合理的网络拓扑结构，优化数据传输效率和容错能力。

**硬件选型**：根据应用场景选择合适的硬件配置，平衡性能和成本。

**自动化工具**：使用自动化工具简化部署过程，提高部署效率和一致性。

**监控运维**：建立完善的监控体系，确保集群的稳定运行和及时故障处理。

理解这些部署与运维技术，对于在生产环境中成功运行Hadoop集群具有重要意义。随着云计算和容器化技术的发展，Hadoop部署模式也在不断演进。
