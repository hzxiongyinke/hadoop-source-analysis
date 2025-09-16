# 第17章：自定义Hadoop组件开发

## 17.1 引言

Hadoop作为一个开放的分布式计算平台，提供了丰富的扩展机制，允许开发者根据特定的业务需求开发自定义组件。这种可扩展性是Hadoop生态系统能够适应各种应用场景的重要原因。通过开发自定义组件，我们可以扩展Hadoop的功能，优化特定场景下的性能，或者集成第三方系统。

自定义Hadoop组件开发涉及多个层面：从底层的存储格式和序列化机制，到中间层的输入输出格式和数据处理逻辑，再到上层的应用框架和用户接口。每个层面都有其特定的扩展点和开发模式。存储层面可以开发自定义的文件格式、压缩算法和序列化框架；计算层面可以开发自定义的MapReduce作业、YARN应用和调度器；服务层面可以开发自定义的RPC服务、监控组件和管理工具。

开发自定义Hadoop组件需要深入理解Hadoop的架构设计和核心接口。Hadoop采用了大量的设计模式，如工厂模式、策略模式、观察者模式等，这些模式为扩展开发提供了清晰的框架。同时，Hadoop的配置系统、服务框架、RPC机制等基础设施也为自定义组件提供了强大的支撑。掌握这些基础知识，是开发高质量自定义组件的前提。

本章将系统性地介绍Hadoop自定义组件开发的理论基础、技术方法和最佳实践，通过具体的代码示例和实战案例，帮助读者掌握从简单的数据格式扩展到复杂的分布式服务开发的完整技能体系。

## 17.2 自定义输入输出格式

### 17.2.1 自定义InputFormat开发

InputFormat是MapReduce作业读取数据的核心接口：

```java
/**
 * 自定义JSON输入格式
 */
public class JsonInputFormat extends FileInputFormat<LongWritable, JsonWritable> {
  
  @Override
  public RecordReader<LongWritable, JsonWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    
    return new JsonRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    // JSON文件通常不可分割，除非有特殊的分割策略
    return false;
  }
  
  /**
   * JSON记录读取器
   */
  public static class JsonRecordReader extends RecordReader<LongWritable, JsonWritable> {
    
    private LineRecordReader lineReader;
    private LongWritable key;
    private JsonWritable value;
    private ObjectMapper objectMapper;
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
      
      lineReader = new LineRecordReader();
      lineReader.initialize(split, context);
      
      objectMapper = new ObjectMapper();
      objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
      objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      if (!lineReader.nextKeyValue()) {
        return false;
      }
      
      // 获取当前行
      Text currentLine = lineReader.getCurrentValue();
      
      try {
        // 解析JSON
        JsonNode jsonNode = objectMapper.readTree(currentLine.toString());
        
        // 设置键值对
        key = lineReader.getCurrentKey();
        value = new JsonWritable(jsonNode);
        
        return true;
        
      } catch (JsonProcessingException e) {
        // 跳过无效的JSON行
        LOG.warn("Invalid JSON line: {}", currentLine.toString(), e);
        return nextKeyValue(); // 递归调用读取下一行
      }
    }
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }
    
    @Override
    public JsonWritable getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
      return lineReader.getProgress();
    }
    
    @Override
    public void close() throws IOException {
      if (lineReader != null) {
        lineReader.close();
      }
    }
  }
}

/**
 * JSON数据的Writable包装类
 */
public class JsonWritable implements Writable {
  
  private JsonNode jsonNode;
  private ObjectMapper objectMapper;
  
  public JsonWritable() {
    this.objectMapper = new ObjectMapper();
  }
  
  public JsonWritable(JsonNode jsonNode) {
    this();
    this.jsonNode = jsonNode;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    if (jsonNode == null) {
      out.writeBoolean(false);
    } else {
      out.writeBoolean(true);
      String jsonString = objectMapper.writeValueAsString(jsonNode);
      Text.writeString(out, jsonString);
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    boolean hasValue = in.readBoolean();
    if (hasValue) {
      String jsonString = Text.readString(in);
      try {
        jsonNode = objectMapper.readTree(jsonString);
      } catch (JsonProcessingException e) {
        throw new IOException("Failed to parse JSON", e);
      }
    } else {
      jsonNode = null;
    }
  }
  
  public JsonNode getJsonNode() {
    return jsonNode;
  }
  
  public void setJsonNode(JsonNode jsonNode) {
    this.jsonNode = jsonNode;
  }
  
  /**
   * 获取JSON字段值
   */
  public String getString(String fieldName) {
    if (jsonNode == null || !jsonNode.has(fieldName)) {
      return null;
    }
    return jsonNode.get(fieldName).asText();
  }
  
  public int getInt(String fieldName, int defaultValue) {
    if (jsonNode == null || !jsonNode.has(fieldName)) {
      return defaultValue;
    }
    return jsonNode.get(fieldName).asInt(defaultValue);
  }
  
  public long getLong(String fieldName, long defaultValue) {
    if (jsonNode == null || !jsonNode.has(fieldName)) {
      return defaultValue;
    }
    return jsonNode.get(fieldName).asLong(defaultValue);
  }
  
  public double getDouble(String fieldName, double defaultValue) {
    if (jsonNode == null || !jsonNode.has(fieldName)) {
      return defaultValue;
    }
    return jsonNode.get(fieldName).asDouble(defaultValue);
  }
  
  public boolean getBoolean(String fieldName, boolean defaultValue) {
    if (jsonNode == null || !jsonNode.has(fieldName)) {
      return defaultValue;
    }
    return jsonNode.get(fieldName).asBoolean(defaultValue);
  }
  
  @Override
  public String toString() {
    if (jsonNode == null) {
      return "null";
    }
    return jsonNode.toString();
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    
    JsonWritable that = (JsonWritable) obj;
    return Objects.equals(jsonNode, that.jsonNode);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(jsonNode);
  }
}

/**
 * 多行JSON输入格式（支持格式化的JSON）
 */
public class MultiLineJsonInputFormat extends FileInputFormat<LongWritable, JsonWritable> {
  
  @Override
  public RecordReader<LongWritable, JsonWritable> createRecordReader(
      InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
    
    return new MultiLineJsonRecordReader();
  }
  
  @Override
  protected boolean isSplitable(JobContext context, Path filename) {
    return false; // 多行JSON不支持分割
  }
  
  /**
   * 多行JSON记录读取器
   */
  public static class MultiLineJsonRecordReader extends RecordReader<LongWritable, JsonWritable> {
    
    private BufferedReader reader;
    private LongWritable key;
    private JsonWritable value;
    private ObjectMapper objectMapper;
    private long recordCount;
    private long totalBytes;
    private long processedBytes;
    
    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) 
        throws IOException, InterruptedException {
      
      FileSplit fileSplit = (FileSplit) split;
      Path file = fileSplit.getPath();
      FileSystem fs = file.getFileSystem(context.getConfiguration());
      
      FSDataInputStream inputStream = fs.open(file);
      reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8));
      
      objectMapper = new ObjectMapper();
      objectMapper.configure(JsonParser.Feature.ALLOW_COMMENTS, true);
      objectMapper.configure(JsonParser.Feature.ALLOW_TRAILING_COMMA, true);
      
      recordCount = 0;
      totalBytes = fileSplit.getLength();
      processedBytes = 0;
    }
    
    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
      StringBuilder jsonBuilder = new StringBuilder();
      String line;
      int braceCount = 0;
      boolean inString = false;
      boolean escaped = false;
      
      // 读取完整的JSON对象
      while ((line = reader.readLine()) != null) {
        processedBytes += line.getBytes(StandardCharsets.UTF_8).length + 1; // +1 for newline
        
        for (char c : line.toCharArray()) {
          if (escaped) {
            escaped = false;
            continue;
          }
          
          if (c == '\\') {
            escaped = true;
            continue;
          }
          
          if (c == '"' && !escaped) {
            inString = !inString;
          }
          
          if (!inString) {
            if (c == '{') {
              braceCount++;
            } else if (c == '}') {
              braceCount--;
            }
          }
        }
        
        jsonBuilder.append(line).append('\n');
        
        // 如果找到完整的JSON对象
        if (braceCount == 0 && jsonBuilder.length() > 0) {
          try {
            String jsonString = jsonBuilder.toString().trim();
            if (!jsonString.isEmpty()) {
              JsonNode jsonNode = objectMapper.readTree(jsonString);
              
              key = new LongWritable(recordCount++);
              value = new JsonWritable(jsonNode);
              
              return true;
            }
          } catch (JsonProcessingException e) {
            LOG.warn("Invalid JSON object: {}", jsonBuilder.toString(), e);
          }
          
          // 重置构建器
          jsonBuilder.setLength(0);
        }
      }
      
      return false;
    }
    
    @Override
    public LongWritable getCurrentKey() throws IOException, InterruptedException {
      return key;
    }
    
    @Override
    public JsonWritable getCurrentValue() throws IOException, InterruptedException {
      return value;
    }
    
    @Override
    public float getProgress() throws IOException, InterruptedException {
      if (totalBytes == 0) {
        return 1.0f;
      }
      return Math.min(1.0f, (float) processedBytes / totalBytes);
    }
    
    @Override
    public void close() throws IOException {
      if (reader != null) {
        reader.close();
      }
    }
  }
}
```

### 17.2.2 自定义OutputFormat开发

OutputFormat负责将MapReduce作业的输出写入存储系统：

```java
/**
 * 自定义JSON输出格式
 */
public class JsonOutputFormat<K, V> extends FileOutputFormat<K, V> {
  
  public static final String JSON_OUTPUT_PRETTY_PRINT = "json.output.pretty.print";
  public static final String JSON_OUTPUT_ARRAY_FORMAT = "json.output.array.format";
  
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    
    Configuration conf = context.getConfiguration();
    boolean prettyPrint = conf.getBoolean(JSON_OUTPUT_PRETTY_PRINT, false);
    boolean arrayFormat = conf.getBoolean(JSON_OUTPUT_ARRAY_FORMAT, false);
    
    Path file = getDefaultWorkFile(context, ".json");
    FileSystem fs = file.getFileSystem(conf);
    FSDataOutputStream fileOut = fs.create(file, false);
    
    return new JsonRecordWriter<>(fileOut, prettyPrint, arrayFormat);
  }
  
  /**
   * JSON记录写入器
   */
  public static class JsonRecordWriter<K, V> extends RecordWriter<K, V> {
    
    private final FSDataOutputStream out;
    private final ObjectMapper objectMapper;
    private final boolean arrayFormat;
    private boolean firstRecord = true;
    
    public JsonRecordWriter(FSDataOutputStream out, boolean prettyPrint, boolean arrayFormat) {
      this.out = out;
      this.arrayFormat = arrayFormat;
      
      this.objectMapper = new ObjectMapper();
      if (prettyPrint) {
        objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
      }
      
      try {
        if (arrayFormat) {
          out.writeBytes("[\n");
        }
      } catch (IOException e) {
        throw new RuntimeException("Failed to write array start", e);
      }
    }
    
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      JsonNode jsonNode = convertToJsonNode(key, value);
      
      if (arrayFormat) {
        if (!firstRecord) {
          out.writeBytes(",\n");
        }
        firstRecord = false;
      }
      
      String jsonString = objectMapper.writeValueAsString(jsonNode);
      out.writeBytes(jsonString);
      
      if (!arrayFormat) {
        out.writeBytes("\n");
      }
    }
    
    private JsonNode convertToJsonNode(K key, V value) {
      ObjectNode objectNode = objectMapper.createObjectNode();
      
      // 处理键
      if (key != null) {
        if (key instanceof Writable) {
          objectNode.set("key", convertWritableToJsonNode((Writable) key));
        } else {
          objectNode.put("key", key.toString());
        }
      }
      
      // 处理值
      if (value != null) {
        if (value instanceof JsonWritable) {
          JsonWritable jsonWritable = (JsonWritable) value;
          return jsonWritable.getJsonNode();
        } else if (value instanceof Writable) {
          objectNode.set("value", convertWritableToJsonNode((Writable) value));
        } else {
          objectNode.put("value", value.toString());
        }
      }
      
      return objectNode;
    }
    
    private JsonNode convertWritableToJsonNode(Writable writable) {
      if (writable instanceof Text) {
        return objectMapper.valueToTree(((Text) writable).toString());
      } else if (writable instanceof IntWritable) {
        return objectMapper.valueToTree(((IntWritable) writable).get());
      } else if (writable instanceof LongWritable) {
        return objectMapper.valueToTree(((LongWritable) writable).get());
      } else if (writable instanceof DoubleWritable) {
        return objectMapper.valueToTree(((DoubleWritable) writable).get());
      } else if (writable instanceof BooleanWritable) {
        return objectMapper.valueToTree(((BooleanWritable) writable).get());
      } else {
        return objectMapper.valueToTree(writable.toString());
      }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      try {
        if (arrayFormat) {
          out.writeBytes("\n]");
        }
      } finally {
        out.close();
      }
    }
  }
}

/**
 * 多文件输出格式（根据键值分别输出到不同文件）
 */
public class MultiFileJsonOutputFormat<K, V> extends FileOutputFormat<K, V> {
  
  public static final String MULTI_FILE_OUTPUT_PARTITION_FIELD = "multi.file.output.partition.field";
  
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    
    return new MultiFileJsonRecordWriter<>(context);
  }
  
  /**
   * 多文件JSON记录写入器
   */
  public static class MultiFileJsonRecordWriter<K, V> extends RecordWriter<K, V> {
    
    private final TaskAttemptContext context;
    private final Map<String, RecordWriter<K, V>> recordWriters;
    private final String partitionField;
    
    public MultiFileJsonRecordWriter(TaskAttemptContext context) {
      this.context = context;
      this.recordWriters = new HashMap<>();
      this.partitionField = context.getConfiguration().get(MULTI_FILE_OUTPUT_PARTITION_FIELD);
    }
    
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      String partition = getPartition(key, value);
      
      RecordWriter<K, V> writer = recordWriters.get(partition);
      if (writer == null) {
        writer = createRecordWriter(partition);
        recordWriters.put(partition, writer);
      }
      
      writer.write(key, value);
    }
    
    private String getPartition(K key, V value) {
      if (partitionField != null && value instanceof JsonWritable) {
        JsonWritable jsonValue = (JsonWritable) value;
        String fieldValue = jsonValue.getString(partitionField);
        if (fieldValue != null) {
          return fieldValue;
        }
      }
      
      // 默认分区
      return "default";
    }
    
    private RecordWriter<K, V> createRecordWriter(String partition) throws IOException {
      Configuration conf = context.getConfiguration();
      
      // 创建分区特定的文件路径
      Path outputDir = FileOutputFormat.getOutputPath(context);
      Path partitionFile = new Path(outputDir, partition + ".json");
      
      FileSystem fs = partitionFile.getFileSystem(conf);
      FSDataOutputStream fileOut = fs.create(partitionFile, false);
      
      boolean prettyPrint = conf.getBoolean(JsonOutputFormat.JSON_OUTPUT_PRETTY_PRINT, false);
      boolean arrayFormat = conf.getBoolean(JsonOutputFormat.JSON_OUTPUT_ARRAY_FORMAT, false);
      
      return new JsonOutputFormat.JsonRecordWriter<>(fileOut, prettyPrint, arrayFormat);
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      for (RecordWriter<K, V> writer : recordWriters.values()) {
        writer.close(context);
      }
      recordWriters.clear();
    }
  }
}

/**
 * 数据库输出格式
 */
public class DatabaseOutputFormat<K extends DBWritable, V> extends OutputFormat<K, V> {
  
  public static final String DATABASE_OUTPUT_URL = "database.output.url";
  public static final String DATABASE_OUTPUT_USERNAME = "database.output.username";
  public static final String DATABASE_OUTPUT_PASSWORD = "database.output.password";
  public static final String DATABASE_OUTPUT_TABLE = "database.output.table";
  public static final String DATABASE_OUTPUT_BATCH_SIZE = "database.output.batch.size";
  
  @Override
  public RecordWriter<K, V> getRecordWriter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    
    Configuration conf = context.getConfiguration();
    
    String url = conf.get(DATABASE_OUTPUT_URL);
    String username = conf.get(DATABASE_OUTPUT_USERNAME);
    String password = conf.get(DATABASE_OUTPUT_PASSWORD);
    String table = conf.get(DATABASE_OUTPUT_TABLE);
    int batchSize = conf.getInt(DATABASE_OUTPUT_BATCH_SIZE, 1000);
    
    if (url == null || table == null) {
      throw new IOException("Database URL and table must be specified");
    }
    
    return new DatabaseRecordWriter<>(url, username, password, table, batchSize);
  }
  
  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    
    String url = conf.get(DATABASE_OUTPUT_URL);
    String table = conf.get(DATABASE_OUTPUT_TABLE);
    
    if (url == null) {
      throw new IOException("Database URL not specified");
    }
    
    if (table == null) {
      throw new IOException("Database table not specified");
    }
  }
  
  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context) 
      throws IOException, InterruptedException {
    return new NullOutputCommitter();
  }
  
  /**
   * 数据库记录写入器
   */
  public static class DatabaseRecordWriter<K extends DBWritable, V> extends RecordWriter<K, V> {
    
    private final Connection connection;
    private final PreparedStatement statement;
    private final int batchSize;
    private int recordCount;
    
    public DatabaseRecordWriter(String url, String username, String password, 
                               String table, int batchSize) throws IOException {
      try {
        this.batchSize = batchSize;
        this.recordCount = 0;
        
        // 建立数据库连接
        this.connection = DriverManager.getConnection(url, username, password);
        this.connection.setAutoCommit(false);
        
        // 创建预编译语句
        this.statement = createPreparedStatement(connection, table);
        
      } catch (SQLException e) {
        throw new IOException("Failed to create database connection", e);
      }
    }
    
    private PreparedStatement createPreparedStatement(Connection conn, String table) 
        throws SQLException {
      
      // 这里简化为通用的插入语句，实际应用中需要根据具体的表结构来构建
      String sql = "INSERT INTO " + table + " VALUES (?, ?)";
      return conn.prepareStatement(sql);
    }
    
    @Override
    public void write(K key, V value) throws IOException, InterruptedException {
      try {
        // 设置参数
        key.write(statement);
        
        // 添加到批处理
        statement.addBatch();
        recordCount++;
        
        // 达到批处理大小时执行
        if (recordCount >= batchSize) {
          executeBatch();
        }
        
      } catch (SQLException e) {
        throw new IOException("Failed to write record to database", e);
      }
    }
    
    private void executeBatch() throws SQLException {
      if (recordCount > 0) {
        statement.executeBatch();
        connection.commit();
        recordCount = 0;
      }
    }
    
    @Override
    public void close(TaskAttemptContext context) throws IOException, InterruptedException {
      try {
        // 执行剩余的批处理
        executeBatch();
        
      } catch (SQLException e) {
        LOG.error("Failed to execute final batch", e);
      } finally {
        try {
          if (statement != null) {
            statement.close();
          }
          if (connection != null) {
            connection.close();
          }
        } catch (SQLException e) {
          LOG.error("Failed to close database resources", e);
        }
      }
    }
  }
  
  /**
   * 空输出提交器
   */
  public static class NullOutputCommitter extends OutputCommitter {
    
    @Override
    public void setupJob(JobContext jobContext) throws IOException {
      // 不需要设置
    }
    
    @Override
    public void setupTask(TaskAttemptContext taskContext) throws IOException {
      // 不需要设置
    }
    
    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) throws IOException {
      return false;
    }
    
    @Override
    public void commitTask(TaskAttemptContext taskContext) throws IOException {
      // 不需要提交
    }
    
    @Override
    public void abortTask(TaskAttemptContext taskContext) throws IOException {
      // 不需要中止
    }
  }
}
```

## 17.3 自定义序列化机制

### 17.3.1 自定义Writable类型

Writable是Hadoop的核心序列化接口：

```java
/**
 * 自定义用户信息Writable类型
 */
public class UserInfoWritable implements Writable, Comparable<UserInfoWritable> {
  
  private long userId;
  private String username;
  private String email;
  private int age;
  private double score;
  private List<String> tags;
  private Map<String, String> attributes;
  
  public UserInfoWritable() {
    this.tags = new ArrayList<>();
    this.attributes = new HashMap<>();
  }
  
  public UserInfoWritable(long userId, String username, String email, 
                         int age, double score) {
    this();
    this.userId = userId;
    this.username = username;
    this.email = email;
    this.age = age;
    this.score = score;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    // 写入基本类型
    out.writeLong(userId);
    out.writeInt(age);
    out.writeDouble(score);
    
    // 写入字符串（处理null值）
    Text.writeString(out, username != null ? username : "");
    Text.writeString(out, email != null ? email : "");
    
    // 写入列表
    out.writeInt(tags.size());
    for (String tag : tags) {
      Text.writeString(out, tag);
    }
    
    // 写入映射
    out.writeInt(attributes.size());
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      Text.writeString(out, entry.getKey());
      Text.writeString(out, entry.getValue());
    }
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    // 读取基本类型
    userId = in.readLong();
    age = in.readInt();
    score = in.readDouble();
    
    // 读取字符串
    username = Text.readString(in);
    email = Text.readString(in);
    
    // 处理空字符串
    if (username.isEmpty()) username = null;
    if (email.isEmpty()) email = null;
    
    // 读取列表
    tags.clear();
    int tagCount = in.readInt();
    for (int i = 0; i < tagCount; i++) {
      tags.add(Text.readString(in));
    }
    
    // 读取映射
    attributes.clear();
    int attrCount = in.readInt();
    for (int i = 0; i < attrCount; i++) {
      String key = Text.readString(in);
      String value = Text.readString(in);
      attributes.put(key, value);
    }
  }
  
  @Override
  public int compareTo(UserInfoWritable other) {
    // 首先按用户ID比较
    int result = Long.compare(this.userId, other.userId);
    if (result != 0) {
      return result;
    }
    
    // 然后按用户名比较
    if (this.username != null && other.username != null) {
      result = this.username.compareTo(other.username);
      if (result != 0) {
        return result;
      }
    } else if (this.username != null) {
      return 1;
    } else if (other.username != null) {
      return -1;
    }
    
    // 最后按分数比较
    return Double.compare(this.score, other.score);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    
    UserInfoWritable that = (UserInfoWritable) obj;
    
    return userId == that.userId &&
           age == that.age &&
           Double.compare(that.score, score) == 0 &&
           Objects.equals(username, that.username) &&
           Objects.equals(email, that.email) &&
           Objects.equals(tags, that.tags) &&
           Objects.equals(attributes, that.attributes);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(userId, username, email, age, score, tags, attributes);
  }
  
  @Override
  public String toString() {
    return String.format("UserInfo{id=%d, username='%s', email='%s', age=%d, score=%.2f, tags=%s, attributes=%s}",
                        userId, username, email, age, score, tags, attributes);
  }
  
  // Getters and setters
  public long getUserId() { return userId; }
  public void setUserId(long userId) { this.userId = userId; }
  
  public String getUsername() { return username; }
  public void setUsername(String username) { this.username = username; }
  
  public String getEmail() { return email; }
  public void setEmail(String email) { this.email = email; }
  
  public int getAge() { return age; }
  public void setAge(int age) { this.age = age; }
  
  public double getScore() { return score; }
  public void setScore(double score) { this.score = score; }
  
  public List<String> getTags() { return tags; }
  public void setTags(List<String> tags) { this.tags = tags != null ? tags : new ArrayList<>(); }
  
  public Map<String, String> getAttributes() { return attributes; }
  public void setAttributes(Map<String, String> attributes) { 
    this.attributes = attributes != null ? attributes : new HashMap<>(); 
  }
  
  public void addTag(String tag) {
    if (tag != null && !tag.isEmpty()) {
      tags.add(tag);
    }
  }
  
  public void setAttribute(String key, String value) {
    if (key != null && !key.isEmpty()) {
      attributes.put(key, value);
    }
  }
  
  public String getAttribute(String key) {
    return attributes.get(key);
  }
}
```

## 17.4 本章小结

本章深入分析了Hadoop自定义组件开发的理论基础和技术实现。通过开发自定义组件，我们可以扩展Hadoop的功能，适应特定的业务需求。

**核心要点**：

**输入输出格式**：自定义InputFormat和OutputFormat是扩展数据处理能力的重要手段。

**序列化机制**：自定义Writable类型提供了高效的数据序列化和传输能力。

**接口设计**：遵循Hadoop的接口规范，确保组件的兼容性和可扩展性。

**性能优化**：在开发过程中要考虑性能因素，如序列化效率、内存使用等。

**错误处理**：完善的错误处理机制是生产环境中稳定运行的保障。

### 17.3.2 自定义RawComparator

RawComparator用于在不反序列化的情况下直接比较字节数据：

```java
/**
 * UserInfoWritable的原始比较器
 */
public class UserInfoRawComparator implements RawComparator<UserInfoWritable> {

  @Override
  public int compare(UserInfoWritable o1, UserInfoWritable o2) {
    return o1.compareTo(o2);
  }

  @Override
  public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      // 比较用户ID（前8个字节）
      long userId1 = readLong(b1, s1);
      long userId2 = readLong(b2, s2);

      int result = Long.compare(userId1, userId2);
      if (result != 0) {
        return result;
      }

      // 如果用户ID相同，需要进一步比较
      // 这里简化处理，实际应用中可能需要更复杂的逻辑
      return compareBytes(b1, s1, l1, b2, s2, l2);

    } catch (Exception e) {
      // 如果原始比较失败，回退到对象比较
      return compareObjects(b1, s1, l1, b2, s2, l2);
    }
  }

  private long readLong(byte[] bytes, int start) {
    return ((long) (bytes[start] & 0xff) << 56) |
           ((long) (bytes[start + 1] & 0xff) << 48) |
           ((long) (bytes[start + 2] & 0xff) << 40) |
           ((long) (bytes[start + 3] & 0xff) << 32) |
           ((long) (bytes[start + 4] & 0xff) << 24) |
           ((long) (bytes[start + 5] & 0xff) << 16) |
           ((long) (bytes[start + 6] & 0xff) << 8) |
           ((long) (bytes[start + 7] & 0xff));
  }

  private int compareBytes(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    int minLength = Math.min(l1, l2);

    for (int i = 0; i < minLength; i++) {
      int result = Byte.compare(b1[s1 + i], b2[s2 + i]);
      if (result != 0) {
        return result;
      }
    }

    return Integer.compare(l1, l2);
  }

  private int compareObjects(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
    try {
      UserInfoWritable obj1 = new UserInfoWritable();
      UserInfoWritable obj2 = new UserInfoWritable();

      DataInputStream dis1 = new DataInputStream(new ByteArrayInputStream(b1, s1, l1));
      DataInputStream dis2 = new DataInputStream(new ByteArrayInputStream(b2, s2, l2));

      obj1.readFields(dis1);
      obj2.readFields(dis2);

      return obj1.compareTo(obj2);

    } catch (IOException e) {
      throw new RuntimeException("Failed to compare objects", e);
    }
  }
}
```

## 17.4 自定义MapReduce组件

### 17.4.1 自定义Mapper和Reducer

开发自定义的MapReduce处理逻辑：

```java
/**
 * 用户数据分析Mapper
 */
public class UserAnalysisMapper extends Mapper<LongWritable, Text, Text, UserInfoWritable> {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Text outputKey = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);

    // 配置JSON解析器
    objectMapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_FIELD_NAMES, true);
    objectMapper.configure(JsonParser.Feature.ALLOW_SINGLE_QUOTES, true);
  }

  @Override
  protected void map(LongWritable key, Text value, Context context)
      throws IOException, InterruptedException {

    try {
      // 解析JSON数据
      JsonNode jsonNode = objectMapper.readTree(value.toString());

      // 提取用户信息
      UserInfoWritable userInfo = parseUserInfo(jsonNode);

      if (userInfo != null) {
        // 按年龄段分组
        String ageGroup = getAgeGroup(userInfo.getAge());
        outputKey.set(ageGroup);

        context.write(outputKey, userInfo);

        // 更新计数器
        context.getCounter("UserAnalysis", "ProcessedUsers").increment(1);
      }

    } catch (JsonProcessingException e) {
      // 记录无效数据
      context.getCounter("UserAnalysis", "InvalidRecords").increment(1);
      LOG.warn("Invalid JSON record: {}", value.toString(), e);
    }
  }

  private UserInfoWritable parseUserInfo(JsonNode jsonNode) {
    try {
      long userId = jsonNode.get("user_id").asLong();
      String username = jsonNode.get("username").asText();
      String email = jsonNode.get("email").asText();
      int age = jsonNode.get("age").asInt();
      double score = jsonNode.get("score").asDouble();

      UserInfoWritable userInfo = new UserInfoWritable(userId, username, email, age, score);

      // 解析标签
      JsonNode tagsNode = jsonNode.get("tags");
      if (tagsNode != null && tagsNode.isArray()) {
        for (JsonNode tagNode : tagsNode) {
          userInfo.addTag(tagNode.asText());
        }
      }

      // 解析属性
      JsonNode attributesNode = jsonNode.get("attributes");
      if (attributesNode != null && attributesNode.isObject()) {
        attributesNode.fields().forEachRemaining(entry -> {
          userInfo.setAttribute(entry.getKey(), entry.getValue().asText());
        });
      }

      return userInfo;

    } catch (Exception e) {
      LOG.warn("Failed to parse user info from JSON: {}", jsonNode.toString(), e);
      return null;
    }
  }

  private String getAgeGroup(int age) {
    if (age < 18) {
      return "under_18";
    } else if (age < 30) {
      return "18_29";
    } else if (age < 50) {
      return "30_49";
    } else if (age < 65) {
      return "50_64";
    } else {
      return "65_plus";
    }
  }
}

/**
 * 用户统计Reducer
 */
public class UserStatisticsReducer extends Reducer<Text, UserInfoWritable, Text, Text> {

  private final ObjectMapper objectMapper = new ObjectMapper();
  private final Text outputValue = new Text();

  @Override
  protected void setup(Context context) throws IOException, InterruptedException {
    super.setup(context);
    objectMapper.enable(SerializationFeature.INDENT_OUTPUT);
  }

  @Override
  protected void reduce(Text key, Iterable<UserInfoWritable> values, Context context)
      throws IOException, InterruptedException {

    UserGroupStatistics stats = new UserGroupStatistics(key.toString());

    // 统计用户数据
    for (UserInfoWritable user : values) {
      stats.addUser(user);
    }

    // 计算统计结果
    stats.calculateStatistics();

    try {
      // 输出JSON格式的统计结果
      String jsonOutput = objectMapper.writeValueAsString(stats);
      outputValue.set(jsonOutput);

      context.write(key, outputValue);

      // 更新计数器
      context.getCounter("UserStatistics", "ProcessedGroups").increment(1);
      context.getCounter("UserStatistics", "TotalUsers").increment(stats.getUserCount());

    } catch (JsonProcessingException e) {
      LOG.error("Failed to serialize statistics for group: " + key.toString(), e);
    }
  }

  /**
   * 用户组统计信息
   */
  public static class UserGroupStatistics {

    private String ageGroup;
    private int userCount;
    private double totalScore;
    private double averageScore;
    private double minScore;
    private double maxScore;
    private int totalAge;
    private double averageAge;
    private Map<String, Integer> tagCounts;
    private Map<String, Integer> attributeCounts;

    public UserGroupStatistics(String ageGroup) {
      this.ageGroup = ageGroup;
      this.userCount = 0;
      this.totalScore = 0.0;
      this.minScore = Double.MAX_VALUE;
      this.maxScore = Double.MIN_VALUE;
      this.totalAge = 0;
      this.tagCounts = new HashMap<>();
      this.attributeCounts = new HashMap<>();
    }

    public void addUser(UserInfoWritable user) {
      userCount++;

      // 分数统计
      double score = user.getScore();
      totalScore += score;
      minScore = Math.min(minScore, score);
      maxScore = Math.max(maxScore, score);

      // 年龄统计
      totalAge += user.getAge();

      // 标签统计
      for (String tag : user.getTags()) {
        tagCounts.merge(tag, 1, Integer::sum);
      }

      // 属性统计
      for (String attrKey : user.getAttributes().keySet()) {
        attributeCounts.merge(attrKey, 1, Integer::sum);
      }
    }

    public void calculateStatistics() {
      if (userCount > 0) {
        averageScore = totalScore / userCount;
        averageAge = (double) totalAge / userCount;
      }

      if (userCount == 0) {
        minScore = 0.0;
        maxScore = 0.0;
      }
    }

    // Getters for JSON serialization
    public String getAgeGroup() { return ageGroup; }
    public int getUserCount() { return userCount; }
    public double getTotalScore() { return totalScore; }
    public double getAverageScore() { return averageScore; }
    public double getMinScore() { return minScore; }
    public double getMaxScore() { return maxScore; }
    public double getAverageAge() { return averageAge; }
    public Map<String, Integer> getTagCounts() { return tagCounts; }
    public Map<String, Integer> getAttributeCounts() { return attributeCounts; }
  }
}

/**
 * 自定义Partitioner
 */
public class UserInfoPartitioner extends Partitioner<Text, UserInfoWritable> {

  @Override
  public int getPartition(Text key, UserInfoWritable value, int numPartitions) {
    // 根据年龄段进行分区
    String ageGroup = key.toString();

    switch (ageGroup) {
      case "under_18":
        return 0 % numPartitions;
      case "18_29":
        return 1 % numPartitions;
      case "30_49":
        return 2 % numPartitions;
      case "50_64":
        return 3 % numPartitions;
      case "65_plus":
        return 4 % numPartitions;
      default:
        return (ageGroup.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
  }
}

/**
 * 自定义Combiner
 */
public class UserStatisticsCombiner extends Reducer<Text, UserInfoWritable, Text, UserInfoWritable> {

  @Override
  protected void reduce(Text key, Iterable<UserInfoWritable> values, Context context)
      throws IOException, InterruptedException {

    // Combiner阶段只是简单地传递数据，不进行聚合
    // 因为我们需要在Reducer阶段进行完整的统计计算
    for (UserInfoWritable user : values) {
      context.write(key, user);
    }
  }
}
```

### 17.4.2 自定义作业配置

创建完整的MapReduce作业配置：

```java
/**
 * 用户分析作业驱动程序
 */
public class UserAnalysisJob extends Configured implements Tool {

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new UserAnalysisJob(), args);
    System.exit(exitCode);
  }

  @Override
  public int run(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: UserAnalysisJob <input path> <output path>");
      return -1;
    }

    Configuration conf = getConf();

    // 设置自定义配置
    setupJobConfiguration(conf);

    Job job = Job.getInstance(conf, "user-analysis");
    job.setJarByClass(UserAnalysisJob.class);

    // 设置输入输出路径
    FileInputFormat.addInputPath(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // 设置输入输出格式
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(JsonOutputFormat.class);

    // 设置Mapper和Reducer
    job.setMapperClass(UserAnalysisMapper.class);
    job.setCombinerClass(UserStatisticsCombiner.class);
    job.setReducerClass(UserStatisticsReducer.class);
    job.setPartitionerClass(UserInfoPartitioner.class);

    // 设置输出键值类型
    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(UserInfoWritable.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // 设置Reducer数量
    job.setNumReduceTasks(5); // 对应5个年龄段

    // 等待作业完成
    boolean success = job.waitForCompletion(true);

    if (success) {
      // 打印作业统计信息
      printJobStatistics(job);
    }

    return success ? 0 : 1;
  }

  private void setupJobConfiguration(Configuration conf) {
    // 设置JSON输出格式
    conf.setBoolean(JsonOutputFormat.JSON_OUTPUT_PRETTY_PRINT, true);
    conf.setBoolean(JsonOutputFormat.JSON_OUTPUT_ARRAY_FORMAT, false);

    // 设置内存配置
    conf.set("mapreduce.map.memory.mb", "2048");
    conf.set("mapreduce.reduce.memory.mb", "4096");
    conf.set("mapreduce.map.java.opts", "-Xmx1536m");
    conf.set("mapreduce.reduce.java.opts", "-Xmx3072m");

    // 设置压缩
    conf.setBoolean("mapreduce.map.output.compress", true);
    conf.setClass("mapreduce.map.output.compress.codec",
                 SnappyCodec.class, CompressionCodec.class);

    // 设置推测执行
    conf.setBoolean("mapreduce.map.speculative", false);
    conf.setBoolean("mapreduce.reduce.speculative", false);

    // 设置自定义比较器
    conf.setClass("mapreduce.job.output.value.groupfn.class",
                 UserInfoRawComparator.class, RawComparator.class);
  }

  private void printJobStatistics(Job job) throws IOException, InterruptedException {
    Counters counters = job.getCounters();

    System.out.println("\n=== Job Statistics ===");

    // 打印用户分析计数器
    CounterGroup userAnalysisGroup = counters.getGroup("UserAnalysis");
    for (Counter counter : userAnalysisGroup) {
      System.out.println(counter.getDisplayName() + ": " + counter.getValue());
    }

    // 打印用户统计计数器
    CounterGroup userStatsGroup = counters.getGroup("UserStatistics");
    for (Counter counter : userStatsGroup) {
      System.out.println(counter.getDisplayName() + ": " + counter.getValue());
    }

    // 打印内置计数器
    System.out.println("\n=== Built-in Counters ===");
    System.out.println("Map input records: " +
                      counters.findCounter(TaskCounter.MAP_INPUT_RECORDS).getValue());
    System.out.println("Map output records: " +
                      counters.findCounter(TaskCounter.MAP_OUTPUT_RECORDS).getValue());
    System.out.println("Reduce input records: " +
                      counters.findCounter(TaskCounter.REDUCE_INPUT_RECORDS).getValue());
    System.out.println("Reduce output records: " +
                      counters.findCounter(TaskCounter.REDUCE_OUTPUT_RECORDS).getValue());

    System.out.println("Job completed successfully!");
  }
}
```

## 17.5 自定义YARN应用

### 17.5.1 自定义ApplicationMaster

开发自定义的YARN应用：

```java
/**
 * 自定义ApplicationMaster
 */
public class CustomApplicationMaster {

  private Configuration conf;
  private YarnClient yarnClient;
  private AMRMClient<ContainerRequest> amrmClient;
  private NMClient nmClient;
  private ApplicationAttemptId appAttemptId;

  private volatile boolean done = false;
  private int numTotalContainers = 1;
  private int numCompletedContainers = 0;
  private int numFailedContainers = 0;

  public static void main(String[] args) throws Exception {
    CustomApplicationMaster appMaster = new CustomApplicationMaster();
    boolean result = appMaster.run();
    System.exit(result ? 0 : 1);
  }

  public boolean run() throws Exception {
    // 初始化
    initialize();

    try {
      // 启动客户端
      startClients();

      // 注册ApplicationMaster
      registerApplicationMaster();

      // 请求容器
      requestContainers();

      // 监控容器
      monitorContainers();

      // 完成应用
      finishApplication();

      return numFailedContainers == 0;

    } finally {
      cleanup();
    }
  }

  private void initialize() throws Exception {
    conf = new YarnConfiguration();

    // 从环境变量获取ApplicationAttemptId
    Map<String, String> envs = System.getenv();
    String containerIdString = envs.get(ApplicationConstants.Environment.CONTAINER_ID.name());
    ContainerId containerId = ContainerId.fromString(containerIdString);
    appAttemptId = containerId.getApplicationAttemptId();

    LOG.info("ApplicationMaster initialized with attempt ID: {}", appAttemptId);
  }

  private void startClients() throws Exception {
    // 启动YARN客户端
    yarnClient = YarnClient.createYarnClient();
    yarnClient.init(conf);
    yarnClient.start();

    // 启动AM-RM客户端
    amrmClient = AMRMClient.createAMRMClient();
    amrmClient.init(conf);
    amrmClient.start();

    // 启动NM客户端
    nmClient = NMClient.createNMClient();
    nmClient.init(conf);
    nmClient.start();

    LOG.info("YARN clients started");
  }

  private void registerApplicationMaster() throws Exception {
    // 注册ApplicationMaster
    RegisterApplicationMasterResponse response = amrmClient.registerApplicationMaster(
        "", // hostname
        0,  // port
        ""  // tracking URL
    );

    LOG.info("ApplicationMaster registered successfully");
    LOG.info("Maximum resource capability: {}", response.getMaximumResourceCapability());
  }

  private void requestContainers() throws Exception {
    // 创建资源请求
    Resource capability = Resource.newInstance(1024, 1); // 1GB内存，1个vCore
    Priority priority = Priority.newInstance(0);

    for (int i = 0; i < numTotalContainers; i++) {
      ContainerRequest containerRequest = new ContainerRequest(
          capability,
          null, // nodes
          null, // racks
          priority
      );

      amrmClient.addContainerRequest(containerRequest);
    }

    LOG.info("Requested {} containers", numTotalContainers);
  }

  private void monitorContainers() throws Exception {
    while (!done && numCompletedContainers < numTotalContainers) {
      try {
        // 分配容器
        AllocateResponse allocateResponse = amrmClient.allocate(0.1f);

        // 处理新分配的容器
        List<Container> allocatedContainers = allocateResponse.getAllocatedContainers();
        for (Container container : allocatedContainers) {
          launchContainer(container);
        }

        // 处理完成的容器
        List<ContainerStatus> completedContainers = allocateResponse.getCompletedContainersStatuses();
        for (ContainerStatus containerStatus : completedContainers) {
          handleCompletedContainer(containerStatus);
        }

        Thread.sleep(1000); // 等待1秒

      } catch (Exception e) {
        LOG.error("Error in container monitoring", e);
        done = true;
      }
    }
  }

  private void launchContainer(Container container) {
    LOG.info("Launching container: {}", container.getId());

    try {
      // 创建容器启动上下文
      ContainerLaunchContext containerContext = createContainerLaunchContext();

      // 启动容器
      nmClient.startContainer(container, containerContext);

      LOG.info("Container {} launched successfully", container.getId());

    } catch (Exception e) {
      LOG.error("Failed to launch container: " + container.getId(), e);
      numFailedContainers++;
    }
  }

  private ContainerLaunchContext createContainerLaunchContext() {
    ContainerLaunchContext containerContext = ContainerLaunchContext.newInstance(
        null, // localResources
        createEnvironment(), // environment
        createCommands(), // commands
        null, // serviceData
        null, // tokens
        null  // applicationACLs
    );

    return containerContext;
  }

  private Map<String, String> createEnvironment() {
    Map<String, String> env = new HashMap<>();

    // 设置Java环境
    env.put("JAVA_HOME", System.getenv("JAVA_HOME"));

    // 设置类路径
    StringBuilder classPath = new StringBuilder();
    classPath.append(ApplicationConstants.Environment.CLASSPATH.$$());
    classPath.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
    classPath.append("./*");

    env.put("CLASSPATH", classPath.toString());

    return env;
  }

  private List<String> createCommands() {
    List<String> commands = new ArrayList<>();

    // 创建Java命令
    StringBuilder command = new StringBuilder();
    command.append("$JAVA_HOME/bin/java");
    command.append(" -Xmx512m");
    command.append(" ").append(CustomContainerTask.class.getName());
    command.append(" 1>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stdout");
    command.append(" 2>").append(ApplicationConstants.LOG_DIR_EXPANSION_VAR).append("/stderr");

    commands.add(command.toString());

    return commands;
  }

  private void handleCompletedContainer(ContainerStatus containerStatus) {
    LOG.info("Container completed: {} with status: {}",
             containerStatus.getContainerId(), containerStatus.getExitStatus());

    numCompletedContainers++;

    if (containerStatus.getExitStatus() != 0) {
      numFailedContainers++;
      LOG.error("Container {} failed with exit status: {}",
               containerStatus.getContainerId(), containerStatus.getExitStatus());
    }
  }

  private void finishApplication() throws Exception {
    FinalApplicationStatus finalStatus = numFailedContainers == 0 ?
        FinalApplicationStatus.SUCCEEDED : FinalApplicationStatus.FAILED;

    String diagnostics = String.format("Completed containers: %d, Failed containers: %d",
                                      numCompletedContainers, numFailedContainers);

    amrmClient.unregisterApplicationMaster(finalStatus, diagnostics, "");

    LOG.info("ApplicationMaster finished with status: {}", finalStatus);
  }

  private void cleanup() {
    if (nmClient != null) {
      nmClient.stop();
    }

    if (amrmClient != null) {
      amrmClient.stop();
    }

    if (yarnClient != null) {
      yarnClient.stop();
    }
  }

  /**
   * 容器任务
   */
  public static class CustomContainerTask {

    public static void main(String[] args) {
      LOG.info("Container task started");

      try {
        // 执行自定义任务
        performTask();

        LOG.info("Container task completed successfully");
        System.exit(0);

      } catch (Exception e) {
        LOG.error("Container task failed", e);
        System.exit(1);
      }
    }

    private static void performTask() throws Exception {
      // 模拟任务执行
      LOG.info("Performing custom task...");

      // 这里可以执行任何自定义逻辑
      // 例如：数据处理、计算任务、服务启动等

      Thread.sleep(10000); // 模拟10秒的工作

      LOG.info("Custom task completed");
    }
  }
}
```

## 17.6 本章小结

理解这些自定义组件开发技术，对于在实际项目中扩展Hadoop功能具有重要意义。随着业务需求的不断变化，自定义组件开发将成为Hadoop应用的重要技能。
