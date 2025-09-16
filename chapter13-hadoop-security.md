# 第13章：Hadoop安全机制与认证授权

## 13.1 引言

随着Hadoop在企业级环境中的广泛应用，安全性成为了一个至关重要的考虑因素。早期的Hadoop设计主要关注功能性和性能，安全机制相对简单，主要依赖于操作系统级别的用户权限控制。然而，在多租户、多用户的企业环境中，这种简单的安全模型已经无法满足复杂的安全需求。

现代Hadoop安全架构是一个多层次、多维度的安全体系，涵盖了身份认证、访问控制、数据加密、审计日志等多个方面。其核心包括基于Kerberos的强身份认证、基于Token的轻量级认证、细粒度的访问控制列表（ACL）、端到端的数据加密，以及完整的安全审计机制。

Hadoop安全机制的设计需要在安全性和性能之间找到平衡点。过于复杂的安全机制可能会影响系统性能，而过于简单的安全机制又无法提供足够的保护。因此，Hadoop采用了分层的安全架构，在不同的层次提供不同级别的安全保护，既保证了系统的安全性，又尽可能地减少了对性能的影响。

本章将深入分析Hadoop的安全架构设计、Kerberos认证机制、Token认证体系、访问控制机制、数据加密技术以及安全审计功能，帮助读者全面理解Hadoop的安全实现原理和最佳实践。

## 13.2 Hadoop安全架构概述

### 13.2.1 安全威胁模型

Hadoop面临的主要安全威胁包括：

```java
public enum SecurityThreat {
  // 身份伪造威胁
  IDENTITY_SPOOFING("恶意用户伪造合法用户身份"),
  
  // 未授权访问威胁  
  UNAUTHORIZED_ACCESS("未经授权访问敏感数据或资源"),
  
  // 数据窃取威胁
  DATA_THEFT("通过网络窃听或存储介质获取敏感数据"),
  
  // 权限提升威胁
  PRIVILEGE_ESCALATION("恶意用户获取超出其权限范围的访问权限"),
  
  // 拒绝服务威胁
  DENIAL_OF_SERVICE("通过资源耗尽等方式影响系统正常服务"),
  
  // 数据篡改威胁
  DATA_TAMPERING("恶意修改或删除重要数据"),
  
  // 内部威胁
  INSIDER_THREAT("来自内部用户的恶意行为");
  
  private final String description;
  
  SecurityThreat(String description) {
    this.description = description;
  }
  
  public String getDescription() {
    return description;
  }
}

/**
 * Hadoop安全架构的核心组件
 */
public class HadoopSecurityArchitecture {
  
  // 认证层：验证用户身份
  private final AuthenticationLayer authenticationLayer;
  
  // 授权层：控制资源访问
  private final AuthorizationLayer authorizationLayer;
  
  // 加密层：保护数据传输和存储
  private final EncryptionLayer encryptionLayer;
  
  // 审计层：记录安全相关事件
  private final AuditLayer auditLayer;
  
  public HadoopSecurityArchitecture() {
    this.authenticationLayer = new AuthenticationLayer();
    this.authorizationLayer = new AuthorizationLayer();
    this.encryptionLayer = new EncryptionLayer();
    this.auditLayer = new AuditLayer();
  }
  
  /**
   * 处理用户请求的安全检查流程
   */
  public SecurityContext processRequest(UserRequest request) throws SecurityException {
    // 1. 身份认证
    AuthenticationResult authResult = authenticationLayer.authenticate(request);
    if (!authResult.isAuthenticated()) {
      auditLayer.logAuthenticationFailure(request);
      throw new SecurityException("Authentication failed");
    }
    
    // 2. 授权检查
    AuthorizationResult authzResult = authorizationLayer.authorize(
        authResult.getUser(), request.getResource(), request.getAction());
    if (!authzResult.isAuthorized()) {
      auditLayer.logAuthorizationFailure(authResult.getUser(), request);
      throw new SecurityException("Authorization failed");
    }
    
    // 3. 创建安全上下文
    SecurityContext context = new SecurityContext(
        authResult.getUser(),
        authzResult.getPermissions(),
        encryptionLayer.getEncryptionContext(request)
    );
    
    // 4. 记录成功访问
    auditLayer.logSuccessfulAccess(context, request);
    
    return context;
  }
}
```

### 13.2.2 安全组件架构

Hadoop安全架构由多个相互协作的组件构成：

```java
/**
 * 认证层实现
 */
public class AuthenticationLayer {
  
  private final KerberosAuthenticator kerberosAuth;
  private final TokenAuthenticator tokenAuth;
  private final SimpleAuthenticator simpleAuth;
  
  public AuthenticationResult authenticate(UserRequest request) {
    SecurityMode securityMode = getSecurityMode();
    
    switch (securityMode) {
      case KERBEROS:
        return kerberosAuth.authenticate(request);
      case TOKEN:
        return tokenAuth.authenticate(request);
      case SIMPLE:
        return simpleAuth.authenticate(request);
      default:
        throw new IllegalStateException("Unknown security mode: " + securityMode);
    }
  }
  
  private SecurityMode getSecurityMode() {
    String authMode = Configuration.get("hadoop.security.authentication", "simple");
    return SecurityMode.valueOf(authMode.toUpperCase());
  }
}

/**
 * 授权层实现
 */
public class AuthorizationLayer {
  
  private final AccessControlManager aclManager;
  private final PermissionChecker permissionChecker;
  private final PolicyEngine policyEngine;
  
  public AuthorizationResult authorize(User user, Resource resource, Action action) {
    // 检查ACL权限
    if (!aclManager.checkAccess(user, resource, action)) {
      return AuthorizationResult.denied("ACL check failed");
    }
    
    // 检查文件系统权限
    if (!permissionChecker.checkPermission(user, resource, action)) {
      return AuthorizationResult.denied("Permission check failed");
    }
    
    // 检查策略引擎
    PolicyDecision decision = policyEngine.evaluate(user, resource, action);
    if (decision.isDenied()) {
      return AuthorizationResult.denied("Policy evaluation failed: " + decision.getReason());
    }
    
    return AuthorizationResult.allowed(decision.getPermissions());
  }
}

/**
 * 加密层实现
 */
public class EncryptionLayer {
  
  private final KeyManager keyManager;
  private final CryptoCodecPool codecPool;
  private final EncryptionZoneManager ezManager;
  
  public EncryptionContext getEncryptionContext(UserRequest request) {
    Resource resource = request.getResource();
    
    // 检查是否在加密区域内
    if (ezManager.isInEncryptionZone(resource.getPath())) {
      EncryptionZone ez = ezManager.getEncryptionZone(resource.getPath());
      
      // 获取加密密钥
      EncryptionKey key = keyManager.getKey(ez.getKeyName());
      
      // 获取加密编解码器
      CryptoCodec codec = codecPool.getCryptoCodec(ez.getCipherSuite());
      
      return new EncryptionContext(key, codec, ez);
    }
    
    return EncryptionContext.NONE;
  }
}

/**
 * 审计层实现
 */
public class AuditLayer {
  
  private final AuditLogger auditLogger;
  private final EventProcessor eventProcessor;
  
  public void logAuthenticationFailure(UserRequest request) {
    AuditEvent event = new AuditEvent(
        AuditEventType.AUTHENTICATION_FAILURE,
        request.getUser(),
        request.getClientAddress(),
        System.currentTimeMillis(),
        "Authentication failed for user: " + request.getUser()
    );
    
    auditLogger.log(event);
    eventProcessor.process(event);
  }
  
  public void logAuthorizationFailure(User user, UserRequest request) {
    AuditEvent event = new AuditEvent(
        AuditEventType.AUTHORIZATION_FAILURE,
        user,
        request.getClientAddress(),
        System.currentTimeMillis(),
        "Authorization failed for user: " + user + 
        ", resource: " + request.getResource() +
        ", action: " + request.getAction()
    );
    
    auditLogger.log(event);
    eventProcessor.process(event);
  }
  
  public void logSuccessfulAccess(SecurityContext context, UserRequest request) {
    AuditEvent event = new AuditEvent(
        AuditEventType.SUCCESSFUL_ACCESS,
        context.getUser(),
        request.getClientAddress(),
        System.currentTimeMillis(),
        "Successful access for user: " + context.getUser() +
        ", resource: " + request.getResource() +
        ", action: " + request.getAction()
    );
    
    auditLogger.log(event);
    eventProcessor.process(event);
  }
}
```

**安全架构特点**：
- **分层设计**：认证、授权、加密、审计各层独立
- **可插拔组件**：支持不同的认证和授权机制
- **统一接口**：提供一致的安全API
- **事件驱动**：基于事件的安全监控和响应

### 13.2.3 安全配置管理

Hadoop安全配置的统一管理：

```java
public class SecurityConfiguration {
  
  // 安全模式配置
  public static final String HADOOP_SECURITY_AUTHENTICATION = 
      "hadoop.security.authentication";
  public static final String HADOOP_SECURITY_AUTHORIZATION = 
      "hadoop.security.authorization";
  
  // Kerberos配置
  public static final String HADOOP_SECURITY_KERBEROS_TICKET_CACHE_PATH = 
      "hadoop.security.kerberos.ticket.cache.path";
  public static final String HADOOP_SECURITY_KERBEROS_KEYTAB_FILE = 
      "hadoop.security.kerberos.keytab.file";
  
  // Token配置
  public static final String HADOOP_TOKEN_SERVICE_USE_IP = 
      "hadoop.token.service.use_ip";
  public static final String HADOOP_SECURITY_TOKEN_SERVICE_CLASS = 
      "hadoop.security.token.service.class";
  
  // 加密配置
  public static final String HADOOP_RPC_PROTECTION = 
      "hadoop.rpc.protection";
  public static final String DFS_ENCRYPT_DATA_TRANSFER = 
      "dfs.encrypt.data.transfer";
  public static final String DFS_ENCRYPTION_KEY_PROVIDER_URI = 
      "dfs.encryption.key.provider.uri";
  
  private final Configuration conf;
  
  public SecurityConfiguration(Configuration conf) {
    this.conf = conf;
    validateConfiguration();
  }
  
  public boolean isSecurityEnabled() {
    return "kerberos".equalsIgnoreCase(
        conf.get(HADOOP_SECURITY_AUTHENTICATION, "simple"));
  }
  
  public boolean isAuthorizationEnabled() {
    return conf.getBoolean(HADOOP_SECURITY_AUTHORIZATION, false);
  }
  
  public RpcProtection getRpcProtection() {
    String protection = conf.get(HADOOP_RPC_PROTECTION, "authentication");
    return RpcProtection.valueOf(protection.toUpperCase());
  }
  
  public boolean isDataTransferEncryptionEnabled() {
    return conf.getBoolean(DFS_ENCRYPT_DATA_TRANSFER, false);
  }
  
  private void validateConfiguration() {
    if (isSecurityEnabled()) {
      // 验证Kerberos配置
      validateKerberosConfiguration();
    }
    
    if (isAuthorizationEnabled()) {
      // 验证授权配置
      validateAuthorizationConfiguration();
    }
    
    if (isDataTransferEncryptionEnabled()) {
      // 验证加密配置
      validateEncryptionConfiguration();
    }
  }
  
  private void validateKerberosConfiguration() {
    String keytabFile = conf.get(HADOOP_SECURITY_KERBEROS_KEYTAB_FILE);
    if (keytabFile == null || keytabFile.isEmpty()) {
      throw new IllegalArgumentException("Kerberos keytab file must be specified");
    }
    
    File keytab = new File(keytabFile);
    if (!keytab.exists() || !keytab.canRead()) {
      throw new IllegalArgumentException("Kerberos keytab file is not accessible: " + keytabFile);
    }
  }
  
  private void validateAuthorizationConfiguration() {
    // 验证授权相关配置
    if (!isSecurityEnabled()) {
      LOG.warn("Authorization is enabled but authentication is not secure");
    }
  }
  
  private void validateEncryptionConfiguration() {
    String keyProviderUri = conf.get(DFS_ENCRYPTION_KEY_PROVIDER_URI);
    if (keyProviderUri == null || keyProviderUri.isEmpty()) {
      throw new IllegalArgumentException("Encryption key provider URI must be specified");
    }
  }
}
```

## 13.3 Kerberos认证机制

### 13.3.1 Kerberos协议原理

Kerberos是Hadoop安全认证的核心协议：

```java
/**
 * Kerberos认证实现
 */
public class KerberosAuthenticator implements Authenticator {
  
  private final String realm;
  private final String kdcHost;
  private final int kdcPort;
  private final LoginContext loginContext;
  
  public KerberosAuthenticator(Configuration conf) throws LoginException {
    this.realm = conf.get("hadoop.security.kerberos.realm");
    this.kdcHost = conf.get("hadoop.security.kerberos.kdc.host");
    this.kdcPort = conf.getInt("hadoop.security.kerberos.kdc.port", 88);
    
    // 初始化登录上下文
    this.loginContext = createLoginContext(conf);
  }
  
  @Override
  public AuthenticationResult authenticate(UserRequest request) {
    try {
      // 执行Kerberos登录
      loginContext.login();
      
      Subject subject = loginContext.getSubject();
      
      // 获取用户主体
      Set<Principal> principals = subject.getPrincipals(KerberosPrincipal.class);
      if (principals.isEmpty()) {
        return AuthenticationResult.failed("No Kerberos principal found");
      }
      
      KerberosPrincipal principal = (KerberosPrincipal) principals.iterator().next();
      
      // 验证票据有效性
      if (!isTicketValid(subject)) {
        return AuthenticationResult.failed("Kerberos ticket is invalid or expired");
      }
      
      // 创建用户对象
      User user = new KerberosUser(principal.getName(), subject);
      
      return AuthenticationResult.success(user);
      
    } catch (LoginException e) {
      LOG.error("Kerberos authentication failed", e);
      return AuthenticationResult.failed("Kerberos authentication failed: " + e.getMessage());
    }
  }
  
  private LoginContext createLoginContext(Configuration conf) throws LoginException {
    String keytabFile = conf.get("hadoop.security.kerberos.keytab.file");
    String principal = conf.get("hadoop.security.kerberos.principal");
    
    if (keytabFile != null && principal != null) {
      // 使用keytab文件登录
      return createKeytabLoginContext(keytabFile, principal);
    } else {
      // 使用票据缓存登录
      return createTicketCacheLoginContext();
    }
  }
  
  private LoginContext createKeytabLoginContext(String keytabFile, String principal) 
      throws LoginException {
    
    Map<String, String> options = new HashMap<>();
    options.put("useKeyTab", "true");
    options.put("keyTab", keytabFile);
    options.put("principal", principal);
    options.put("useTicketCache", "false");
    options.put("doNotPrompt", "true");
    options.put("storeKey", "true");
    
    AppConfigurationEntry entry = new AppConfigurationEntry(
        "com.sun.security.auth.module.Krb5LoginModule",
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        options
    );
    
    Configuration loginConf = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] { entry };
      }
    };
    
    return new LoginContext("hadoop-keytab", null, null, loginConf);
  }
  
  private LoginContext createTicketCacheLoginContext() throws LoginException {
    Map<String, String> options = new HashMap<>();
    options.put("useTicketCache", "true");
    options.put("doNotPrompt", "true");
    options.put("useKeyTab", "false");
    
    AppConfigurationEntry entry = new AppConfigurationEntry(
        "com.sun.security.auth.module.Krb5LoginModule",
        AppConfigurationEntry.LoginModuleControlFlag.REQUIRED,
        options
    );
    
    Configuration loginConf = new Configuration() {
      @Override
      public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        return new AppConfigurationEntry[] { entry };
      }
    };
    
    return new LoginContext("hadoop-ticket-cache", null, null, loginConf);
  }
  
  private boolean isTicketValid(Subject subject) {
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    
    for (KerberosTicket ticket : tickets) {
      if (ticket.isCurrent() && !ticket.isDestroyed()) {
        // 检查票据是否即将过期
        Date endTime = ticket.getEndTime();
        long timeToExpiry = endTime.getTime() - System.currentTimeMillis();
        
        // 如果票据在5分钟内过期，认为无效
        if (timeToExpiry > 5 * 60 * 1000) {
          return true;
        }
      }
    }
    
    return false;
  }
  
  /**
   * 刷新Kerberos票据
   */
  public void refreshTicket() throws LoginException {
    Subject subject = loginContext.getSubject();
    Set<KerberosTicket> tickets = subject.getPrivateCredentials(KerberosTicket.class);
    
    for (KerberosTicket ticket : tickets) {
      if (ticket.isRenewable()) {
        try {
          ticket.refresh();
          LOG.info("Successfully refreshed Kerberos ticket for " + ticket.getClient());
        } catch (RefreshFailedException e) {
          LOG.warn("Failed to refresh Kerberos ticket", e);
          // 重新登录
          loginContext.logout();
          loginContext.login();
        }
      }
    }
  }
}

/**
 * Kerberos用户实现
 */
public class KerberosUser implements User {
  
  private final String name;
  private final Subject subject;
  private final String shortName;
  private final String realm;
  
  public KerberosUser(String principalName, Subject subject) {
    this.name = principalName;
    this.subject = subject;
    
    // 解析主体名称
    String[] parts = principalName.split("@");
    if (parts.length == 2) {
      this.shortName = parts[0];
      this.realm = parts[1];
    } else {
      this.shortName = principalName;
      this.realm = null;
    }
  }
  
  @Override
  public String getName() {
    return name;
  }
  
  @Override
  public String getShortName() {
    return shortName;
  }
  
  public String getRealm() {
    return realm;
  }
  
  public Subject getSubject() {
    return subject;
  }
  
  @Override
  public String[] getGroupNames() {
    // 从Subject中获取组信息
    Set<Group> groups = subject.getPrincipals(Group.class);
    return groups.stream()
                 .map(Group::getName)
                 .toArray(String[]::new);
  }
  
  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    
    KerberosUser that = (KerberosUser) obj;
    return Objects.equals(name, that.name);
  }
  
  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
  
  @Override
  public String toString() {
    return "KerberosUser{" +
           "name='" + name + '\'' +
           ", realm='" + realm + '\'' +
           '}';
  }
}
```

**Kerberos认证流程**：
1. **初始认证**：用户向KDC请求TGT（票据授权票据）
2. **服务认证**：使用TGT向KDC请求服务票据
3. **服务访问**：使用服务票据访问Hadoop服务
4. **票据刷新**：定期刷新票据保持认证状态

### 13.3.2 SASL安全层

SASL（Simple Authentication and Security Layer）为Hadoop提供了灵活的认证框架：

```java
/**
 * SASL认证处理器
 */
public class SaslRpcServer {
  
  public static final String SASL_PROPS_RESOLVER_CLASS = 
      "hadoop.security.saslproperties.resolver.class";
  
  private static SaslPropertiesResolver saslPropsResolver;
  private static Map<String, ?> saslProps;
  
  static {
    initializeSaslProperties();
  }
  
  private static void initializeSaslProperties() {
    Class<? extends SaslPropertiesResolver> resolverClass = 
        conf.getClass(SASL_PROPS_RESOLVER_CLASS, 
                     SaslPropertiesResolver.class, 
                     SaslPropertiesResolver.class);
    
    saslPropsResolver = ReflectionUtils.newInstance(resolverClass, conf);
    saslProps = saslPropsResolver.getDefaultProperties();
  }
  
  /**
   * 创建SASL服务器
   */
  public static SaslServer createSaslServer(String mechanism, String protocol, 
                                           String serverName, Map<String, ?> props, 
                                           CallbackHandler cbh) throws SaslException {
    
    return Sasl.createSaslServer(mechanism, protocol, serverName, props, cbh);
  }
  
  /**
   * 获取SASL属性
   */
  public static Map<String, ?> getSaslProperties(InetAddress addr) {
    if (saslPropsResolver != null) {
      return saslPropsResolver.getServerProperties(addr);
    }
    return saslProps;
  }
  
  /**
   * SASL回调处理器
   */
  public static class SaslServerCallbackHandler implements CallbackHandler {
    
    private final String userName;
    private final char[] userPassword;
    
    public SaslServerCallbackHandler(Configuration conf) throws IOException {
      this.userName = getServerUser(conf);
      this.userPassword = getServerPassword(conf);
    }
    
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      NameCallback nc = null;
      PasswordCallback pc = null;
      AuthorizeCallback ac = null;
      
      for (Callback callback : callbacks) {
        if (callback instanceof AuthorizeCallback) {
          ac = (AuthorizeCallback) callback;
        } else if (callback instanceof NameCallback) {
          nc = (NameCallback) callback;
        } else if (callback instanceof PasswordCallback) {
          pc = (PasswordCallback) callback;
        } else if (callback instanceof RealmCallback) {
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else {
          throw new UnsupportedCallbackException(callback, "Unrecognized SASL callback");
        }
      }
      
      if (nc != null) {
        nc.setName(userName);
      }
      
      if (pc != null) {
        pc.setPassword(userPassword);
      }
      
      if (ac != null) {
        String authenticationID = ac.getAuthenticationID();
        String authorizationID = ac.getAuthorizationID();
        
        if (authenticationID.equals(authorizationID)) {
          ac.setAuthorized(true);
        } else {
          ac.setAuthorized(false);
        }
        
        if (ac.isAuthorized()) {
          ac.setAuthorizedID(authorizationID);
        }
      }
    }
    
    private String getServerUser(Configuration conf) throws IOException {
      String keytabFile = conf.get("hadoop.security.kerberos.keytab.file");
      String principal = conf.get("hadoop.security.kerberos.principal");
      
      if (keytabFile != null && principal != null) {
        return SecurityUtil.getServerPrincipal(principal, "0.0.0.0");
      }
      
      return UserGroupInformation.getCurrentUser().getUserName();
    }
    
    private char[] getServerPassword(Configuration conf) {
      // 在实际实现中，密码通常从安全存储中获取
      return new char[0];
    }
  }
}

/**
 * SASL客户端实现
 */
public class SaslRpcClient {
  
  private final UserGroupInformation ugi;
  private final String serverPrincipal;
  private final InetSocketAddress serverAddr;
  
  public SaslRpcClient(UserGroupInformation ugi, String serverPrincipal, 
                       InetSocketAddress serverAddr) {
    this.ugi = ugi;
    this.serverPrincipal = serverPrincipal;
    this.serverAddr = serverAddr;
  }
  
  /**
   * 创建SASL客户端连接
   */
  public SaslClient createSaslClient() throws IOException {
    Map<String, String> props = new HashMap<>();
    props.put(Sasl.QOP, getQualityOfProtection());
    props.put(Sasl.SERVER_AUTH, "true");
    
    String[] mechanisms = {"GSSAPI"}; // Kerberos
    
    try {
      return Sasl.createSaslClient(mechanisms, null, "hadoop", 
                                  serverPrincipal, props, 
                                  new SaslClientCallbackHandler());
    } catch (SaslException e) {
      throw new IOException("Failed to create SASL client", e);
    }
  }
  
  private String getQualityOfProtection() {
    // 根据配置返回QOP级别：auth, auth-int, auth-conf
    return "auth";
  }
  
  /**
   * SASL客户端回调处理器
   */
  private class SaslClientCallbackHandler implements CallbackHandler {
    
    @Override
    public void handle(Callback[] callbacks) throws IOException, UnsupportedCallbackException {
      for (Callback callback : callbacks) {
        if (callback instanceof NameCallback) {
          NameCallback nc = (NameCallback) callback;
          nc.setName(ugi.getUserName());
        } else if (callback instanceof PasswordCallback) {
          PasswordCallback pc = (PasswordCallback) callback;
          // Kerberos不需要密码回调
          pc.setPassword(new char[0]);
        } else if (callback instanceof RealmCallback) {
          RealmCallback rc = (RealmCallback) callback;
          rc.setText(rc.getDefaultText());
        } else {
          throw new UnsupportedCallbackException(callback, "Unsupported callback");
        }
      }
    }
  }
}
```

**SASL特性**：
- **机制无关**：支持多种认证机制
- **安全层**：提供完整性和机密性保护
- **可扩展**：支持自定义认证机制
- **标准化**：基于RFC标准实现

## 13.4 Token认证体系

### 13.4.1 Token机制设计

Token认证为Hadoop提供了轻量级的认证方案：

```java
/**
 * Token认证的核心接口
 */
public abstract class Token<T extends TokenIdentifier> implements Writable {
  
  private byte[] identifier;
  private byte[] password;
  private Text kind;
  private Text service;
  
  public Token() {
    this.identifier = new byte[0];
    this.password = new byte[0];
    this.kind = new Text();
    this.service = new Text();
  }
  
  public Token(byte[] identifier, byte[] password, Text kind, Text service) {
    this.identifier = identifier != null ? identifier.clone() : new byte[0];
    this.password = password != null ? password.clone() : new byte[0];
    this.kind = kind;
    this.service = service;
  }
  
  /**
   * 获取Token标识符
   */
  public T decodeIdentifier() throws IOException {
    Class<T> identifierClass = getIdentifierClass();
    T tokenIdentifier = ReflectionUtils.newInstance(identifierClass, null);
    
    DataInputStream in = new DataInputStream(new ByteArrayInputStream(identifier));
    tokenIdentifier.readFields(in);
    in.close();
    
    return tokenIdentifier;
  }
  
  /**
   * 验证Token有效性
   */
  public boolean isValid() {
    try {
      T tokenId = decodeIdentifier();
      return tokenId.getExpiryDate() > System.currentTimeMillis();
    } catch (IOException e) {
      return false;
    }
  }
  
  /**
   * 获取Token标识符类
   */
  @SuppressWarnings("unchecked")
  private Class<T> getIdentifierClass() {
    ParameterizedType pt = (ParameterizedType) getClass().getGenericSuperclass();
    return (Class<T>) pt.getActualTypeArguments()[0];
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    out.writeInt(identifier.length);
    out.write(identifier);
    out.writeInt(password.length);
    out.write(password);
    kind.write(out);
    service.write(out);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    int len = in.readInt();
    identifier = new byte[len];
    in.readFully(identifier);
    
    len = in.readInt();
    password = new byte[len];
    in.readFully(password);
    
    kind.readFields(in);
    service.readFields(in);
  }
  
  // Getters and setters
  public byte[] getIdentifier() { return identifier.clone(); }
  public byte[] getPassword() { return password.clone(); }
  public Text getKind() { return kind; }
  public Text getService() { return service; }
}

/**
 * Token标识符基类
 */
public abstract class TokenIdentifier implements Writable {
  
  private Text owner;
  private Text renewer;
  private Text realUser;
  private long issueDate;
  private long maxDate;
  private int sequenceNumber;
  
  public TokenIdentifier() {
    this.owner = new Text();
    this.renewer = new Text();
    this.realUser = new Text();
  }
  
  public TokenIdentifier(Text owner, Text renewer, Text realUser) {
    this.owner = owner;
    this.renewer = renewer;
    this.realUser = realUser;
    this.issueDate = System.currentTimeMillis();
    this.maxDate = issueDate + getMaxLifetime();
  }
  
  /**
   * 获取Token类型
   */
  public abstract Text getKind();
  
  /**
   * 获取最大生命周期
   */
  protected abstract long getMaxLifetime();
  
  /**
   * 获取过期时间
   */
  public long getExpiryDate() {
    return maxDate;
  }
  
  /**
   * 检查是否可以续期
   */
  public boolean isRenewable() {
    return renewer != null && !renewer.toString().isEmpty();
  }
  
  /**
   * 续期Token
   */
  public long renew(long renewTime) throws IOException {
    if (!isRenewable()) {
      throw new IOException("Token is not renewable");
    }
    
    if (renewTime > maxDate) {
      throw new IOException("Token renewal time exceeds max date");
    }
    
    this.maxDate = renewTime;
    return maxDate;
  }
  
  @Override
  public void write(DataOutput out) throws IOException {
    owner.write(out);
    renewer.write(out);
    realUser.write(out);
    out.writeLong(issueDate);
    out.writeLong(maxDate);
    out.writeInt(sequenceNumber);
  }
  
  @Override
  public void readFields(DataInput in) throws IOException {
    owner.readFields(in);
    renewer.readFields(in);
    realUser.readFields(in);
    issueDate = in.readLong();
    maxDate = in.readLong();
    sequenceNumber = in.readInt();
  }
  
  // Getters and setters
  public Text getOwner() { return owner; }
  public Text getRenewer() { return renewer; }
  public Text getRealUser() { return realUser; }
  public long getIssueDate() { return issueDate; }
  public int getSequenceNumber() { return sequenceNumber; }
  
  public void setOwner(Text owner) { this.owner = owner; }
  public void setRenewer(Text renewer) { this.renewer = renewer; }
  public void setRealUser(Text realUser) { this.realUser = realUser; }
  public void setSequenceNumber(int sequenceNumber) { this.sequenceNumber = sequenceNumber; }
}
```

### 13.4.2 Delegation Token实现

Delegation Token是Hadoop中最重要的Token类型：

```java
/**
 * Delegation Token标识符
 */
public class DelegationTokenIdentifier extends TokenIdentifier {
  
  public static final Text HDFS_DELEGATION_KIND = new Text("HDFS_DELEGATION_TOKEN");
  
  private static final long DEFAULT_MAX_LIFETIME = 7 * 24 * 60 * 60 * 1000L; // 7天
  
  public DelegationTokenIdentifier() {
    super();
  }
  
  public DelegationTokenIdentifier(Text owner, Text renewer, Text realUser) {
    super(owner, renewer, realUser);
  }
  
  @Override
  public Text getKind() {
    return HDFS_DELEGATION_KIND;
  }
  
  @Override
  protected long getMaxLifetime() {
    return DEFAULT_MAX_LIFETIME;
  }
  
  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("owner=").append(getOwner())
      .append(", renewer=").append(getRenewer())
      .append(", realUser=").append(getRealUser())
      .append(", issueDate=").append(new Date(getIssueDate()))
      .append(", maxDate=").append(new Date(getExpiryDate()))
      .append(", sequenceNumber=").append(getSequenceNumber());
    return sb.toString();
  }
}

/**
 * Delegation Token管理器
 */
public class DelegationTokenManager {
  
  private final Map<DelegationTokenIdentifier, DelegationTokenInformation> currentTokens;
  private final Map<Text, DelegationKey> allKeys;
  private final Timer tokenRemoverTimer;
  private final long tokenMaxLifetime;
  private final long tokenRenewInterval;
  private final SecretManager secretManager;
  
  public DelegationTokenManager(Configuration conf) {
    this.currentTokens = new ConcurrentHashMap<>();
    this.allKeys = new ConcurrentHashMap<>();
    this.tokenMaxLifetime = conf.getLong("hadoop.security.delegation.token.max-lifetime", 
                                        7 * 24 * 60 * 60 * 1000L);
    this.tokenRenewInterval = conf.getLong("hadoop.security.delegation.token.renew-interval", 
                                          24 * 60 * 60 * 1000L);
    this.secretManager = new DelegationTokenSecretManager(conf);
    
    // 启动Token清理定时器
    this.tokenRemoverTimer = new Timer("DelegationTokenRemover", true);
    this.tokenRemoverTimer.schedule(new TokenRemover(), tokenRenewInterval, tokenRenewInterval);
  }
  
  /**
   * 创建Delegation Token
   */
  public Token<DelegationTokenIdentifier> createToken(UserGroupInformation ugi, 
                                                     Text renewer) throws IOException {
    Text owner = new Text(ugi.getUserName());
    Text realUser = null;
    if (ugi.getRealUser() != null) {
      realUser = new Text(ugi.getRealUser().getUserName());
    }
    
    DelegationTokenIdentifier identifier = new DelegationTokenIdentifier(owner, renewer, realUser);
    identifier.setSequenceNumber(getNextSequenceNumber());
    
    long now = System.currentTimeMillis();
    identifier.setIssueDate(now);
    identifier.setMaxDate(now + tokenMaxLifetime);
    
    byte[] password = secretManager.createPassword(identifier);
    
    Token<DelegationTokenIdentifier> token = new Token<>(
        identifier.getBytes(), password, identifier.getKind(), new Text(""));
    
    // 存储Token信息
    DelegationTokenInformation tokenInfo = new DelegationTokenInformation(
        now + tokenRenewInterval, password);
    currentTokens.put(identifier, tokenInfo);
    
    LOG.info("Created delegation token for " + owner + ", renewer=" + renewer);
    return token;
  }
  
  /**
   * 续期Delegation Token
   */
  public long renewToken(Token<DelegationTokenIdentifier> token, 
                        String renewer) throws IOException {
    DelegationTokenIdentifier id = token.decodeIdentifier();
    
    if (!id.getRenewer().toString().equals(renewer)) {
      throw new AccessControlException("Renewer " + renewer + 
                                     " does not match token renewer " + id.getRenewer());
    }
    
    DelegationTokenInformation info = currentTokens.get(id);
    if (info == null) {
      throw new InvalidToken("Token not found");
    }
    
    if (id.getMaxDate() < System.currentTimeMillis()) {
      throw new InvalidToken("Token has expired");
    }
    
    long renewTime = Math.min(id.getMaxDate(), System.currentTimeMillis() + tokenRenewInterval);
    info.setRenewDate(renewTime);
    
    LOG.info("Renewed delegation token for " + id.getOwner() + 
             ", new expiry: " + new Date(renewTime));
    
    return renewTime;
  }
  
  /**
   * 取消Delegation Token
   */
  public void cancelToken(Token<DelegationTokenIdentifier> token, 
                         String canceller) throws IOException {
    DelegationTokenIdentifier id = token.decodeIdentifier();
    
    if (!id.getOwner().toString().equals(canceller) && 
        !id.getRenewer().toString().equals(canceller)) {
      throw new AccessControlException("Canceller " + canceller + 
                                     " is not authorized to cancel this token");
    }
    
    DelegationTokenInformation info = currentTokens.remove(id);
    if (info == null) {
      throw new InvalidToken("Token not found");
    }
    
    LOG.info("Cancelled delegation token for " + id.getOwner());
  }
  
  /**
   * 验证Token
   */
  public void verifyToken(DelegationTokenIdentifier identifier, 
                         byte[] password) throws InvalidToken {
    DelegationTokenInformation info = currentTokens.get(identifier);
    if (info == null) {
      throw new InvalidToken("Token not found");
    }
    
    if (info.getRenewDate() < System.currentTimeMillis()) {
      throw new InvalidToken("Token has expired");
    }
    
    if (!Arrays.equals(info.getPassword(), password)) {
      throw new InvalidToken("Invalid token password");
    }
  }
  
  /**
   * Token清理任务
   */
  private class TokenRemover extends TimerTask {
    @Override
    public void run() {
      long now = System.currentTimeMillis();
      Iterator<Map.Entry<DelegationTokenIdentifier, DelegationTokenInformation>> iter = 
          currentTokens.entrySet().iterator();
      
      while (iter.hasNext()) {
        Map.Entry<DelegationTokenIdentifier, DelegationTokenInformation> entry = iter.next();
        DelegationTokenIdentifier id = entry.getKey();
        DelegationTokenInformation info = entry.getValue();
        
        if (info.getRenewDate() < now) {
          iter.remove();
          LOG.info("Removed expired delegation token for " + id.getOwner());
        }
      }
    }
  }
  
  /**
   * Token信息存储
   */
  private static class DelegationTokenInformation {
    private long renewDate;
    private byte[] password;
    
    public DelegationTokenInformation(long renewDate, byte[] password) {
      this.renewDate = renewDate;
      this.password = password.clone();
    }
    
    public long getRenewDate() { return renewDate; }
    public void setRenewDate(long renewDate) { this.renewDate = renewDate; }
    public byte[] getPassword() { return password.clone(); }
  }
  
  private int sequenceNumber = 0;
  
  private synchronized int getNextSequenceNumber() {
    return ++sequenceNumber;
  }
}
```

**Token认证优势**：
- **性能优化**：避免频繁的Kerberos认证
- **网络友好**：减少与KDC的交互
- **可控性**：支持Token的创建、续期、取消
- **安全性**：基于密码学的安全机制

## 13.5 访问控制机制

### 13.5.1 HDFS权限模型

HDFS实现了类似POSIX的权限模型：

```java
/**
 * HDFS权限检查器
 */
public class FSPermissionChecker {

  private final String user;
  private final Set<String> groups;
  private final boolean isSuper;

  public FSPermissionChecker(String user, String[] groups, String supergroup) {
    this.user = user;
    this.groups = Sets.newHashSet(groups);
    this.isSuper = this.groups.contains(supergroup);
  }

  /**
   * 检查文件/目录访问权限
   */
  public void checkPermission(String path, INodeAttributes inode,
                             boolean doCheckOwner, FsAction ancestorAccess,
                             FsAction parentAccess, FsAction access,
                             boolean ignoreEmptyDir) throws AccessControlException {

    if (LOG.isDebugEnabled()) {
      LOG.debug("Permission check for user=" + user + ", path=" + path +
               ", access=" + access);
    }

    // 超级用户拥有所有权限
    if (isSuper) {
      return;
    }

    // 检查所有者权限
    if (doCheckOwner && !user.equals(inode.getUserName())) {
      throw new AccessControlException("Permission denied: user=" + user +
                                     ", access=" + access + ", inode=\"" + path +
                                     "\":" + inode.getUserName() + ":" +
                                     inode.getGroupName() + ":" +
                                     (inode.isDirectory() ? "d" : "-") +
                                     inode.getFsPermission());
    }

    // 检查具体权限
    if (access != null) {
      checkAccessAcl(inode, path, access);
    }

    // 检查父目录权限
    if (parentAccess != null && !hasPermission(inode, parentAccess)) {
      throw new AccessControlException("Permission denied: user=" + user +
                                     ", access=" + parentAccess + ", inode=\"" + path +
                                     "\":" + inode.getUserName() + ":" +
                                     inode.getGroupName() + ":" +
                                     (inode.isDirectory() ? "d" : "-") +
                                     inode.getFsPermission());
    }
  }

  private void checkAccessAcl(INodeAttributes inode, String path, FsAction access)
      throws AccessControlException {

    FsPermission mode = inode.getFsPermission();
    AclFeature aclFeature = inode.getAclFeature();

    if (aclFeature != null) {
      // 使用ACL进行权限检查
      List<AclEntry> featureEntries = aclFeature.getEntries();
      List<AclEntry> aclSpec = Lists.newArrayListWithCapacity(featureEntries.size());

      for (int i = 0; i < featureEntries.size(); i++) {
        aclSpec.add(AclEntryStatusFormat.toAclEntry(featureEntries.get(i)));
      }

      if (!hasAclPermission(inode, aclSpec, access)) {
        throw new AccessControlException("Permission denied: user=" + user +
                                       ", access=" + access + ", inode=\"" + path +
                                       "\":" + inode.getUserName() + ":" +
                                       inode.getGroupName() + ":" +
                                       (inode.isDirectory() ? "d" : "-") + mode);
      }
    } else {
      // 使用传统权限模式
      if (!hasPermission(inode, access)) {
        throw new AccessControlException("Permission denied: user=" + user +
                                       ", access=" + access + ", inode=\"" + path +
                                       "\":" + inode.getUserName() + ":" +
                                       inode.getGroupName() + ":" +
                                       (inode.isDirectory() ? "d" : "-") + mode);
      }
    }
  }

  private boolean hasPermission(INodeAttributes inode, FsAction access) {
    if (access == null) {
      return true;
    }

    FsPermission mode = inode.getFsPermission();
    FsAction perm = null;

    // 检查所有者权限
    if (user.equals(inode.getUserName())) {
      perm = mode.getUserAction();
    }
    // 检查组权限
    else if (groups.contains(inode.getGroupName())) {
      perm = mode.getGroupAction();
    }
    // 检查其他用户权限
    else {
      perm = mode.getOtherAction();
    }

    return perm.implies(access);
  }

  private boolean hasAclPermission(INodeAttributes inode, List<AclEntry> aclSpec,
                                  FsAction access) {

    // ACL权限检查算法
    // 1. 如果用户是所有者，检查所有者权限
    if (user.equals(inode.getUserName())) {
      AclEntry ownerEntry = findAclEntry(aclSpec, AclEntryType.USER, user);
      if (ownerEntry != null) {
        return ownerEntry.getPermission().implies(access);
      }
      return inode.getFsPermission().getUserAction().implies(access);
    }

    // 2. 检查命名用户权限
    AclEntry namedUserEntry = findAclEntry(aclSpec, AclEntryType.USER, user);
    if (namedUserEntry != null) {
      FsAction mask = getMask(aclSpec);
      FsAction effectivePerm = namedUserEntry.getPermission().and(mask);
      return effectivePerm.implies(access);
    }

    // 3. 检查所属组权限
    if (groups.contains(inode.getGroupName())) {
      AclEntry groupEntry = findAclEntry(aclSpec, AclEntryType.GROUP, inode.getGroupName());
      if (groupEntry != null) {
        FsAction mask = getMask(aclSpec);
        FsAction effectivePerm = groupEntry.getPermission().and(mask);
        return effectivePerm.implies(access);
      }
      FsAction mask = getMask(aclSpec);
      FsAction effectivePerm = inode.getFsPermission().getGroupAction().and(mask);
      return effectivePerm.implies(access);
    }

    // 4. 检查命名组权限
    for (String group : groups) {
      AclEntry namedGroupEntry = findAclEntry(aclSpec, AclEntryType.GROUP, group);
      if (namedGroupEntry != null) {
        FsAction mask = getMask(aclSpec);
        FsAction effectivePerm = namedGroupEntry.getPermission().and(mask);
        if (effectivePerm.implies(access)) {
          return true;
        }
      }
    }

    // 5. 检查其他用户权限
    return inode.getFsPermission().getOtherAction().implies(access);
  }

  private AclEntry findAclEntry(List<AclEntry> aclSpec, AclEntryType type, String name) {
    for (AclEntry entry : aclSpec) {
      if (entry.getType() == type &&
          (name == null || name.equals(entry.getName()))) {
        return entry;
      }
    }
    return null;
  }

  private FsAction getMask(List<AclEntry> aclSpec) {
    AclEntry maskEntry = findAclEntry(aclSpec, AclEntryType.MASK, null);
    return maskEntry != null ? maskEntry.getPermission() : FsAction.ALL;
  }
}

/**
 * ACL管理器
 */
public class AclManager {

  /**
   * 设置ACL
   */
  public void setAcl(String src, List<AclEntry> aclSpec) throws IOException {
    // 验证ACL规范
    validateAclSpec(aclSpec);

    // 获取INode
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    INode inode = iip.getLastINode();

    if (inode == null) {
      throw new FileNotFoundException("File not found: " + src);
    }

    // 检查权限
    fsd.checkOwner(pc, iip);

    // 设置ACL
    List<AclEntry> existingAcl = AclStorage.readINodeAcl(inode);
    List<AclEntry> newAcl = mergeAclEntries(existingAcl, aclSpec);

    AclStorage.updateINodeAcl(inode, newAcl, Snapshot.CURRENT_STATE_ID);

    LOG.info("Set ACL for " + src + ": " + newAcl);
  }

  /**
   * 获取ACL
   */
  public AclStatus getAclStatus(String src) throws IOException {
    INodesInPath iip = fsd.getINodesInPath(src, true);
    INode inode = iip.getLastINode();

    if (inode == null) {
      throw new FileNotFoundException("File not found: " + src);
    }

    // 检查读权限
    fsd.checkPermission(pc, iip, false, null, null, FsAction.READ_EXECUTE, null);

    List<AclEntry> acl = AclStorage.readINodeAcl(inode);
    FsPermission perm = inode.getFsPermission();

    return new AclStatus.Builder()
        .owner(inode.getUserName())
        .group(inode.getGroupName())
        .permission(perm)
        .addEntries(acl)
        .build();
  }

  /**
   * 移除ACL
   */
  public void removeAcl(String src) throws IOException {
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    INode inode = iip.getLastINode();

    if (inode == null) {
      throw new FileNotFoundException("File not found: " + src);
    }

    // 检查权限
    fsd.checkOwner(pc, iip);

    // 移除ACL，保留基本权限
    AclStorage.removeINodeAcl(inode);

    LOG.info("Removed ACL for " + src);
  }

  private void validateAclSpec(List<AclEntry> aclSpec) throws AclException {
    if (aclSpec == null || aclSpec.isEmpty()) {
      throw new AclException("ACL spec cannot be empty");
    }

    // 验证ACL条目的有效性
    Set<String> userEntries = new HashSet<>();
    Set<String> groupEntries = new HashSet<>();
    boolean hasOwner = false;
    boolean hasGroup = false;
    boolean hasOther = false;
    boolean hasMask = false;

    for (AclEntry entry : aclSpec) {
      switch (entry.getType()) {
        case USER:
          if (entry.getName() == null) {
            if (hasOwner) {
              throw new AclException("Duplicate owner entry");
            }
            hasOwner = true;
          } else {
            if (!userEntries.add(entry.getName())) {
              throw new AclException("Duplicate user entry: " + entry.getName());
            }
            hasMask = true;
          }
          break;

        case GROUP:
          if (entry.getName() == null) {
            if (hasGroup) {
              throw new AclException("Duplicate group entry");
            }
            hasGroup = true;
          } else {
            if (!groupEntries.add(entry.getName())) {
              throw new AclException("Duplicate group entry: " + entry.getName());
            }
            hasMask = true;
          }
          break;

        case MASK:
          if (hasMask) {
            throw new AclException("Duplicate mask entry");
          }
          hasMask = true;
          break;

        case OTHER:
          if (hasOther) {
            throw new AclException("Duplicate other entry");
          }
          hasOther = true;
          break;
      }
    }

    // 必须包含所有者、组和其他用户条目
    if (!hasOwner || !hasGroup || !hasOther) {
      throw new AclException("ACL must contain owner, group, and other entries");
    }
  }
}
```

### 13.5.2 YARN资源访问控制

YARN实现了队列级别的资源访问控制：

```java
/**
 * YARN队列访问控制管理器
 */
public class QueueAccessControlManager {

  private final Map<String, QueueACL> queueACLs;
  private final Configuration conf;

  public QueueAccessControlManager(Configuration conf) {
    this.conf = conf;
    this.queueACLs = new ConcurrentHashMap<>();
    loadQueueACLs();
  }

  /**
   * 检查队列访问权限
   */
  public boolean checkAccess(UserGroupInformation ugi, QueueACL acl, String queueName) {
    if (LOG.isDebugEnabled()) {
      LOG.debug("Checking queue access: user=" + ugi.getUserName() +
               ", queue=" + queueName + ", acl=" + acl);
    }

    // 获取队列ACL配置
    QueueACL queueAcl = queueACLs.get(queueName);
    if (queueAcl == null) {
      // 如果没有配置ACL，默认拒绝访问
      return false;
    }

    AccessControlList accessList = queueAcl.getAccessControlList(acl);
    if (accessList == null) {
      return false;
    }

    // 检查用户和组权限
    return accessList.isUserAllowed(ugi);
  }

  /**
   * 检查应用提交权限
   */
  public boolean checkApplicationAccess(UserGroupInformation ugi, String queueName,
                                       ApplicationSubmissionContext context) {

    // 检查基本提交权限
    if (!checkAccess(ugi, QueueACL.SUBMIT_APPLICATIONS, queueName)) {
      LOG.warn("User " + ugi.getUserName() + " denied submit access to queue " + queueName);
      return false;
    }

    // 检查应用类型权限
    String applicationType = context.getApplicationType();
    if (applicationType != null && !checkApplicationTypeAccess(ugi, queueName, applicationType)) {
      LOG.warn("User " + ugi.getUserName() + " denied access to application type " +
               applicationType + " in queue " + queueName);
      return false;
    }

    // 检查资源限制
    Resource requestedResource = context.getResource();
    if (!checkResourceLimits(ugi, queueName, requestedResource)) {
      LOG.warn("User " + ugi.getUserName() + " exceeded resource limits in queue " + queueName);
      return false;
    }

    return true;
  }

  /**
   * 检查应用管理权限
   */
  public boolean checkApplicationManagementAccess(UserGroupInformation ugi,
                                                 ApplicationId appId,
                                                 String queueName) {

    // 应用所有者总是有管理权限
    RMApp app = rmContext.getRMApps().get(appId);
    if (app != null && app.getUser().equals(ugi.getUserName())) {
      return true;
    }

    // 检查队列管理权限
    return checkAccess(ugi, QueueACL.ADMINISTER_QUEUE, queueName);
  }

  private boolean checkApplicationTypeAccess(UserGroupInformation ugi, String queueName,
                                           String applicationType) {
    String configKey = "yarn.scheduler.capacity." + queueName +
                      ".application-type." + applicationType + ".accessible-users";
    String allowedUsers = conf.get(configKey);

    if (allowedUsers == null) {
      // 如果没有特定配置，允许访问
      return true;
    }

    AccessControlList acl = new AccessControlList(allowedUsers);
    return acl.isUserAllowed(ugi);
  }

  private boolean checkResourceLimits(UserGroupInformation ugi, String queueName,
                                     Resource requestedResource) {
    // 检查用户资源限制
    String userLimitConfigKey = "yarn.scheduler.capacity." + queueName +
                               ".user-limit-factor";
    float userLimitFactor = conf.getFloat(userLimitConfigKey, 1.0f);

    // 获取队列容量
    String capacityConfigKey = "yarn.scheduler.capacity." + queueName + ".capacity";
    float queueCapacity = conf.getFloat(capacityConfigKey, 0.0f);

    // 计算用户可用资源
    Resource clusterResource = rmContext.getScheduler().getClusterResource();
    Resource queueResource = Resources.multiply(clusterResource, queueCapacity / 100.0f);
    Resource userLimit = Resources.multiply(queueResource, userLimitFactor);

    // 检查是否超过限制
    return Resources.fitsIn(requestedResource, userLimit);
  }

  private void loadQueueACLs() {
    // 从配置中加载队列ACL
    Map<String, String> queueConfigs = conf.getPropsWithPrefix("yarn.scheduler.capacity.");

    for (Map.Entry<String, String> entry : queueConfigs.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();

      if (key.endsWith(".acl_submit_applications")) {
        String queueName = extractQueueName(key, ".acl_submit_applications");
        QueueACL queueAcl = queueACLs.computeIfAbsent(queueName, k -> new QueueACL());
        queueAcl.setSubmitACL(new AccessControlList(value));
      } else if (key.endsWith(".acl_administer_queue")) {
        String queueName = extractQueueName(key, ".acl_administer_queue");
        QueueACL queueAcl = queueACLs.computeIfAbsent(queueName, k -> new QueueACL());
        queueAcl.setAdminACL(new AccessControlList(value));
      }
    }
  }

  private String extractQueueName(String configKey, String suffix) {
    int prefixLen = "yarn.scheduler.capacity.".length();
    int suffixStart = configKey.length() - suffix.length();
    return configKey.substring(prefixLen, suffixStart);
  }

  /**
   * 队列ACL配置
   */
  private static class QueueACL {
    private AccessControlList submitACL;
    private AccessControlList adminACL;

    public AccessControlList getAccessControlList(QueueACL acl) {
      switch (acl) {
        case SUBMIT_APPLICATIONS:
          return submitACL;
        case ADMINISTER_QUEUE:
          return adminACL;
        default:
          return null;
      }
    }

    public void setSubmitACL(AccessControlList submitACL) {
      this.submitACL = submitACL;
    }

    public void setAdminACL(AccessControlList adminACL) {
      this.adminACL = adminACL;
    }
  }
}

/**
 * 访问控制列表实现
 */
public class AccessControlList {

  private final Set<String> users;
  private final Set<String> groups;
  private final boolean allAllowed;

  public AccessControlList(String aclString) {
    if (aclString == null || aclString.trim().isEmpty()) {
      this.users = Collections.emptySet();
      this.groups = Collections.emptySet();
      this.allAllowed = false;
    } else if ("*".equals(aclString.trim())) {
      this.users = Collections.emptySet();
      this.groups = Collections.emptySet();
      this.allAllowed = true;
    } else {
      this.allAllowed = false;
      this.users = new HashSet<>();
      this.groups = new HashSet<>();

      parseAclString(aclString);
    }
  }

  private void parseAclString(String aclString) {
    String[] parts = aclString.split("\\s+");

    for (String part : parts) {
      if (part.trim().isEmpty()) {
        continue;
      }

      if (part.startsWith("@")) {
        // 组名以@开头
        groups.add(part.substring(1));
      } else {
        // 用户名
        users.add(part);
      }
    }
  }

  /**
   * 检查用户是否被允许
   */
  public boolean isUserAllowed(UserGroupInformation ugi) {
    if (allAllowed) {
      return true;
    }

    // 检查用户名
    if (users.contains(ugi.getUserName())) {
      return true;
    }

    // 检查用户组
    String[] userGroups = ugi.getGroupNames();
    for (String group : userGroups) {
      if (groups.contains(group)) {
        return true;
      }
    }

    return false;
  }

  @Override
  public String toString() {
    if (allAllowed) {
      return "*";
    }

    StringBuilder sb = new StringBuilder();

    for (String user : users) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append(user);
    }

    for (String group : groups) {
      if (sb.length() > 0) {
        sb.append(" ");
      }
      sb.append("@").append(group);
    }

    return sb.toString();
  }
}
```

## 13.6 数据加密技术

### 13.6.1 传输加密

Hadoop支持RPC和数据传输的加密：

```java
/**
 * RPC加密配置
 */
public class RpcEncryption {

  public enum QualityOfProtection {
    AUTHENTICATION("auth"),           // 仅认证
    INTEGRITY("auth-int"),           // 认证+完整性
    PRIVACY("auth-conf");            // 认证+完整性+机密性

    private final String saslQop;

    QualityOfProtection(String saslQop) {
      this.saslQop = saslQop;
    }

    public String getSaslQop() {
      return saslQop;
    }
  }

  /**
   * 配置RPC加密
   */
  public static void configureRpcEncryption(Configuration conf) {
    // 设置RPC保护级别
    String protection = conf.get("hadoop.rpc.protection", "authentication");
    QualityOfProtection qop = QualityOfProtection.valueOf(protection.toUpperCase());

    // 配置SASL属性
    Map<String, String> saslProps = new HashMap<>();
    saslProps.put(Sasl.QOP, qop.getSaslQop());
    saslProps.put(Sasl.SERVER_AUTH, "true");

    if (qop == QualityOfProtection.PRIVACY) {
      // 启用加密
      saslProps.put(Sasl.STRENGTH, "high");
      saslProps.put("javax.security.sasl.cipher.des", "true");
      saslProps.put("javax.security.sasl.cipher.3des", "true");
      saslProps.put("javax.security.sasl.cipher.aes128", "true");
      saslProps.put("javax.security.sasl.cipher.aes256", "true");
    }

    // 设置到配置中
    for (Map.Entry<String, String> entry : saslProps.entrySet()) {
      conf.set("hadoop.rpc.sasl." + entry.getKey(), entry.getValue());
    }
  }
}

/**
 * 数据传输加密
 */
public class DataTransferEncryption {

  private final boolean encryptDataTransfer;
  private final String encryptionAlgorithm;
  private final CryptoCodec codec;

  public DataTransferEncryption(Configuration conf) throws IOException {
    this.encryptDataTransfer = conf.getBoolean("dfs.encrypt.data.transfer", false);
    this.encryptionAlgorithm = conf.get("dfs.encrypt.data.transfer.algorithm", "3des");

    if (encryptDataTransfer) {
      this.codec = CryptoCodec.getInstance(conf, encryptionAlgorithm);
    } else {
      this.codec = null;
    }
  }

  /**
   * 创建加密输出流
   */
  public OutputStream createEncryptedOutputStream(OutputStream out, byte[] key, byte[] iv)
      throws IOException {
    if (!encryptDataTransfer) {
      return out;
    }

    Encryptor encryptor = codec.createEncryptor();
    encryptor.init(key, iv);

    return new CryptoOutputStream(out, codec, encryptor, CryptoStreamUtils.getBufferSize(conf));
  }

  /**
   * 创建解密输入流
   */
  public InputStream createDecryptedInputStream(InputStream in, byte[] key, byte[] iv)
      throws IOException {
    if (!encryptDataTransfer) {
      return in;
    }

    Decryptor decryptor = codec.createDecryptor();
    decryptor.init(key, iv);

    return new CryptoInputStream(in, codec, decryptor, CryptoStreamUtils.getBufferSize(conf));
  }

  /**
   * 生成随机密钥
   */
  public byte[] generateKey() {
    if (!encryptDataTransfer) {
      return null;
    }

    int keyLen = codec.getCipherSuite().getAlgorithmBlockSize();
    byte[] key = new byte[keyLen];
    new SecureRandom().nextBytes(key);
    return key;
  }

  /**
   * 生成随机初始化向量
   */
  public byte[] generateIV() {
    if (!encryptDataTransfer) {
      return null;
    }

    int ivLen = codec.getCipherSuite().getAlgorithmBlockSize();
    byte[] iv = new byte[ivLen];
    new SecureRandom().nextBytes(iv);
    return iv;
  }
}
```

### 13.6.2 静态数据加密

HDFS透明加密保护静态数据：

```java
/**
 * HDFS加密区域管理器
 */
public class EncryptionZoneManager {

  private final Map<Long, EncryptionZone> encryptionZones;
  private final KeyProvider keyProvider;
  private final Configuration conf;

  public EncryptionZoneManager(Configuration conf) throws IOException {
    this.conf = conf;
    this.encryptionZones = new ConcurrentHashMap<>();
    this.keyProvider = createKeyProvider(conf);
  }

  /**
   * 创建加密区域
   */
  public void createEncryptionZone(String src, String keyName) throws IOException {
    // 验证路径
    INodesInPath iip = fsd.getINodesInPath4Write(src);
    INode inode = iip.getLastINode();

    if (inode == null) {
      throw new FileNotFoundException("Directory not found: " + src);
    }

    if (!inode.isDirectory()) {
      throw new IOException("Encryption zone must be a directory: " + src);
    }

    // 检查权限
    fsd.checkOwner(pc, iip);

    // 验证密钥存在
    if (keyProvider.getKeyVersion(keyName) == null) {
      throw new IOException("Key not found: " + keyName);
    }

    // 检查是否已经在加密区域内
    if (isInEncryptionZone(src)) {
      throw new IOException("Directory is already in an encryption zone: " + src);
    }

    // 创建加密区域
    long inodeId = inode.getId();
    EncryptionZone ez = new EncryptionZone(inodeId, src, keyName,
                                          CipherSuite.AES_CTR_NOPADDING);
    encryptionZones.put(inodeId, ez);

    // 设置INode的加密属性
    inode.addXAttr(CRYPTO_XATTR_ENCRYPTION_ZONE, ez.toByteArray());

    LOG.info("Created encryption zone: " + src + " with key: " + keyName);
  }

  /**
   * 检查路径是否在加密区域内
   */
  public boolean isInEncryptionZone(String src) {
    INodesInPath iip = fsd.getINodesInPath(src, true);
    return getEncryptionZone(iip) != null;
  }

  /**
   * 获取路径的加密区域
   */
  public EncryptionZone getEncryptionZone(String src) {
    INodesInPath iip = fsd.getINodesInPath(src, true);
    return getEncryptionZone(iip);
  }

  private EncryptionZone getEncryptionZone(INodesInPath iip) {
    for (int i = iip.length() - 1; i >= 0; i--) {
      INode inode = iip.getINode(i);
      if (inode != null) {
        EncryptionZone ez = encryptionZones.get(inode.getId());
        if (ez != null) {
          return ez;
        }
      }
    }
    return null;
  }

  /**
   * 为文件生成加密密钥
   */
  public FileEncryptionInfo createFileEncryptionInfo(String src, CipherSuite suite)
      throws IOException {

    EncryptionZone ez = getEncryptionZone(src);
    if (ez == null) {
      return null;
    }

    // 生成数据加密密钥
    KeyVersion keyVersion = keyProvider.getCurrentKey(ez.getKeyName());
    byte[] encryptionKey = generateEncryptionKey(keyVersion, suite);

    // 生成初始化向量
    byte[] iv = new byte[suite.getAlgorithmBlockSize()];
    new SecureRandom().nextBytes(iv);

    // 加密数据加密密钥
    byte[] encryptedKey = encryptKey(encryptionKey, keyVersion);

    return new FileEncryptionInfo(suite, CryptoProtocolVersion.ENCRYPTION_ZONES,
                                 encryptedKey, iv, ez.getKeyName(), keyVersion.getName());
  }

  /**
   * 解密文件加密密钥
   */
  public byte[] decryptEncryptionKey(FileEncryptionInfo feInfo) throws IOException {
    KeyVersion keyVersion = keyProvider.getKeyVersion(feInfo.getKeyName(),
                                                     feInfo.getEzKeyVersionName());
    return decryptKey(feInfo.getEncryptedDataEncryptionKey(), keyVersion);
  }

  private byte[] generateEncryptionKey(KeyVersion keyVersion, CipherSuite suite) {
    // 使用HKDF从主密钥派生数据加密密钥
    byte[] masterKey = keyVersion.getMaterial();
    byte[] salt = "HDFS_EZ_DEK".getBytes(StandardCharsets.UTF_8);

    return HKDF.expand(HKDF.extract(salt, masterKey),
                      "HDFS_EZ_DEK".getBytes(StandardCharsets.UTF_8),
                      suite.getKeyLength());
  }

  private byte[] encryptKey(byte[] key, KeyVersion keyVersion) throws IOException {
    // 使用AES-GCM加密数据加密密钥
    try {
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      SecretKeySpec keySpec = new SecretKeySpec(keyVersion.getMaterial(), "AES");
      cipher.init(Cipher.ENCRYPT_MODE, keySpec);

      return cipher.doFinal(key);
    } catch (Exception e) {
      throw new IOException("Failed to encrypt key", e);
    }
  }

  private byte[] decryptKey(byte[] encryptedKey, KeyVersion keyVersion) throws IOException {
    // 使用AES-GCM解密数据加密密钥
    try {
      Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
      SecretKeySpec keySpec = new SecretKeySpec(keyVersion.getMaterial(), "AES");
      cipher.init(Cipher.DECRYPT_MODE, keySpec);

      return cipher.doFinal(encryptedKey);
    } catch (Exception e) {
      throw new IOException("Failed to decrypt key", e);
    }
  }

  private KeyProvider createKeyProvider(Configuration conf) throws IOException {
    String providerUriStr = conf.get("dfs.encryption.key.provider.uri");
    if (providerUriStr == null || providerUriStr.isEmpty()) {
      throw new IOException("Key provider URI not configured");
    }

    URI providerUri = URI.create(providerUriStr);
    return KeyProviderFactory.get(providerUri, conf);
  }
}

/**
 * 加密区域信息
 */
public class EncryptionZone {

  private final long inodeId;
  private final String path;
  private final String keyName;
  private final CipherSuite suite;
  private final CryptoProtocolVersion version;

  public EncryptionZone(long inodeId, String path, String keyName, CipherSuite suite) {
    this.inodeId = inodeId;
    this.path = path;
    this.keyName = keyName;
    this.suite = suite;
    this.version = CryptoProtocolVersion.ENCRYPTION_ZONES;
  }

  public byte[] toByteArray() throws IOException {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream dos = new DataOutputStream(baos);

    dos.writeLong(inodeId);
    dos.writeUTF(path);
    dos.writeUTF(keyName);
    dos.writeUTF(suite.getName());
    dos.writeInt(version.getVersion());

    dos.close();
    return baos.toByteArray();
  }

  public static EncryptionZone fromByteArray(byte[] data) throws IOException {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    DataInputStream dis = new DataInputStream(bais);

    long inodeId = dis.readLong();
    String path = dis.readUTF();
    String keyName = dis.readUTF();
    String suiteName = dis.readUTF();
    int version = dis.readInt();

    dis.close();

    CipherSuite suite = CipherSuite.convert(suiteName);
    return new EncryptionZone(inodeId, path, keyName, suite);
  }

  // Getters
  public long getInodeId() { return inodeId; }
  public String getPath() { return path; }
  public String getKeyName() { return keyName; }
  public CipherSuite getCipherSuite() { return suite; }
  public CryptoProtocolVersion getVersion() { return version; }
}
```

## 13.7 安全审计与监控

### 13.7.1 审计日志系统

Hadoop提供了完整的安全审计功能：

```java
/**
 * 审计日志记录器
 */
public class AuditLogger {

  private static final Logger AUDIT_LOG = LoggerFactory.getLogger("SecurityAudit");
  private static final String AUDIT_LOG_FORMAT =
      "allowed=%s\tugi=%s\tip=%s\tcmd=%s\tsrc=%s\tdst=%s\tperm=%s\tproto=%s";

  /**
   * 记录文件系统操作审计
   */
  public static void logAuditEvent(boolean succeeded, String userName,
                                  InetAddress addr, String cmd, String src,
                                  String dest, FileStatus stat) {

    if (AUDIT_LOG.isInfoEnabled()) {
      StringBuilder sb = new StringBuilder();
      sb.append("allowed=").append(succeeded).append("\t");
      sb.append("ugi=").append(userName).append("\t");
      sb.append("ip=").append(addr != null ? addr.getHostAddress() : "unknown").append("\t");
      sb.append("cmd=").append(cmd).append("\t");
      sb.append("src=").append(src != null ? src : "null").append("\t");
      sb.append("dst=").append(dest != null ? dest : "null").append("\t");

      if (stat != null) {
        sb.append("perm=");
        sb.append(stat.getOwner()).append(":");
        sb.append(stat.getGroup()).append(":");
        sb.append(stat.getPermission());
      } else {
        sb.append("perm=null");
      }

      AUDIT_LOG.info(sb.toString());
    }
  }

  /**
   * 记录认证事件审计
   */
  public static void logAuthenticationEvent(boolean succeeded, String userName,
                                          InetAddress addr, String protocol) {
    if (AUDIT_LOG.isInfoEnabled()) {
      String result = succeeded ? "SUCCESS" : "FAILURE";
      AUDIT_LOG.info("AUTH: {} user={} ip={} protocol={}",
                     result, userName,
                     addr != null ? addr.getHostAddress() : "unknown",
                     protocol);
    }
  }

  /**
   * 记录授权事件审计
   */
  public static void logAuthorizationEvent(boolean succeeded, String userName,
                                         InetAddress addr, String resource,
                                         String action) {
    if (AUDIT_LOG.isInfoEnabled()) {
      String result = succeeded ? "SUCCESS" : "FAILURE";
      AUDIT_LOG.info("AUTHZ: {} user={} ip={} resource={} action={}",
                     result, userName,
                     addr != null ? addr.getHostAddress() : "unknown",
                     resource, action);
    }
  }

  /**
   * 记录Token操作审计
   */
  public static void logTokenEvent(String operation, String userName,
                                  InetAddress addr, String tokenKind) {
    if (AUDIT_LOG.isInfoEnabled()) {
      AUDIT_LOG.info("TOKEN: operation={} user={} ip={} kind={}",
                     operation, userName,
                     addr != null ? addr.getHostAddress() : "unknown",
                     tokenKind);
    }
  }
}

/**
 * 安全事件监控器
 */
public class SecurityEventMonitor {

  private final EventBus eventBus;
  private final List<SecurityEventHandler> handlers;
  private final ScheduledExecutorService scheduler;

  public SecurityEventMonitor() {
    this.eventBus = new EventBus("SecurityEvents");
    this.handlers = new ArrayList<>();
    this.scheduler = Executors.newScheduledThreadPool(2);

    // 注册默认处理器
    registerHandler(new FailedLoginDetector());
    registerHandler(new SuspiciousActivityDetector());
    registerHandler(new PrivilegeEscalationDetector());
  }

  public void registerHandler(SecurityEventHandler handler) {
    handlers.add(handler);
    eventBus.register(handler);
  }

  public void publishEvent(SecurityEvent event) {
    eventBus.post(event);
  }

  /**
   * 失败登录检测器
   */
  private static class FailedLoginDetector {

    private final Map<String, AtomicInteger> failedAttempts = new ConcurrentHashMap<>();
    private final Map<String, Long> lastFailureTime = new ConcurrentHashMap<>();

    @Subscribe
    public void handleAuthenticationFailure(AuthenticationFailureEvent event) {
      String user = event.getUserName();
      String clientIP = event.getClientAddress();
      String key = user + "@" + clientIP;

      AtomicInteger attempts = failedAttempts.computeIfAbsent(key, k -> new AtomicInteger(0));
      int count = attempts.incrementAndGet();
      lastFailureTime.put(key, System.currentTimeMillis());

      if (count >= 5) {
        // 触发安全警报
        SecurityAlert alert = new SecurityAlert(
            SecurityAlert.Level.HIGH,
            "Multiple failed login attempts",
            "User " + user + " from " + clientIP + " has " + count + " failed login attempts"
        );

        publishSecurityAlert(alert);
      }
    }

    @Subscribe
    public void handleAuthenticationSuccess(AuthenticationSuccessEvent event) {
      String user = event.getUserName();
      String clientIP = event.getClientAddress();
      String key = user + "@" + clientIP;

      // 清除失败计数
      failedAttempts.remove(key);
      lastFailureTime.remove(key);
    }

    private void publishSecurityAlert(SecurityAlert alert) {
      LOG.warn("Security Alert: {} - {}", alert.getLevel(), alert.getMessage());
      // 可以发送到安全监控系统
    }
  }

  /**
   * 可疑活动检测器
   */
  private static class SuspiciousActivityDetector {

    private final Map<String, UserActivityProfile> userProfiles = new ConcurrentHashMap<>();

    @Subscribe
    public void handleFileAccess(FileAccessEvent event) {
      String user = event.getUserName();
      UserActivityProfile profile = userProfiles.computeIfAbsent(user,
          k -> new UserActivityProfile());

      profile.recordAccess(event.getPath(), event.getAction());

      // 检测异常访问模式
      if (profile.isAnomalousAccess(event.getPath(), event.getAction())) {
        SecurityAlert alert = new SecurityAlert(
            SecurityAlert.Level.MEDIUM,
            "Suspicious file access pattern",
            "User " + user + " accessed " + event.getPath() +
            " which is unusual for this user"
        );

        publishSecurityAlert(alert);
      }
    }

    private void publishSecurityAlert(SecurityAlert alert) {
      LOG.warn("Security Alert: {} - {}", alert.getLevel(), alert.getMessage());
    }
  }

  /**
   * 权限提升检测器
   */
  private static class PrivilegeEscalationDetector {

    @Subscribe
    public void handlePermissionChange(PermissionChangeEvent event) {
      if (event.isPrivilegeEscalation()) {
        SecurityAlert alert = new SecurityAlert(
            SecurityAlert.Level.HIGH,
            "Privilege escalation detected",
            "User " + event.getUserName() + " changed permissions on " +
            event.getPath() + " from " + event.getOldPermission() +
            " to " + event.getNewPermission()
        );

        publishSecurityAlert(alert);
      }
    }

    private void publishSecurityAlert(SecurityAlert alert) {
      LOG.warn("Security Alert: {} - {}", alert.getLevel(), alert.getMessage());
    }
  }
}
```

## 13.8 本章小结

本章深入分析了Hadoop的安全机制与认证授权体系。Hadoop安全架构是一个多层次、多维度的综合安全解决方案，为大数据环境提供了强有力的安全保障。

**核心要点**：

**安全架构**：分层的安全设计，包括认证、授权、加密、审计四个层次。

**Kerberos认证**：基于票据的强身份认证机制，提供了可靠的身份验证。

**SASL安全层**：灵活的认证框架，支持多种认证机制和安全级别。

**Token认证**：轻量级的认证方案，优化了性能和用户体验。

**配置管理**：统一的安全配置管理，简化了安全部署和维护。

Hadoop安全机制的设计体现了企业级安全的要求，在保证安全性的同时，也考虑了性能和可用性。理解这些安全机制的实现原理，对于在生产环境中正确部署和管理Hadoop集群具有重要意义。
