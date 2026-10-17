# Config Server 多服务自动热刷新设计文档

**版本**：v1.0  
**适用场景**：Kubernetes 多 Pod、多微服务，无 Webhook、无 MQ / Bus、无 ConfigMap watch  
**配置中心**：Spring Cloud Config Server（Git）  
**目标**：Config Server 发布（Git 变更）后，所有服务的所有 Pod **自动、最终一致地**热刷新白名单配置，同时**最大限度降低 Config Server 访问压力**

---

## 1. 背景与目标

### 1.1 现状约束

| 约束项 | 值 |
|---|---|
| 配置源 | Spring Cloud Config Server（背后为 Git 仓库） |
| 部署形态 | Kubernetes，每个服务多副本（Pod） |
| GitHub / GitLab Webhook | ❌ 不可用 |
| MQ（RabbitMQ / Kafka）与 Spring Cloud Bus | ❌ 不可用 |
| K8s ConfigMap watch（Spring Cloud Kubernetes reload） | ❌ 不可用 |
| 目标 | Config Server 发布（Git 推送）后，多服务多 Pod **自动热刷新**可热刷配置 |

### 1.2 核心矛盾

Spring Cloud Config **只保证“启动期拉取”**，没有内置“客户端自动轮询 + 多实例广播”。因此“自动刷新”必须由客户端侧实现。但：

- 每个 Pod 各自轮询 Config Server → 会放大 Config Server 流量
- Pod 数 × 服务数 × 轮询频率 若不控制，容易压垮 Config Server
- 一旦刷新风暴，反而影响所有服务的配置获取

### 1.3 设计目标

1. **自动刷新**：Git 变更后，所有服务所有 Pod 最终一致地拿到新配置
2. **压力可控**：大幅降低 Config Server 请求压力，避免高频全量拉取
3. **多服务通用**：接入方式统一，服务间零差异
4. **安全可靠**：非白名单配置变更不依赖热刷，强制滚动重启
5. **可观测可排障**：结构化日志 + 指标 + 告警，运维可快速定位

---

## 2. 总体设计思路（三层降级）

采用**“先廉价判断，后决定是否拉取”**的三层降级策略，把高成本操作放到最后：

```
┌─────────────────────────────────────────────────────┐
│ 第 1 层：版本号检查（最廉价）                         │
│  GET /actuator/info 或 /admin/version               │
│  服务端返回 { "config.version": "abc123" }           │
│  本地缓存上一次 version，不一致才进入第 2 层          │
└───────────────────────┬─────────────────────────────┘
                        │ version 变化
                        ▼
┌─────────────────────────────────────────────────────┐
│ 第 2 层：指纹检查（中等成本）                        │
│  GET /application/{app}/{profile}/{label}            │
│  对响应体做 MD5，与本地指纹比对                       │
│  指纹未变 → 直接返回，绝不触发 refresh                │
└───────────────────────┬─────────────────────────────┘
                        │ 指纹变化
                        ▼
┌─────────────────────────────────────────────────────┐
│ 第 3 层：真正刷新                                    │
│  RefreshEndpoint.refresh()                           │
│  由 RefreshWhitelistGuard 校验白名单                  │
│  记录结构化日志 + 指标                                │
└─────────────────────────────────────────────────────┘
```

**关键洞察**：绝大多数轮询周期内，配置并没有变化。  
因此：

- 第 1 层（版本号）成本极低，可以**高频**执行
- 第 2 层（指纹）只在版本变化时才执行
- 第 3 层（refresh）只在指纹变化时才执行

这样就把“全量拉取频率”降到最低。

---

## 3. 架构总览

```
                    ┌──────────────────────────┐
                    │       Git Repository     │
                    └───────────┬──────────────┘
                                │ push
                                ▼
                    ┌──────────────────────────┐
                    │     Config Server         │
                    │  · git.refresh-rate=15s  │
                    │  · /actuator/info        │◀── 版本号（轻）
                    │  · /application/{app}/   │◀── 指纹（中）
                    │    {profile}/{label}     │
                    └───────────┬──────────────┘
                                │
        ┌───────────────────────┼───────────────────────┐
        ▼                       ▼                       ▼
┌───────────────┐       ┌───────────────┐       ┌───────────────┐
│ order-svc Pod1│       │ payment-svc   │       │ user-svc      │
│ ConfigPoller  │       │ ConfigPoller  │       │ ConfigPoller  │
│  · version TTL│       │  · version TTL│       │  · version TTL│
│  · fingerprint│       │  · fingerprint│       │  · fingerprint│
│  · refresh    │       │  · refresh    │       │  · refresh    │
│  · whitelist  │       │  · whitelist  │       │  · whitelist  │
└───────────────┘       └───────────────┘       └───────────────┘
       每个 Pod 独立轮询，随机抖动，最终一致
```

### 3.1 配置隔离

- 每个服务通过 `spring.application.name` 自动决定轮询路径
- 配置仓库目录规范：`{service}-{profile}.yml` + `{service}-{profile}-hot.yml`
- 指纹按服务独立计算，互不影响

### 3.2 多服务时序

```
T+0s    git push
T+0~15s Config Server 拿到新提交
T+16s   order-svc Pod-7a2f  → 版本变化 → 拉指纹 → 指纹变化 → refresh
T+19s   payment-svc Pod-x   → 版本变化 → 拉指纹 → 指纹变化 → refresh
T+22s   user-svc Pod-y      → 版本变化 → 拉指纹 → 指纹变化 → refresh
T+25s   全部收敛
```

最坏不一致窗口 ≈ `version TTL + jitter + 指纹拉取耗时` ≈ **20~30 秒**。

---

## 4. 压力控制设计（核心）

### 4.1 两层 TTL 分离

| 检查项 | 默认 TTL | 成本 | 说明 |
|---|---|---|---|
| 版本号检查 | **5 秒** | 极小（轻量 endpoint） | 判断“有没有可能变了” |
| 指纹检查 | **60 秒** | 中（返回整份 Environment） | 只在版本变化时执行 |
| 真正 refresh | 事件触发 | 中高（Bean 重建） | 只在指纹变化时执行 |

> ✅ 效果：即使 100 个 Pod 每 5 秒查一次版本号，也只是一个轻量 HTTP；  
> 而**整份配置拉取被限制在“版本变化”时才发生**，压力下降数量级。

### 4.2 指数退避（Config Server 异常时）

当 Config Server 不可达：

- 第 1 次失败 → 下次轮询 5s 后
- 连续失败 → 退避：10s、20s、40s … 上限 60s
- 恢复后 → 立即回到正常 TTL

避免雪崩式重试。

### 4.3 随机抖动

- 每个 Pod 启动时随机偏移 `0 ~ maxJitterSeconds`
- 防止所有 Pod 同时发起请求

### 4.4 失败静默

- 拉取失败 / 超时 → 本轮跳过，不打 error 刷屏
- 只记录 `CONFIG_POLL_FAILED`（WARN）+ 指标

---

## 5. 配置分层与白名单

### 5.1 可热刷白名单（全公司统一）

```yaml
app.*           # 业务开关、限流、超时
feature.*       # feature flag
biz.*           # 领域参数
logging.level.* # 日志级别
```

### 5.2 不可热刷（改了必须滚动重启）

```yaml
server.*
spring.datasource.*
spring.jpa.*
spring.redis.*
spring.kafka.*
spring.rabbitmq.*
spring.ssl.*
management.*
```

> ⚠️ 不可热刷配置即使“值变了”，也**绝不依赖热刷生效**，必须由 CI 触发 `kubectl rollout restart`。

### 5.3 Git 仓库目录规范

```
config-repo/
├── order-service-prod.yml          # 启停配置（不可热刷）
├── order-service-prod-hot.yml      # 热配置（可热刷）
├── payment-service-prod.yml
├── payment-service-prod-hot.yml
├── user-service-prod.yml
└── user-service-prod-hot.yml
```

Config Server 把同 profile 的文件合并进一个 Environment；客户端指纹基于**合并后的整份配置**。

---

## 6. 完整代码实现

> 包名：`com.example.config.autorefresh`  
> 形态：Spring Boot Starter，多服务接入只需依赖 + 配置

### 6.1 自动装配入口

```java
@Configuration
@ConditionalOnProperty(
    name = "config.auto-refresh.enabled",
    havingValue = "true",
    matchIfMissing = true
)
@EnableScheduling
@ConfigurationPropertiesScan
public class ConfigAutoRefreshAutoConfiguration {

    @Bean
    public ConfigAutoRefreshProperties autoRefreshProperties() {
        return new ConfigAutoRefreshProperties();
    }

    @Bean
    public RuntimeIdentity runtimeIdentity(Environment env) {
        return new RuntimeIdentity(env);
    }

    @Bean
    public ConfigRefreshLogger refreshLogger(RuntimeIdentity id,
                                             ConfigAutoRefreshProperties props) {
        return new ConfigRefreshLogger(id, props);
    }

    @Bean
    public ConfigVersionProvider versionProvider(RestTemplateBuilder builder,
                                                ConfigAutoRefreshProperties props) {
        return new ConfigServerVersionProvider(builder.build(), props);
    }

    @Bean
    public ConfigFingerprintProvider fingerprintProvider(RestTemplateBuilder builder,
                                                        ConfigAutoRefreshProperties props,
                                                        Environment env) {
        return new ConfigServerFingerprintProvider(builder.build(), props, env);
    }

    @Bean
    public RefreshWhitelistGuard whitelistGuard(ConfigAutoRefreshProperties props) {
        return new RefreshWhitelistGuard(props.getWhitelistPrefixes());
    }

    @Bean
    public ConfigPoller configPoller(ConfigVersionProvider versionProvider,
                                     ConfigFingerprintProvider fpProvider,
                                     RefreshEndpoint refreshEndpoint,
                                     RefreshWhitelistGuard guard,
                                     ConfigRefreshLogger logger,
                                     ConfigAutoRefreshProperties props) {
        return new ConfigPoller(versionProvider, fpProvider, refreshEndpoint,
                                guard, logger, props);
    }

    @Bean
    public ConfigAutoRefreshMetrics metrics() {
        return new ConfigAutoRefreshMetrics();
    }
}
```

### 6.2 配置属性

```java
@ConfigurationProperties("config.auto-refresh")
public class ConfigAutoRefreshProperties {

    /** 是否启用自动刷新 */
    private boolean enabled = true;

    /** Config Server 基础地址 */
    private String server = "http://config-server:8888";

    /** 版本号检查周期（秒） */
    private int versionCheckSeconds = 5;

    /** 指纹拉取最小间隔（秒），防止版本号频繁变化时反复拉取 */
    private int fingerprintMinIntervalSeconds = 60;

    /** 随机抖动上限（秒） */
    private int maxJitterSeconds = 10;

    /** profile */
    private String profile = "default";

    /** label / 分支 */
    private String label = "main";

    /** 热刷白名单前缀 */
    private List<String> whitelistPrefixes =
            List.of("app.", "feature.", "biz.", "logging.level.");

    /** Config Server 不可达时最大退避（秒） */
    private int maxBackoffSeconds = 60;

    /** 单次拉取超时（秒） */
    private int connectTimeoutSeconds = 3;
    private int readTimeoutSeconds = 5;

    // getters / setters
}
```

### 6.3 运行时身份（服务名 + Pod 名）

```java
public class RuntimeIdentity {
    private final String service;
    private final String pod;

    public RuntimeIdentity(Environment env) {
        this.service = env.getProperty("spring.application.name", "unknown");
        String podName = env.getProperty("HOSTNAME");
        if (podName == null) podName = env.getProperty("POD_NAME");
        if (podName == null) podName = "unknown";
        this.pod = podName;
    }

    public String service() { return service; }
    public String pod() { return pod; }
}
```

### 6.4 版本号 Provider（第 1 层）

```java
public interface ConfigVersionProvider {
    /** 返回 Config Server 当前配置版本，失败返回 null */
    String currentVersion();
}

/**
 * 通过 Config Server 的 /actuator/info 暴露 config.version
 * Config Server 侧需配置：
 *   management.info.config.enabled=true
 *   spring.cloud.config.server.prefix=/config
 * 或在 Config Server 自定义一个返回 Git commit hash 的 endpoint
 */
public class ConfigServerVersionProvider implements ConfigVersionProvider {

    private final RestTemplate rt;
    private final String url;

    public ConfigServerVersionProvider(RestTemplate rt,
                                       ConfigAutoRefreshProperties props) {
        this.rt = rt;
        this.url = props.getServer() + "/actuator/info";
    }

    @Override
    public String currentVersion() {
        try {
            JsonNode node = rt.getForObject(url, JsonNode.class);
            if (node != null && node.has("config")) {
                return node.get("config").path("version").asText(null);
            }
            return null;
        } catch (RestClientException e) {
            return null;
        }
    }
}
```

> **Config Server 侧配合**：暴露 `config.version`（Git commit hash / 时间戳），可自定义 `InfoContributor`：
> ```java
> @Component
> public class ConfigVersionInfoContributor implements InfoContributor {
>     @Autowired Environment env;
>     @Override public void contribute(Info.Builder builder) {
>         builder.withDetail("config",
>             Map.of("version", env.getProperty("spring.cloud.config.server.git.commit", "unknown")));
>     }
> }
> ```

### 6.5 指纹 Provider（第 2 层）

```java
public interface ConfigFingerprintProvider {
    /** 返回当前远程配置指纹，失败返回 null */
    String currentFingerprint();
}

public class ConfigServerFingerprintProvider implements ConfigFingerprintProvider {

    private final RestTemplate rt;
    private final String url;

    public ConfigServerFingerprintProvider(RestTemplate rt,
                                          ConfigAutoRefreshProperties props,
                                          Environment env) {
        this.rt = rt;
        String app = env.getRequiredProperty("spring.application.name");
        this.url = String.format("%s/application/%s/%s/%s",
                props.getServer(), app, props.getProfile(), props.getLabel());
    }

    @Override
    public String currentFingerprint() {
        try {
            String body = rt.getForObject(url, String.class);
            if (body == null) return null;
            return DigestUtils.md5DigestAsHex(body.getBytes(StandardCharsets.UTF_8));
        } catch (RestClientException e) {
            return null;
        }
    }
}
```

### 6.6 白名单守卫

```java
public class RefreshWhitelistGuard {

    private final Set<String> whitelist;

    public RefreshWhitelistGuard(List<String> prefixes) {
        this.whitelist = prefixes.stream()
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .collect(Collectors.toUnmodifiableSet());
    }

    public Set<String> validate(Set<String> changedKeys) {
        Set<String> illegal = changedKeys.stream()
                .filter(k -> whitelist.stream().noneMatch(k::startsWith))
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));

        return illegal;
    }

    public Set<String> hotKeys(Set<String> changedKeys) {
        return changedKeys.stream()
                .filter(k -> whitelist.stream().anyMatch(k::startsWith))
                .sorted()
                .collect(Collectors.toCollection(LinkedHashSet::new));
    }
}
```

### 6.7 轮询器核心（三层降级）

```java
@Slf4j
public class ConfigPoller implements InitializingBean {

    private final ConfigVersionProvider versionProvider;
    private final ConfigFingerprintProvider fpProvider;
    private final RefreshEndpoint refreshEndpoint;
    private final RefreshWhitelistGuard guard;
    private final ConfigRefreshLogger logger;
    private final ConfigAutoRefreshProperties props;

    private volatile String lastVersion;
    private volatile String lastFingerprint;
    private volatile long lastFingerprintFetchTs = 0;

    private int backoffSeconds;
    private final ScheduledExecutorService scheduler =
            Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, "config-auto-refresh");
                t.setDaemon(true);
                return t;
            });

    public ConfigPoller(ConfigVersionProvider versionProvider,
                        ConfigFingerprintProvider fpProvider,
                        RefreshEndpoint refreshEndpoint,
                        RefreshWhitelistGuard guard,
                        ConfigRefreshLogger logger,
                        ConfigAutoRefreshProperties props) {
        this.versionProvider = versionProvider;
        this.fpProvider = fpProvider;
        this.refreshEndpoint = refreshEndpoint;
        this.guard = guard;
        this.logger = logger;
        this.props = props;
    }

    @Override
    public void afterPropertiesSet() {
        long initialDelay = ThreadLocalRandom.current()
                .nextLong(0, props.getMaxJitterSeconds() + 1);
        scheduler.scheduleAtFixedRate(this::tick,
                initialDelay,
                props.getVersionCheckSeconds(),
                TimeUnit.SECONDS);
        logger.started();
    }

    private void tick() {
        try {
            String version = versionProvider.currentVersion();
            if (version == null) {
                handleFailure("version endpoint unavailable");
                return;
            }

            // 第 1 层：版本未变 → 直接返回，零成本
            if (lastVersion != null && lastVersion.equals(version)) {
                resetBackoff();
                return;
            }

            // 版本变了：检查指纹最小间隔，避免频繁拉取
            long now = System.currentTimeMillis();
            if (lastFingerprint != null
                    && (now - lastFingerprintFetchTs)
                        < props.getFingerprintMinIntervalSeconds() * 1000L) {
                // 距上次指纹拉取不足最小间隔，仅更新版本号，不拉指纹
                lastVersion = version;
                return;
            }

            // 第 2 层：拉取指纹
            String fp = fpProvider.currentFingerprint();
            lastFingerprintFetchTs = now;
            if (fp == null) {
                handleFailure("fingerprint endpoint unavailable");
                return;
            }
            resetBackoff();

            // 指纹未变（同一次提交可能多服务共用） → 跳过
            if (lastFingerprint != null && lastFingerprint.equals(fp)) {
                lastVersion = version;
                return;
            }

            // 第 3 层：真正刷新
            String oldFp = lastFingerprint;
            String oldVersion = lastVersion;
            lastVersion = version;
            lastFingerprint = fp;

            Map<String, Object> result = refreshEndpoint.refresh();
            Set<String> changed = result.keySet();

            Set<String> illegal = guard.validate(changed);
            Set<String> hot = guard.hotKeys(changed);

            if (illegal.isEmpty() && hot.isEmpty()) {
                logger.noop(oldFp, fp);
                ConfigAutoRefreshMetrics.INSTANCE.markNoop();
                return;
            }

            if (!illegal.isEmpty()) {
                logger.illegalKeys(illegal);
                ConfigAutoRefreshMetrics.INSTANCE.markIllegal(illegal.size());
            }

            logger.changed(oldVersion, version, oldFp, fp, hot, illegal);
            ConfigAutoRefreshMetrics.INSTANCE.markRefresh(hot.size(), fp);

        } catch (Exception e) {
            handleFailure(e.getMessage());
        }
    }

    private void handleFailure(String msg) {
        log.warn("[config-auto-refresh] poll failed: {}", msg);
        logger.failed(msg);
        ConfigAutoRefreshMetrics.INSTANCE.markPollFailure();
        // 退避：增大下次版本检查间隔，减轻 Config Server 压力
        backoffSeconds = Math.min(
                backoffSeconds == 0 ? 5 : backoffSeconds * 2,
                props.getMaxBackoffSeconds());
        // 重新调度（简化：实际可改用 reschedule）
    }

    private void resetBackoff() {
        backoffSeconds = 0;
    }
}
```

### 6.8 结构化日志

```java
@Slf4j
public class ConfigRefreshLogger {

    private final RuntimeIdentity id;
    private final String profile;
    private final String label;
    private final ObjectMapper mapper = new ObjectMapper();

    public ConfigRefreshLogger(RuntimeIdentity id, ConfigAutoRefreshProperties props) {
        this.id = id;
        this.profile = props.getProfile();
        this.label = props.getLabel();
    }

    public void started() {
        info("CONFIG_AUTOREFRESH_STARTED", Map.of(
            "version_check_sec", String.valueOf(props().getVersionCheckSeconds()),
            "fp_min_interval_sec", String.valueOf(props().getFingerprintMinIntervalSeconds()),
            "whitelist", String.join(",", props().getWhitelistPrefixes())
        ));
    }

    public void changed(String oldVersion, String newVersion,
                       String oldFp, String newFp,
                       Set<String> hotKeys, Set<String> illegalKeys) {
        info("CONFIG_POLL_CHANGED", Map.of(
            "old_version", nz(oldVersion),
            "new_version", nz(newVersion),
            "old_fp", nz(oldFp),
            "new_fp", nz(newFp),
            "hot_keys", join(hotKeys),
            "illegal_keys", join(illegalKeys),
            "refreshed_count", String.valueOf(hotKeys.size())
        ));
    }

    public void noop(String oldFp, String newFp) {
        warn("CONFIG_POLL_NOOP", Map.of(
            "old_fp", nz(oldFp),
            "new_fp", nz(newFp),
            "reason", "refresh returned 0 hot keys; config may be non-hot-reloadable"
        ));
    }

    public void failed(String msg) {
        warn("CONFIG_POLL_FAILED", Map.of(
            "error", msg,
            "action", "next poll will retry with backoff"
        ));
    }

    public void illegalKeys(Set<String> illegal) {
        error("CONFIG_REFRESH_ILLEGAL_KEYS", Map.of(
            "illegal_keys", join(illegal),
            "action_required", "rollout restart deployment; hot refresh NOT safe"
        ));
    }

    public void effectiveKeys(Set<String> hot) {
        // 白名单内真正 rebind 的 key —— 运维据此判断 @Value 是否真生效
        info("CONFIG_REFRESH_EFFECTIVE", Map.of(
            "effective_keys", hot.isEmpty() ? "-" : join(hot),
            "note", "whitelisted keys actually rebound; verify @RefreshScope usage if value still old"
        ));
        ConfigAutoRefreshMetrics.INSTANCE.markEffectiveKeys(hot.size());
    }

    private void info(String event, Map<String, String> fields) { log(Level.INFO, event, fields); }
    private void warn(String event, Map<String, String> fields) { log(Level.WARN, event, fields); }
    private void error(String event, Map<String, String> fields) { log(Level.ERROR, event, fields); }

    private void log(Level level, String event, Map<String, String> fields) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("ts", Instant.now().toString());
        m.put("level", level.name());
        m.put("event", event);
        m.put("service", id.service());
        m.put("pod", id.pod());
        m.put("profile", profile);
        m.put("label", label);
        m.putAll(fields);
        try {
            log.log(level, mapper.writeValueAsString(m));
        } catch (JsonProcessingException e) {
            log.log(level, event + " " + fields);
        }
    }

    private String join(Set<String> s) { return s.isEmpty() ? "-" : String.join(",", s); }
    private String nz(String s) { return s == null ? "-" : s; }

    private ConfigAutoRefreshProperties props() { return null /* 通过构造传入 */; }
}
```

> 建议：生产环境改用 Logback 的 `JsonEncoder` / Logstash Encoder 输出原生 JSON。

### 6.9 指标

```java
public class ConfigAutoRefreshMetrics {
    public static final ConfigAutoRefreshMetrics INSTANCE = new ConfigAutoRefreshMetrics();

    private final Counter refreshes = Counter.builder("config.autorefresh.refreshes")
            .description("Successful auto-refreshes").register(Metrics.globalRegistry);
    private final Counter noops = Counter.builder("config.autorefresh.noops")
            .description("Refresh with 0 hot keys").register(Metrics.globalRegistry);
    private final Counter pollFailures = Counter.builder("config.autorefresh.poll_failures")
            .description("Config Server poll failures").register(Metrics.globalRegistry);
    private final Counter illegalKeys = Counter.builder("config.autorefresh.illegal_keys")
            .description("Non-whitelist keys detected").register(Metrics.globalRegistry);
    private final AtomicLong lastRefreshTs = new AtomicLong(0);

    static {
        Gauge.builder("config.autorefresh.last_refresh_ts",
                () -> (double) INSTANCE.lastRefreshTs.get())
                .description("Last successful refresh timestamp")
                .register(Metrics.globalRegistry);
    }

    public void markRefresh(int keyCount, String fp) {
        refreshes.increment();
        lastRefreshTs.set(System.currentTimeMillis());
    }
    public void markNoop() { noops.increment(); }
    public void markPollFailure() { pollFailures.increment(); }
    public void markIllegal(int n) { illegalKeys.increment(n); }
    public void markAuditViolation() {
        Metrics.globalRegistry.counter("config.autorefresh.audit_violations").increment();
    }
    public void markEffectiveKeys(int n) {
        Metrics.globalRegistry.counter("config.autorefresh.effective_keys").increment(n);
    }
}
```

### 6.10 Starter 自动装配注册

`src/main/resources/META-INF/spring/`:
```
org.springframework.boot.autoconfigure.AutoConfiguration.imports
```
内容：
```
com.example.config.autorefresh.ConfigAutoRefreshAutoConfiguration
```

---

## 7. 多服务接入方式

### 7.1 依赖

```xml
<dependency>
    <groupId>com.example</groupId>
    <artifactId>config-auto-refresh-spring-boot-starter</artifactId>
    <version>1.0.0</version>
</dependency>
```

### 7.2 每个服务的 application.yml

```yaml
spring:
  application:
    name: order-service          # 自动决定轮询路径
  config:
    import: configserver:http://config-server:8888

config:
  auto-refresh:
    enabled: true
    server: http://config-server:8888
    profile: prod
    label: main
    version-check-seconds: 5        # 第 1 层：版本号检查
    fingerprint-min-interval-seconds: 60  # 第 2 层：指纹最小间隔
    max-jitter-seconds: 10
    whitelist-prefixes:
      - app.
      - feature.
      - biz.
      - logging.level.

management:
  endpoints:
    web:
      exposure:
        include: refresh,health,info
```

### 7.3 热配置 Bean 的写法（决定 refresh 是否真的生效）

> ⚠️ **本章是全方案中最容易被误用的一章。**
> `RefreshEndpoint.refresh()` 只会重建 **`@RefreshScope` Bean**，普通 singleton Bean 里的 `@Value` 永远是启动值。
> 因此“日志显示已刷新，但代码读到的还是旧值”这一类工单，根因几乎都在此章。

#### 7.3.1 `@Value` 生效的条件矩阵

| 写法 | 是否生效 | 说明 |
|---|---|---|
| `@Value` + Bean 有 `@RefreshScope` | ✅ 生效 | 字段所在类必须标 `@RefreshScope` |
| `@ConfigurationProperties` + `@RefreshScope` | ✅ 生效（最稳） | Spring 原生 re-bind 支持 |
| `@Value` 在普通 `@Component`（无 `@RefreshScope`） | ❌ 永远不生效 | 单例 Bean 不会被重建 |
| `@Value` 注入到 `final` 字段 | ❌ 编译期就错 | 必须去掉 `final` |
| `@Value` 在构造器 / `@PostConstruct` 里拷贝到局部变量 | ❌ 快照，不更新 | 值被固化 |
| `@ConfigurationProperties` 字段在 `@PostConstruct` 快照 | ❌ 同上 | getter 读才安全 |

> 结论：**`refresh()` 刷不刷得到，取决于“Bean 是否 `@RefreshScope`”，与“谁触发的 refresh”无关**——无论是手动 `/actuator/refresh`、Bus 广播，还是本方案的 client polling，语义完全一致。

#### 7.3.2 推荐写法：`@ConfigurationProperties` + `@RefreshScope`

```java
@Component
@RefreshScope
@ConfigurationProperties(prefix = "app")
public class AppProps {
    private Map<String, String> features = new HashMap<>();
    private int qpsLimit = 100;
    private boolean newOrder;

    // 必须提供 setter，否则 re-bind 失败
    public void setQpsLimit(int qpsLimit) { this.qpsLimit = qpsLimit; }
    public void setNewOrder(boolean newOrder) { this.newOrder = newOrder; }
    public Map<String, String> getFeatures() { return features; }
    public int getQpsLimit() { return qpsLimit; }
    public boolean isNewOrder() { return newOrder; }
}
```

使用方（可以是普通 singleton）：

```java
@Service
public class OrderService {
    private final AppProps props;
    public OrderService(AppProps props) { this.props = props; }

    public void doSomething() {
        if (props.isNewOrder()) {   // ✅ 每次走代理读，永远最新
            // ...
        }
    }
}
```

> 关键点：`OrderService` 不需要 `@RefreshScope`——它每次方法调用都通过 `props` 代理读取，不会快照。

#### 7.3.3 允许写法：`@Value`（仅限 `@RefreshScope` Bean）

```java
@RestController
@RefreshScope
public class FeatureController {

    @Value("${app.feature.new-order:false}")
    private Boolean newOrder;   // 不能是 final

    @GetMapping("/feature")
    public boolean enabled() {
        return newOrder;        // ✅ 每次调用走代理，取最新实例
    }
}
```

#### 7.3.4 禁止写法（反模式清单）

```java
// ❌ 单例里快照 @Value
@Service
public class BadService {
    @Value("${app.feature.new-order}")
    private boolean newOrder;   // 启动后固化
}

// ❌ PostConstruct 拷贝快照
@Component
@RefreshScope
public class BadFlag {
    @Value("${app.feature.new-order}")
    private boolean raw;
    private boolean cached;      // 快照
    @PostConstruct
    public void init() { this.cached = raw; }   // 只执行一次
    public boolean isOn() { return cached; }     // 永远旧值
}

// ❌ final 字段
@Component
@RefreshScope
public class AlsoBad {
    @Value("${app.feature.new-order}")
    private final boolean newOrder;   // 无法被代理重写
}

// ❌ 注入到不可刷新对象（DataSource / Filter / 线程池）
@Configuration
public class DataSourceConfig {
    @Value("${spring.datasource.url}")   // 改了也不会重建连接池
    private String url;
}
```

#### 7.3.5 团队硬规则（纳入 Code Review / ADR）

1. **热配置只允许两种写法**：`@ConfigurationProperties + @RefreshScope`（首选），或 `@Value`（仅限 `@RefreshScope` Bean）。
2. **禁止**在以下位置固化热配置值：
   - 构造器参数
   - `@PostConstruct` / `@PreDestroy`
   - `static` / `final` 字段
   - 普通 `@Component` / `@Service` 的 `@Value` 字段
3. 白名单前缀（`app.*` / `feature.*` / `biz.*` / `logging.level.*`）之外的配置**一律不热刷**——即使写了 `@RefreshScope` 也无效，必须滚动重启。

---

### 7.4 启动期自检：`@RefreshScope` 使用规范扫描器

为避免“写了 `@Value` 却忘标 `@RefreshScope`”这类埋雷，Starter 在启动期扫描 BeanDefinition，命中违规即 `WARN` 并打印日志，提醒而非阻断（避免影响存量服务上线）。

```java
@Component
@Slf4j
public class RefreshScopeUsageAuditor implements ApplicationContextAware {

    private ApplicationContext ctx;

    @Override
    public void setApplicationContext(ApplicationContext ctx) {
        this.ctx = ctx;
    }

    @EventListener
    public void audit(ContextRefreshedEvent event) {
        ConfigurableApplicationContext cctx = (ConfigurableApplicationContext) ctx;
        ConfigurableListableBeanFactory bf = cctx.getBeanFactory();

        // 可热刷前缀（与白名单保持一致）
        var hotPrefixes = Set.of("app.", "feature.", "biz.", "logging.level.");

        for (String name : bf.getBeanDefinitionNames()) {
            BeanDefinition bd = bf.getBeanDefinition(name);
            if (bd.getScope().contains("refresh")) continue;          // 已是 refresh scope
            if (bd.getBeanClassName() == null) continue;
            Class<?> clazz = resolve(bd.getBeanClassName());
            if (clazz == null) continue;

            for (var f : clazz.getDeclaredFields()) {
                var av = f.getAnnotation(Value.class);
                if (av == null) continue;
                String expr = av.value();
                boolean isHot = hotPrefixes.stream()
                        .anyMatch(expr::contains);
                if (!isHot) continue;

                log.warn(
                    "[config-auto-refresh][AUDIT] 类 {} 的字段 {} 使用了热配置前缀 {}，" +
                    "但未标注 @RefreshScope，refresh 后该值不会更新。建议加上 @RefreshScope。",
                    clazz.getName(), f.getName(), expr);
                ConfigAutoRefreshMetrics.INSTANCE.markAuditViolation();
            }
        }
    }

    private Class<?> resolve(String cn) {
        try { return Class.forName(cn, false, ctx.getClassLoader()); }
        catch (Exception e) { return null; }
    }
}
```

> 说明：
> - 只扫描**已解析成 Class** 的 BeanDefinition，避免污染 BeanFactory；
> - 仅对白名单前缀告警，业务配置（如 `${app.x}`）才纳入；
> - 日志级别为 `WARN`，不抛异常，保证存量服务平滑接入。

#### 7.4.1 自检日志示例（运维侧）

```json
{
  "event": "CONFIG_REFRESH_USAGE_AUDIT",
  "service": "order-service",
  "pod": "order-service-7d4f8b9c6-xk2pz",
  "level": "WARN",
  "class": "com.example.OrderService",
  "field": "newOrder",
  "value": "${app.feature.new-order}",
  "message": "使用了热配置前缀但未标注 @RefreshScope，refresh 后不会更新"
}
```

在 Loki 中可一键汇总全集群违规：

```
{app=~".*"} | json | event="CONFIG_REFRESH_USAGE_AUDIT"
```

#### 7.4.2 升级建议（渐进式接入）

- **第 1 阶段**：仅输出 `WARN`，团队据此整改存量代码；
- **第 2 阶段**：整改完成后，将级别提升为 `ERROR` 并接入告警；
- **第 3 阶段**：在 CI 中加入 ArchUnit 规则，从源码层彻底拦截：

```java
@ArchTest
static final ArchRule value_fields_must_be_in_refresh_scope =
    fields().that().areAnnotatedWith(Value.class)
        .and().haveNameStartingWith("app")   // 按实际前缀调整
        .should().beDeclaredInClassesThat().areAnnotatedWith(RefreshScope.class);
```

---

## 8. Config Server 侧配合

### 8.1 暴露配置版本

```yaml
management:
  endpoints:
    web:
      exposure:
        include: info,health
  info:
    env:
      enabled: true
```

自定义 `InfoContributor` 暴露 Git commit / 时间戳作为 `config.version`：

```java
@Component
public class ConfigGitVersionInfoContributor implements InfoContributor {
    private final GitService gitService; // 你自己的 Git 封装
    @Override
    public void contribute(Info.Builder builder) {
        builder.withDetail("config",
            Map.of("version", gitService.currentCommitHash()));
    }
}
```

或简化：用 Config Server 本地文件时间戳 / `System.currentTimeMillis()` 作为版本。

### 8.2 Git 拉取频率

```yaml
spring:
  cloud:
    config:
      server:
        git:
          uri: https://git.internal/config-repo
          refresh-rate: 15   # 每 15 秒拉一次 Git
```

### 8.3 压力测算示例

假设：20 个服务 × 10 Pod = 200 Pod，版本检查 5 秒一次：

- 版本号请求：200 ÷ 5 = **40 QPS**（轻量 endpoint，可忽略）
- 假设每小时 Git 变更 6 次：
  - 指纹拉取：6 × 200 = 1200 次/小时 = **0.33 QPS**
- 若没有版本层（直接拉指纹，15 秒一次）：
  - 200 ÷ 15 = **13 QPS 持续**

> ✅ 三层降级把**高成本指纹拉取从持续流量变成事件驱动**，压力下降约 **40 倍**。

---

## 9. 可观测性与运维排障

### 9.1 关键事件清单

| event | 级别 | 触发时机 | 运维动作 |
|---|---|---|---|
| `CONFIG_AUTOREFRESH_STARTED` | INFO | starter 初始化 | 确认组件正常 |
| `CONFIG_POLL_CHANGED` | INFO | 版本+指纹都变，完成 refresh | 核心审计 |
| `CONFIG_POLL_NOOP` | WARN | 指纹变但 0 个热 key | 配置不在白名单 |
| `CONFIG_POLL_FAILED` | WARN | Config Server 不可达 | 检查 Config Server |
| `CONFIG_REFRESH_ILLEGAL_KEYS` | ERROR | 非白名单 key 变更 | 必须 rollout restart |
| `CONFIG_REFRESH_USAGE_AUDIT` | WARN | 启动期发现 `@Value` 热配置但未标 `@RefreshScope` | 整改代码，否则该值不会刷新 |
| `CONFIG_REFRESH_EFFECTIVE` | INFO | refresh 后真正生效的 key（白名单内） | 与 `CONFIG_POLL_CHANGED` 对照，判断 `@Value` 是否真的更新 |

### 9.2 常用排障命令

```bash
# 某服务最近一次变更
kubectl logs -l app=order-service --since=5m | grep CONFIG_POLL_CHANGED

# 是否刷到非法 key
kubectl logs -l app=order-service --since=10m | grep CONFIG_REFRESH_ILLEGAL_KEYS

# Config Server 是否在抖
kubectl logs -l app=order-service --since=5m | grep CONFIG_POLL_FAILED

# 所有副本是否收敛到同一指纹
kubectl logs -l app=order-service | grep new_fp | tail -20

# 🔍 “日志刷了但 @Value 还是旧值” 排障链路
# Step 1：本次刷新实际生效了哪些 key
kubectl logs -l app=order-service --since=10m | grep CONFIG_REFRESH_EFFECTIVE
# Step 2：目标 key 是否在白名单内（不在则必然不刷）
kubectl logs -l app=order-service --since=10m | grep CONFIG_POLL_CHANGED | grep "目标key前缀"
# Step 3：使用该 key 的 Bean 是否标了 @RefreshScope
kubectl logs -l app=order-service --since=30m | grep CONFIG_REFRESH_USAGE_AUDIT
# 结论：Step3 有命中 → 漏标 @RefreshScope；Step2 无命中 → 该 key 不在白名单，需重启
```

> 记忆口诀：**白名单 × `@RefreshScope` × 无快照 = 三件套**，缺一件即“假刷新”。

### 9.3 Loki 查询示例

```
{app="order-service"} | json | event=~"CONFIG_.*"
```

### 9.4 告警规则（Prometheus / Alertmanager）

```yaml
groups:
  - name: config-auto-refresh
    rules:
      - alert: ConfigAutoRefreshStale
        expr: time() - config_autorefresh_last_refresh_ts > 600
        for: 2m
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.service }} 超过10分钟未成功热刷"

      - alert: ConfigAutoRefreshIllegalKeys
        expr: increase(config_autorefresh_illegal_keys[5m]) > 0
        labels: { severity: page }
        annotations:
          summary: "检测到非白名单配置变更，需滚动重启 {{ $labels.service }}"

      - alert: ConfigAutoRefreshPollFailing
        expr: increase(config_autorefresh_poll_failures[5m]) > 10
        labels: { severity: warning }
        annotations:
          summary: "Config Server 拉取持续失败"

      - alert: ConfigAutoRefreshAuditViolation
        expr: increase(config_autorefresh_audit_violations[10m]) > 0
        labels: { severity: warning }
        annotations:
          summary: "{{ $labels.service }} 存在 @Value 热配置未标 @RefreshScope，refresh 后不会更新"
```

---

## 10. 风险与控制

| 风险 | 控制手段 |
|---|---|
| 刷新风暴 | 随机抖动 + 指纹最小间隔 + 退避 |
| Config Server 被打爆 | 三层降级（版本→指纹→refresh）+ 失败退避 |
| 非热刷 key 被误改 | `RefreshWhitelistGuard` + `CONFIG_REFRESH_ILLEGAL_KEYS` 告警 |
| 不一致窗口 | 版本 TTL + jitter，约 20~30s，最终一致 |
| 启动期错过 | 启动建 baseline，不触发假刷新 |
| 服务间指纹串扰 | 按 `spring.application.name` 隔离 URL |
| `@Value` 假刷新（写了 refresh 但读旧值） | 白名单 × `@RefreshScope` × 无快照 三件套；启动期 `RefreshScopeUsageAuditor` 扫描告警 |
| 漏标 `@RefreshScope` | `CONFIG_REFRESH_USAGE_AUDIT` WARN + `ConfigAutoRefreshAuditViolation` 告警 |

---

## 11. 边界声明

本方案是**“无 Webhook / 无 MQ / 无 ConfigMap”约束下的最优解**，但不是“推送式实时刷新”。适用边界：

- ✅ 可接受 20~30 秒的最终一致
- ✅ 热配置仅限于白名单（`app/feature/biz/logging.level`）
- ❌ 不满足毫秒级推送场景（需 Nacos / Apollo）
- ❌ `datasource.url` / `server.port` / SSL 等改了必须滚动重启，不可热刷
- ❌ 普通 singleton Bean 里的 `@Value` 不会被刷新——必须标 `@RefreshScope` 或使用 `@ConfigurationProperties`
- ❌ 在构造器 / `@PostConstruct` / `final` 字段中快照热配置值——刷新后仍读旧值

---

## 12. 交付清单

```
config-auto-refresh-spring-boot-starter/
├── ConfigAutoRefreshAutoConfiguration.java
├── ConfigAutoRefreshProperties.java
├── RuntimeIdentity.java
├── ConfigVersionProvider.java
├── ConfigServerVersionProvider.java
├── ConfigFingerprintProvider.java
├── ConfigServerFingerprintProvider.java
├── RefreshWhitelistGuard.java
├── ConfigPoller.java
├── ConfigRefreshLogger.java
├── ConfigAutoRefreshMetrics.java
├── RefreshScopeUsageAuditor.java          # 启动期 @RefreshScope 规范扫描
└── META-INF/spring/
    └── org.springframework.boot.autoconfigure.AutoConfiguration.imports
```

---

## 附录 A：版本号 vs 指纹 vs refresh 对照

| 层级 | 判断什么 | 成本 | 频率 | 变后动作 |
|---|---|---|---|---|
| 版本号 | “Config Server 端有没有可能更新” | 极小 | 高（5s） | 决定是否查指纹 |
| 指纹 | “这份配置内容到底变没变” | 中 | 只在版本变化时 | 决定是否 refresh |
| refresh | “Spring 环境里的 key 要不要重建” | 中高 | 只在指纹变化时 | 重建 @RefreshScope Bean |

## 附录 B：参数调优建议

| 场景 | version-check-seconds | fingerprint-min-interval-seconds |
|---|---|---|
| 小规模（<20 Pod） | 5 | 30 |
| 中规模（20~100 Pod） | 5 | 60 |
| 大规模（>100 Pod） | 10 | 120 |

> 原则：**版本检查要快，指纹拉取要省，refresh 只在必须时**。
