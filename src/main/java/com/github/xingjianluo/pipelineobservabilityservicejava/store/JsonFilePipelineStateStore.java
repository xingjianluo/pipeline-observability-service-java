package com.github.xingjianluo.pipelineobservabilityservicejava.store;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties;
import com.github.xingjianluo.pipelineobservabilityservicejava.dto.RejectedEvent;
import com.github.xingjianluo.pipelineobservabilityservicejava.model.PipelineState;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Function;
import java.util.stream.Stream;

/**
 * 基于 JSON 文件的持久化存储。
 *
 * <ul>
 *   <li>每条流水线一个 JSON 文件，更新走"临时文件 + 原子 move"，崩溃也不会写出半截状态；</li>
 *   <li>每条流水线一把锁，读-改-写在锁内完成，并发写同一条流水线不丢更新、不互相覆盖；</li>
 *   <li>启动时从磁盘全量加载，重启后结论与重启前一致；</li>
 *   <li>被拒事件追加到 JSONL 文件，同样在重启后可查。</li>
 * </ul>
 */
@Component
public class JsonFilePipelineStateStore implements PipelineStateStore {

    private static final Logger log = LoggerFactory.getLogger(JsonFilePipelineStateStore.class);

    private final ObservabilityProperties properties;
    private final ObjectMapper mapper = new ObjectMapper();
    private final ConcurrentHashMap<String, PipelineState> pipelines = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<String, Object> locks = new ConcurrentHashMap<>();
    private final Object rejectedLock = new Object();
    private final List<RejectedEvent> rejectedEvents = new ArrayList<>();

    private Path pipelinesDir;
    private Path rejectedFile;

    @org.springframework.beans.factory.annotation.Autowired
    public JsonFilePipelineStateStore(ObservabilityProperties properties) {
        this.properties = properties;
    }

    /** 测试/多实例用：直接指定持久化目录（跳过 Spring 属性绑定）。 */
    public JsonFilePipelineStateStore(String storageDir) {
        ObservabilityProperties props = new ObservabilityProperties();
        props.setStorageDir(storageDir);
        this.properties = props;
        try {
            init();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to init store at " + storageDir, e);
        }
    }

    @PostConstruct
    public void init() throws IOException {
        Path base = Paths.get(properties.getStorageDir());
        pipelinesDir = base.resolve("pipelines");
        rejectedFile = base.resolve("rejected-events.jsonl");
        Files.createDirectories(pipelinesDir);
        loadAll();
        loadRejected();
        log.info("PipelineStateStore initialized at {} ({} pipelines, {} rejected events)",
                base.toAbsolutePath(), pipelines.size(), rejectedEvents.size());
    }

    @Override
    public PipelineState compute(String pipelineName, Function<PipelineState, PipelineState> updater) {
        Object lock = locks.computeIfAbsent(pipelineName, k -> new Object());
        synchronized (lock) {
            // 二次确认：拿到锁后重新读内存，避免使用进入方法前捕获的旧引用
            PipelineState current = pipelines.get(pipelineName);
            if (current == null) {
                current = loadFromDisk(pipelineName);
            }
            if (current == null) {
                current = new PipelineState();
                current.setPipelineName(pipelineName);
            }
            PipelineState next = updater.apply(current);
            if (next == null) {
                next = current;
            }
            if (next.getPipelineName() == null) {
                next.setPipelineName(pipelineName);
            }
            persist(pipelineName, next);
            // putIfAbsent 语义下仍以本次锁内计算结果为准（同一 pipeline 的更新全程串行）
            pipelines.put(pipelineName, next);
            return next;
        }
    }

    @Override
    public <T> T readWithLock(String pipelineName, java.util.function.Function<PipelineState, T> reader) {
        Object lock = locks.computeIfAbsent(pipelineName, k -> new Object());
        synchronized (lock) {
            PipelineState current = pipelines.get(pipelineName);
            if (current == null) {
                current = loadFromDisk(pipelineName);
            }
            if (current == null) {
                PipelineState empty = new PipelineState();
                empty.setPipelineName(pipelineName);
                return reader.apply(empty);
            }
            return reader.apply(current);
        }
    }

    @Override
    public PipelineState get(String pipelineName) {
        Object lock = locks.computeIfAbsent(pipelineName, k -> new Object());
        synchronized (lock) {
            PipelineState current = pipelines.get(pipelineName);
            if (current != null) {
                return current;
            }
            // 缓存未命中才读磁盘；读到后仅在"内存仍为空"时放入，避免磁盘旧快照覆盖更新写入
            current = loadFromDisk(pipelineName);
            if (current != null) {
                pipelines.putIfAbsent(pipelineName, current);
                current = pipelines.get(pipelineName);
            }
            return current;
        }
    }

    @Override
    public List<PipelineState> findAll() {
        return new ArrayList<>(pipelines.values());
    }

    @Override
    public List<String> findAllNames() {
        return new ArrayList<>(pipelines.keySet());
    }

    @Override
    public void appendRejected(RejectedEvent rejectedEvent) {
        synchronized (rejectedLock) {
            try {
                String line = mapper.writeValueAsString(rejectedEvent);
                Files.writeString(rejectedFile, line + System.lineSeparator(),
                        StandardOpenOption.CREATE, StandardOpenOption.APPEND);
            } catch (IOException e) {
                // 落盘失败不能让事件被悄悄丢掉：至少保留在内存并打错误日志
                log.error("Failed to persist rejected event {}", rejectedEvent.getEventId(), e);
            }
            rejectedEvents.add(rejectedEvent);
        }
    }

    @Override
    public List<RejectedEvent> findRejected() {
        synchronized (rejectedLock) {
            return new ArrayList<>(rejectedEvents);
        }
    }

    private void loadAll() {
        try (Stream<Path> files = Files.list(pipelinesDir)) {
            files.filter(p -> p.toString().endsWith(".json")).forEach(path -> {
                String encoded = path.getFileName().toString().replace(".json", "");
                String name = URLDecoder.decode(encoded, StandardCharsets.UTF_8);
                PipelineState loaded = readFile(path, PipelineState.class);
                if (loaded != null) {
                    if (loaded.getPipelineName() == null) {
                        loaded.setPipelineName(name);
                    }
                    pipelines.put(name, loaded);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load pipeline states from " + pipelinesDir, e);
        }
    }

    private void loadRejected() {
        if (!Files.exists(rejectedFile)) {
            return;
        }
        try {
            for (String line : Files.readAllLines(rejectedFile, StandardCharsets.UTF_8)) {
                if (line.isBlank()) {
                    continue;
                }
                RejectedEvent event = mapper.readValue(line, RejectedEvent.class);
                rejectedEvents.add(event);
            }
        } catch (IOException e) {
            log.error("Failed to load rejected events from {}", rejectedFile, e);
        }
    }

    private PipelineState loadFromDisk(String pipelineName) {
        Path path = pipelinesDir.resolve(encode(pipelineName) + ".json");
        if (!Files.exists(path)) {
            return null;
        }
        PipelineState state = readFile(path, PipelineState.class);
        if (state != null && state.getPipelineName() == null) {
            state.setPipelineName(pipelineName);
        }
        return state;
    }

    private <T> T readFile(Path path, Class<T> type) {
        try {
            return mapper.readValue(path.toFile(), type);
        } catch (IOException e) {
            log.error("Corrupt state file ignored: {}", path, e);
            return null;
        }
    }

    private void persist(String pipelineName, PipelineState state) {
        Path target = pipelinesDir.resolve(encode(pipelineName) + ".json");
        Path tmp = pipelinesDir.resolve(encode(pipelineName) + ".json.tmp");
        try {
            byte[] bytes = mapper.writerWithDefaultPrettyPrinter().writeValueAsBytes(state);
            Files.write(tmp, bytes, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to persist state for pipeline " + pipelineName, e);
        }
    }

    private static String encode(String name) {
        return URLEncoder.encode(name, StandardCharsets.UTF_8).replace("+", "%20");
    }
}
