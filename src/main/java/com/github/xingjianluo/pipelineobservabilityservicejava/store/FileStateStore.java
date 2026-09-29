package com.github.xingjianluo.pipelineobservabilityservicejava.store;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertState;
import com.github.xingjianluo.pipelineobservabilityservicejava.alert.AlertType;
import com.github.xingjianluo.pipelineobservabilityservicejava.audit.DecisionRecord;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineConfig;
import com.github.xingjianluo.pipelineobservabilityservicejava.state.PipelineState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 基于本地 JSON 文件的持久化实现。每个域一个文件，原子替换写盘。
 * 进程重启后重新 load，结论与重启前一致。
 */
@Component
public class FileStateStore implements StateStore {

    private static final Logger log = LoggerFactory.getLogger(FileStateStore.class);

    private final Path dir;
    private final ObjectMapper mapper;
    private final Map<String, Object> fileLocks = new ConcurrentHashMap<>();

    public FileStateStore(com.github.xingjianluo.pipelineobservabilityservicejava.config.ObservabilityProperties props) {
        this.dir = Path.of(props.getStorageDir());
        this.mapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .enable(SerializationFeature.INDENT_OUTPUT);
        try {
            Files.createDirectories(dir);
        } catch (IOException e) {
            throw new UncheckedIOException("无法创建状态目录: " + dir, e);
        }
    }

    private Path file(String name) {
        return dir.resolve(name);
    }

    private Object lock(String name) {
        return fileLocks.computeIfAbsent(name, k -> new Object());
    }

    private <T> T read(String name, Class<T> type, T empty) {
        Path p = file(name);
        if (!Files.exists(p)) {
            return empty;
        }
        try {
            synchronized (lock(name)) {
                if (!Files.exists(p)) {
                    return empty;
                }
                return mapper.readValue(Files.readAllBytes(p), type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取状态文件失败: " + p, e);
        }
    }

    private <T> T readRef(String name, TypeReference<T> type, T empty) {
        Path p = file(name);
        if (!Files.exists(p)) {
            return empty;
        }
        try {
            synchronized (lock(name)) {
                if (!Files.exists(p)) {
                    return empty;
                }
                return mapper.readValue(Files.readAllBytes(p), type);
            }
        } catch (IOException e) {
            throw new UncheckedIOException("读取状态文件失败: " + p, e);
        }
    }

    private void writeAtomically(String name, Object value) {
        Path target = file(name);
        synchronized (lock(name)) {
            try {
                Path tmp = Files.createTempFile(dir, name, ".tmp");
                Files.write(tmp, mapper.writeValueAsBytes(value));
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
            } catch (IOException e) {
                throw new UncheckedIOException("写入状态文件失败: " + target, e);
            }
        }
    }

    @Override
    public Map<String, PipelineState> loadPipelineStates() {
        Map<String, PipelineState> v = readRef("pipelines.json",
                new TypeReference<LinkedHashMap<String, PipelineState>>() {
                }, new LinkedHashMap<>());
        log.info("加载流水线状态: {} 条", v.size());
        return v;
    }

    @Override
    public synchronized void savePipelineState(PipelineState state) {
        Map<String, PipelineState> all = loadPipelineStates();
        all.put(state.getPipelineName(), state);
        writeAtomically("pipelines.json", all);
    }

    @Override
    public Map<String, Map<AlertType, AlertState>> loadAlertStates() {
        Map<String, Map<AlertType, AlertState>> raw = readRef("alerts.json",
                new TypeReference<Map<String, Map<AlertType, AlertState>>>() {
                }, new LinkedHashMap<>());
        return raw;
    }

    @Override
    public synchronized void saveAlertState(String pipelineName, AlertState alertState) {
        Map<String, Map<AlertType, AlertState>> all = loadAlertStates();
        all.computeIfAbsent(pipelineName, k -> new LinkedHashMap<>()).put(alertState.getType(), alertState);
        writeAtomically("alerts.json", all);
    }

    @Override
    public List<com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert> loadAlerts() {
        return readRef("alert-history.json",
                new TypeReference<ArrayList<com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert>>() {
                }, new ArrayList<>());
    }

    @Override
    public synchronized void appendAlert(com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert alert) {
        List<com.github.xingjianluo.pipelineobservabilityservicejava.alert.Alert> all = loadAlerts();
        all.add(alert);
        writeAtomically("alert-history.json", all);
    }

    @Override
    public Map<String, PipelineConfig> loadConfigs() {        return readRef("configs.json",
                new TypeReference<LinkedHashMap<String, PipelineConfig>>() {
                }, new LinkedHashMap<>());
    }

    @Override
    public synchronized void saveConfig(PipelineConfig config) {
        Map<String, PipelineConfig> all = loadConfigs();
        all.put(config.getPipelineName(), config);
        writeAtomically("configs.json", all);
    }

    @Override
    public List<DecisionRecord> loadDecisions() {
        List<DecisionRecord> v = readRef("decisions.json",
                new TypeReference<ArrayList<DecisionRecord>>() {
                }, new ArrayList<>());
        return v;
    }

    @Override
    public synchronized void appendDecision(DecisionRecord record) {
        List<DecisionRecord> all = loadDecisions();
        all.add(record);
        writeAtomically("decisions.json", all);
    }
}
