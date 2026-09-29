package com.github.xingjianluo.pipelineobservabilityservicejava.audit;

import java.util.List;

/** 判定记录的追加式日志。 */
public interface AuditLog {

    /** 追加一条判定记录并持久化。 */
    void append(DecisionRecord record);

    /** 查询某条流水线的判定记录（按时间先后）。 */
    List<DecisionRecord> findByPipeline(String pipelineName);

    /** 查询某次运行的判定记录。 */
    List<DecisionRecord> findByRun(String pipelineName, String runId);

    /** 全部判定记录。 */
    List<DecisionRecord> findAll();
}
