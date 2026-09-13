# StateFlow v1.1 开发计划

依据《StateFlow Architecture & System Design · Final v1.1》，StateFlow 的长期核心应收敛为：

`State Snapshot → Candidate-Action Prediction → Decision → Action → New State → Feedback`

本计划以增量兼容为原则：保留现有 AgentState、Success-First Scheduler、Gateway 和 Backend Adapter，在其旁边建立通用 State/Prediction/Control 契约，再逐步把现有热路径迁移到新契约。

## 1. 当前差距矩阵

| v1.1 能力 | 改造前状态 | 本轮状态 | 后续工作 |
| --- | --- | --- | --- |
| Canonical Key / Schema Registry | 无；字段绑定 AgentState | 已实现首批 key、alias、TTL、单位、语义 | schema negotiation、deprecation、持久化 |
| Source Authority / Provenance | 仅 `authoritative: bool` | 已实现 A0-A4、source_ref、confidence、冲突拒绝 | 多源合并策略、adapter health |
| Component State Graph | 无通用图 | 已实现实体与受控关系索引；Runtime/KV 语义 Adapter 可自动物化 | 对接真实 Runtime/KV client |
| Deployment State Graph | Candidate 中有扁平位置字段 | 已实现 Cluster/Node/Instance/Resource/Link；K8s/DCGM 语义 Adapter 可物化 | 对接 K8s/Ray/DCGM watcher |
| Cross-graph Relation | 无 | 已限制为 deployed_on/executing_on/located_on | 一致性校验与生命周期回收 |
| Snapshot / Query | AgentState 深拷贝 | 已实现不可变 token、freshness、completeness、显式 missing/stale | 批量 RPC、分布式 logical time |
| Subscribe | AgentState callback | 已实现可恢复 cursor change feed 与 callback | backpressure、coalesce、持久游标 |
| Candidate-Action Prediction | 分散在 Scheduler 内 | 已实现通用四维契约、路由转换桥和 shadow analytical service | journal/replay、校准 |
| Decision / Action / Feedback | RoutingDecision 与状态历史 | 已实现通用记录契约 | Action Adapter、Outcome join、Feedback ledger |
| Reservation / Commit | 无 | 已定义 Reservation 契约 | 原子 reserve/commit/release 与冲突指标 |
| Agent-aware KV | 仅路由成本中的 KV value | 未闭环 | KV candidate、transfer model、Mooncake/LMCache adapter |
| Cross-layer Routing | Agent + TargetCandidate 扁平输入 | 已有 Success-First baseline；可导出 v1.1 bundle | 改为同一 State Snapshot、可配置 PolicyIntent |

## 2. 分阶段路线

### Phase 0 — Observability Bridge

目标：一个 Agent request 可重建 `Request ↔ Runtime Instance ↔ Node ↔ KV` 基本关系，且每个状态值可追溯来源和 freshness。

- Harness/Agent adapter：生命周期、phase、deadline、expected resume time。
- Runtime adapter：queue、TTFT/TPOT、prefill/decode、KV usage、health。
- KV adapter：location/tier/replica/size/transfer/pin/eviction。
- Deployment adapter：K8s/Ray instance/node binding、capacity、readiness。
- Hardware adapter：DCGM/NVML/NIC 的窗口聚合，发布 HBM pressure、effective bandwidth 等决策状态。
- Correlation Resolver：统一 trace/session/request/action/component/instance ID。

验收：给定 request_id，可查询完整关系链；每个 key 返回 producer、authority、version、age、stale 和 source_ref。

### Phase 1 — Read-only StateFlow

目标：控制器只通过 StateFlow 读取一致状态，不再在每次决策时 fan-out 到各 owner。

- [x] Canonical Key Registry 和 adapter alias。
- [x] A0-A4 Source Authority、TTL、CAS、idempotency。
- [x] Component/Deployment 两张图和受控跨图关系。
- [x] 不可变 Snapshot Token、missing/stale/completeness。
- [x] Graph query、change cursor 和进程内 subscription。
- [x] HTTP Southbound API：RegisterComponent/PublishState/PublishMetrics/PublishEvent/UpsertRelation/Heartbeat。
- [x] HTTP Northbound API：GetState/GetSnapshot/QueryGraph/Change Cursor/QueryMetrics/GetFreshness/ListSchema。
- [x] Gateway Adapter：自动投影 Request、候选 Runtime/Instance 和实际 executing_on 关系。
- [x] 独立 Adapter 进程与 HTTP Southbound writer。
- [x] 可选 protobuf/gRPC API、Python client、Semantic Adapter writer 与基础
  server-streaming WatchState。
- [x] Runtime/KV/Kubernetes/DCGM 协议无关语义 Adapter 与共享 Correlation Resolver。
- [x] `Request↔Runtime↔Instance↔Node↔KV↔Resource` 请求级关系物化。
- [x] vLLM/SGLang/Ray Serve/DCGM Prometheus、Kubernetes list-watch、KV metadata
  sidecar source client。
- [x] Source timeout/error 隔离、内容 watermark、幂等 observation 与 polling backoff。
- [x] 声明式 Runtime metric profile、Mooncake/LMCache JSON profile、Kubernetes
  Node/Pod 双游标与 410 relist recovery（fixture 验证）。
- [ ] 目标环境版本联调与 profile 差异校验。
- [ ] WatchState coalesce、持久 cursor 与显式队列指标；当前 gRPC stream 具备
  cursor 恢复与 transport backpressure。
- [ ] 持久 Hot Store、Relation Index 和 adapter watermark。

验收：同一候选集共享 snapshot_token；过期或缺失状态不静默补默认值；与 owner API 对照具备一致性报告；关闭 StateFlow 不影响现有 serving。

### Phase 2 — Prediction MVP

目标：把路由/KV 所需预测从 Controller 中抽离，先建立可解释 baseline。

- [x] performance/reliability/cost/future_state + confidence 通用契约。
- [x] 现有 Success-First CandidateEvaluation 转换桥。
- [x] Transfer：`bytes / effective_bw + setup`。
- [x] Routing latency：queue + KV transfer/recompute + prefill + decode。
- [x] HBM pressure：used + reserved + predicted KV growth。
- [x] Deterministic cost accounting。
- [x] applicability、feature freshness、model version 和 fallback 输出。
- [x] In-memory Prediction journal，以及按 snapshot/model version replay。
- [x] calibration sample、MAE/MAPE/coverage/Brier/ECE 聚合报表。

验收：shadow 预测生成 MAE/MAPE/calibration/coverage 报表；低置信度自动退回当前 heuristic；所有预测可按 snapshot/model version 重放。

### Phase 3 — Shadow Decision + Feedback

目标：生成完整决策记录但不执行新动作，先证明潜在收益和可回放性。

- [x] PolicyIntent、Decision、Action、Outcome、Feedback 数据契约。
- [x] RoutingDecision → candidate predictions + DecisionRecord 转换。
- [x] In-memory Decision/Prediction/Outcome journal 与 action_id join。
- [x] interactive/critical/batch 可配置 lexicographic replay policy。
- [x] baseline 与候选策略 side-effect-free controlled replay。
- [x] KPI、prediction error 和 calibration sample materializer。

验收：生产请求无副作用；每个 decision 能重放候选、约束、预测和选择原因；可量化相对 baseline 的 potential gain。

### Phase 4 — P0 Closed-loop

先分开上线两个 Controller，避免一次耦合多个控制域。

1. Agent-aware KV
   - 状态：agent phase/expected next use、KV location/size、HBM pressure、effective BW。
   - 动作：keep/offload/prefetch/migrate；由 Mooncake/LMCache owner 执行。
   - KPI：HBM reclaimed、resume latency、KV reuse、program E2E、transfer bytes。

2. Cross-layer Routing
   - 状态：Agent + Runtime + KV + Instance/Node/Link，全部来自同一 snapshot。
   - 动作：route/admit/fallback；由 Scheduler/Runtime owner 执行。
   - KPI：p50/p95/p99、SLO success、P(success)、cost、KV transfer、OOM/preemption。

控制安全必须先于闭环开启：resource/action ownership、reservation/commit、hysteresis/cooldown、shadow/dry-run、timeout/rollback、stale-decision 拒绝。

### Phase 5 — P1/P2 Expansion

- P1 Program-aware Priority。
- P1 Predictive Prefill/Decode deployment。
- P2 Locality-aware deployment。
- P2 Topology-aware MoE placement。

每个 Controller 独立定义 owner、时间尺度、baseline、KPI 和 rollback，不构建“大一统智能控制器”。

## 3. 推荐迭代拆分

| 迭代 | 可交付物 | 退出条件 |
| --- | --- | --- |
| M1（本轮） | v1.1 contracts、in-memory State Plane、routing bridge、测试、文档 | 全量回归通过；旧 API 兼容 |
| M2（已完成） | HTTP/gRPC Southbound/Northbound API；Gateway Request/Runtime adapter；独立 Adapter writer | HTTP/gRPC 互操作测试通过；已有进程内 Snapshot 延迟基线 |
| M3（进行中） | 语义 Adapter、完整两图物化、Runtime/DCGM/K8s/KV profile、list-watch 与进程化已完成；待目标环境联调 | Request↔Instance↔Node↔KV 可重建；真实数据源联调通过 |
| M4（核心完成） | Analytical Prediction service + in-memory shadow journal/replay/evaluation | 预测误差、coverage 与 calibration 可观测；待持久化和生产流量接入 |
| M5 | Agent-aware KV dry-run → controlled closed-loop | 安全指标达标且相对 baseline 有增量 |
| M6 | Cross-layer Routing shadow → controlled closed-loop | SLO/成本/KV/OOM 指标达标且可回退 |

当前本地基准（非生产 SLO）：7 个实体、10 个 StateValue、4 条关系的内存快照，每轮 2000 次、连续 3 轮得到 p50 0.90–0.92 ms、p95 1.27–1.50 ms、p99 1.77–3.05 ms。使用 `python -m benchmarks.state_plane_snapshot` 可复测；分布式存储和 RPC 延迟需在后续环境单独测量。

## 4. 工程约束

- State Plane 只管理控制所需事实投影，不复制原始 trace/log 或高频硬件 counter。
- State 与 Prediction 严格分离；预测值不能覆盖 authoritative current state。
- Adapter 只做协议、身份和语义转换，不实现优化策略。
- Controller 提出 Decision，domain owner 保留执行权。
- 所有跨组件比较必须共享 snapshot_token、policy_version 和 model_version。
- 缺失、过期、低 authority、低 confidence 必须显式传播并触发拒绝或 baseline fallback。
