# StateFlow Observability Bridge v1.1

## Goal

M3 建立 Runtime、KV Manager、Kubernetes/Ray 和 DCGM/NVML/NIC 到 State Plane
的统一语义边界。给定 `request_id`，StateFlow 可以从同一快照重建
`Request ↔ Runtime ↔ Instance ↔ Node ↔ KV ↔ Resource`，并为每个状态值保留
authority、timestamp、TTL、confidence 和 `source_ref`。

## Non-goals

- 不在 StateFlow 内重新采集或保存原始 Prometheus counter、trace、log、KV tensor。
- Adapter 不实现路由、迁移、驱逐、扩缩容等优化策略。
- 本阶段不引入生产级 watcher、持久存储、gRPC server 或闭环 action。
- 不改变现有 Gateway 和 Success-First Scheduler 的请求执行路径。

## Where

| 文件 | 职责 |
| --- | --- |
| `stateflow/adapters/identity.py` | 稳定 entity ref、跨 source identity binding、冲突拒绝 |
| `stateflow/adapters/runtime.py` | queue、TTFT/TPOT、吞吐、KV usage、health、执行关系 |
| `stateflow/adapters/kv.py` | KV location/size/replica/transfer/pin/cache-hit 与 request 关系 |
| `stateflow/adapters/kubernetes.py` | Cluster/Node/Instance、binding、allocatable、readiness |
| `stateflow/adapters/dcgm.py` | HBM、GPU utilization、node health、effective bandwidth 窗口 |
| `stateflow/adapters/bridge.py` | 共享 resolver，生成请求级图和 immutable snapshot |
| `stateflow/adapters/source.py` | 标准库 HTTP、Prometheus exposition 与 histogram quantile |
| `stateflow/adapters/clients.py` | vLLM、DCGM、Kubernetes、KV metadata source client |
| `stateflow/adapters/runner.py` | 请求热路径之外的失败隔离 polling 与退避 |

## Architecture

Source-specific client 负责读取 owner API、watch 或聚合后的 metric window，然后构造
transport-neutral observation。Semantic Adapter 只完成 identity/key/unit/authority/source
归一化，并调用现有 State Plane contract。Controller 只读取 materialized snapshot，
不会在决策热路径 fan-out 到数据源。

跨源关系使用现有受控 vocabulary：

| Source | Relation | Target |
| --- | --- | --- |
| Request | `executing_on` | Runtime 或 Instance |
| Request | `uses` | KV stateful object |
| Runtime | `deployed_on` | Instance |
| Cluster 或 Node | `contains` | Node、Instance 或 Resource |
| KV | `located_on` | Resource 或 Node |
| Node | `connected_to` | Peer Node |

## API

```python
from stateflow import InMemoryStatePlane, ObservabilityBridge
from stateflow.adapters import RuntimeObservation

plane = InMemoryStatePlane()
bridge = ObservabilityBridge(plane)
bridge.runtime.publish(
    RuntimeObservation(
        runtime_id="runtime-a",
        instance_id="replica-a",
        request_id="request-a",
        queue_depth=3,
        observation_id="runtime-watermark-42",
    )
)
view = bridge.materialize_request("request-a")
```

四类 Adapter 的 `publish()` 都返回 `AdapterPublishReport`。调用方可以检查
`accepted`、`rejected` 和逐条 `WriteResult`；重复 `observation_id` 会被 State Plane
幂等拒绝，不会重复修改状态。每次接收 observation 后都会更新 Adapter heartbeat
和 source watermark。

### Source clients

- `VLLMSourceClient` 读取 `/metrics`，映射 waiting/running、KV usage、TTFT p95
  和 TPOT p95。Metric 名称依据 vLLM 官方 production metrics 文档。
- `DCGMSourceClient` 读取 dcgm-exporter exposition，按 GPU 聚合 FB used、GPU
  utilization 和 XID health；FB used 从 MiB 规范化为 byte，utilization 从百分比
  规范化为 ratio。
- `KubernetesSourceClient` 读取 Node/Pod list API，将 `resourceVersion` 保留为
  watermark；Pod label `stateflow.io/runtime-id` 和 `stateflow.io/instance-id` 可覆盖
  默认 identity。
- `KVMetadataSourceClient` 读取 metadata-only sidecar JSON；KV tensor 和 prompt
  内容不会进入 StateFlow。
- `PollingAdapterRunner` 捕获 source 异常并执行有上限的指数退避；异常不进入
  Gateway 请求路径。

参考来源：

- https://docs.vllm.ai/en/latest/design/metrics/
- https://docs.nvidia.com/datacenter/dcgm/latest/gpu-telemetry/dcgm-exporter.html
- https://kubernetes.io/docs/reference/using-api/api-concepts/#efficient-detection-of-changes

## Algorithm

```text
receive source observation
  validate required source identity
  normalize external IDs to stable graph refs
  reject conflicting unique identity bindings
  upsert only source-owned entities
  publish decision-oriented canonical state with authority and source_ref
  upsert controlled relations with provenance attributes
  advance adapter heartbeat and watermark

materialize(request_id)
  resolve request_id to canonical Request
  traverse controlled relations to bounded depth
  request one immutable snapshot for all discovered entities and keys
  re-read graph relations from the same snapshot token
  return graph + state + freshness/missing metadata
```

## Tests

`tests/test_observability_bridge.py` 覆盖：

- Runtime、KV、Kubernetes、DCGM observation 联合物化完整关系链。
- Runtime 与 KV 状态保留真实 source backend/reference。
- 动态状态 TTL 到期后进入 `snapshot.stale`，不补默认值。
- Adapter heartbeat 单独按自身 TTL 标记 stale。
- 相同 observation ID 幂等，trace 可关联多个 request，唯一 request ID 冲突被拒绝。
- vLLM histogram/gauge、DCGM 多 GPU 与单位转换、Kubernetes Node/Pod
  resourceVersion、KV sidecar JSON 和 polling failure recovery。

## Acceptance

- [x] 给定 `request_id` 可查询 Request、Runtime、Instance、Node、KV 和 Resource。
- [x] 状态值携带 producer、authority、timestamp、TTL、confidence、source_ref。
- [x] stale/missing 显式传播，低 authority 不覆盖新鲜高 authority 状态。
- [x] Adapter 与现有 serving 热路径隔离，无新增第三方运行时依赖。
- [x] 全量单元测试通过。
- [x] vLLM/DCGM Prometheus、Kubernetes list、KV metadata HTTP client 与 fixture 测试。
- [ ] SGLang/Ray、原生 Mooncake/LMCache profile 与目标环境联调。
- [ ] 独立 Adapter 进程、gRPC transport、持久 watermark 与 backpressure。
