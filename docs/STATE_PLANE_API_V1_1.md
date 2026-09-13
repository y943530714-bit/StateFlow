# State Plane HTTP API v1.1

当前参考实现使用标准库 HTTP Server，统一前缀为 `/v1/state-plane`。接口是内存实现的稳定语义边界，不代表生产部署必须使用 HTTP。

`StatePlaneHTTPClient` 实现 Semantic Adapter 所需的 writer/query 子集，供
`stateflow-adapter` 独立进程调用；它保留 source authority、TTL、CAS、
idempotency 和 write rejection，不把失败静默转换为成功。

可选 `stateflow.rpc` 使用同一 `StatePlaneAPI` 校验与响应语义，避免 HTTP/gRPC
实现分叉。`StatePlaneRpcRequest` 携带 canonical JSON payload/query，响应保留
HTTP-compatible status code；传输或参数错误使用标准 gRPC status。

## gRPC

`StatePlaneService` 覆盖 Southbound 写入、Snapshot/State/Graph/Metric/Schema
读取以及 server-streaming `WatchState`。Python client 同时实现 Semantic Adapter
所需 writer 接口，因此现有 Runtime/KV/Kubernetes/DCGM Adapter 无需修改。

```python
from stateflow.rpc import StatePlaneGRPCClient

with StatePlaneGRPCClient("127.0.0.1:50051") as client:
    status, snapshot = client.call(
        "GetSnapshot",
        payload={"entities": ["deployment/instance/i1"], "keys": ["instance.*"]},
    )
```

当前 gRPC server 使用 insecure channel；TLS、认证、租户授权、watch coalesce 和
持久 cursor 仍属于生产化工作。

## Southbound

| Method | Path | 对应 Contract |
| --- | --- | --- |
| POST | `/components/register` | `RegisterComponent` |
| POST | `/entities/upsert` | 注册 Component/Deployment graph entity |
| POST | `/state/publish` | `PublishState(batch)` |
| POST | `/metrics/publish` | `PublishMetrics(batch)` |
| POST | `/events/publish` | `PublishEvent(batch)`；`event_id` 幂等 |
| POST | `/relations/upsert` | `UpsertRelation(batch)` |
| POST | `/heartbeat` | Adapter/Component heartbeat 与 watermark |

批量接口接受 `{ "updates": [...] }`、`{ "samples": [...] }`、`{ "events": [...] }` 或 `{ "relations": [...] }`。State/Relation 写入支持 `expected_version`、`idempotency_key`、authority、TTL 和 provenance。

示例：

```bash
curl -s http://127.0.0.1:8080/v1/state-plane/entities/upsert \
  -H 'content-type: application/json' \
  -d '{"entities":[{"ref":"deployment/instance/i1","graph":"deployment","entity_type":"instance","lifecycle":"ready"}]}'

curl -s http://127.0.0.1:8080/v1/state-plane/state/publish \
  -H 'content-type: application/json' \
  -d '{"updates":[{"entity_ref":"deployment/instance/i1","key":"instance.ready","value":true,"producer":"runtime-adapter","authority":"STRUCTURED_LIFECYCLE","idempotency_key":"i1-ready-1"}]}'
```

## Northbound

| Method | Path | 说明 |
| --- | --- | --- |
| GET | `/state?entity_ref=...&keys=...` | 单实体 canonical state；支持 snapshot_token/max_age/min_authority |
| POST | `/state/batch-get` | 多实体热路径批量读；返回同一 snapshot_token |
| POST | `/snapshots` | 多实体、多 key 一致快照；返回 immutable token、missing/stale/completeness |
| GET | `/snapshots/{token}` | 重读已冻结快照 |
| POST | `/graph/query` | 按 roots/relation_types/depth 遍历两张图 |
| GET | `/changes?after_cursor=...` | 可恢复 state/relation/event delta |
| GET | `/metrics?entity_ref=...&keys=...&agg=...` | raw/avg/min/max/sum/count/last/p95 |
| GET | `/freshness` | State、Metric、Event、Adapter freshness 汇总 |
| GET | `/schema[?key=...]` | Canonical key discovery/alias resolution |
| GET | `/components` | 已注册组件 |
| GET | `/heartbeats` | Adapter watermark 与最后心跳 |

快照示例：

```bash
curl -s http://127.0.0.1:8080/v1/state-plane/snapshots \
  -H 'content-type: application/json' \
  -d '{
    "entities":["component/request/r1","component/component/runtime-a","deployment/instance/i1"],
    "keys":["request.*","runtime.*","instance.*"],
    "max_age_ms":{"request":5000,"runtime":200,"instance":1000},
    "min_authority":"DERIVED",
    "include_relations":true
  }'
```

## 兼容性与安全边界

- 旧 `/v1/state/events` 继续接收 `AgentStateEvent`，不与新 canonical event 接口混用。
- Gateway 自动投影 metadata、token 数、目标 Runtime/Instance 和关系，不投影 prompt/tool payload。
- State Plane 投影为 best-effort；写入或 adapter 故障不阻断模型请求。
- 当前服务没有认证、租户授权、请求大小限制或持久化，不能直接作为公网生产控制面。
