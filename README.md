# StateFlow

StateFlow 是 Agent 基础设施的轻量控制平面。Harness 可直接使用 OpenAI 兼容的请求入口；StateFlow 关联程序状态、选择目标，并把请求转发给已配置的后端。当前版本只实现 `route_model`：KV、Sandbox、工具和集群动作需要目标组件提供接口后再接入。

## 四个模块

| 模块 | 代码入口 | 负责什么 |
| --- | --- | --- |
| Unified State Interface | `stateflow/interface/` | 接收状态与执行反馈；校验并下发 Planner 选定的动作，记录下发结果。 |
| State Manager | `stateflow/state_manager/` | 保存程序事件与调度视图，管理带时效的目标实例状态。 |
| Planner | `stateflow/planner/` | 从状态、请求约束、策略和动作目录选择动作；不执行动作。 |
| Action Catalog | `stateflow/action_catalog/` | 声明动作的名称、目标和必填参数；不执行动作。 |

`stateflow/control_plane/` 只装配这四个模块。`stateflow/gateway/` 负责对外 HTTP 协议和请求代理；`stateflow/backend/` 负责调用后端；`stateflow/config.py` 读取部署配置。现有 `stateflow/scheduler/` 是 Planner 复用的路由策略实现；`stateflow/state/` 提供内存状态存储。旧版状态平面、预测、观测适配器、KV 实验代码仍保留供兼容使用，相关文档在 [`docs/legacy/`](docs/legacy/README.md)，不属于当前四模块接口。

状态和动作的关系：Harness/组件上报事件 → Interface → State Manager；Planner 从 State Manager、Action Catalog 与路由策略选择动作 → Interface 下发 → 后端执行并回报结果。请求转发是目前唯一可执行的下发路径。

## 启动

将 [`examples/local_gateway.json`](examples/local_gateway.json) 中的 `base_url` 和 `model_id` 替换为实际 OpenAI 兼容后端，然后运行：

```bash
python -m stateflow --config examples/local_gateway.json --host 127.0.0.1 --port 8080
```

Harness 将 OpenAI 兼容客户端的 base URL 指向 `http://127.0.0.1:8080/v1`。跨轮请求传入 `x-stateflow-program-id`；不传时每个请求视为独立程序。普通模型请求示例：

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' -H 'x-stateflow-program-id: run-1' \
  -d '{"model":"logical-agent","messages":[{"role":"user","content":"hello"}]}'
```

## 统一控制接口

| API | 行为 |
| --- | --- |
| `POST /v1/control/state` | 接受程序状态、执行反馈，或全局 `TARGET_UPDATED` 实例状态。 |
| `GET /v1/control/state?program_id=run-1` | 查询程序调度视图。 |
| `POST /v1/control/decisions` | 根据一次标准模型请求返回决策和声明动作；**不执行**。 |
| `GET /v1/control/actions` | 查询当前 Action Catalog。 |
| `POST /v1/chat/completions` | 决策、下发并转发到后端；支持 Chat SSE。 |

上报事件：

```bash
curl http://127.0.0.1:8080/v1/control/state \
  -H 'content-type: application/json' \
  -d '{"program_id":"run-1","event_type":"TOOL_FAILED","source":"harness","payload":{"error":"test failed"}}'
```

`/v1/responses`、`/v1/messages` 在目标后端直接支持相应协议时可透传普通请求，不进行协议翻译。可在配置中用 `protocols` 声明每个目标支持的协议。`TARGET_UPDATED` 上报 `model_id`、`endpoint_id`、`replica_id` 和 `healthy`、`available` 等状态，默认 30 秒后过期。状态、决策和去重记录目前仅存于进程内，重启会丢失；部署时在网络边界处理鉴权。

设计与边界见 [`docs/CONTROL_PLANE_V1.md`](docs/CONTROL_PLANE_V1.md)，代码对照见 [`docs/ARCHITECTURE.md`](docs/ARCHITECTURE.md)。运行测试：`python -m unittest discover -s tests -q`。
