# StateFlow

StateFlow 是面向 Agent 基础设施的状态与动作控制平面。Harness 可以把 OpenAI 兼容的模型请求发到同一个地址，由 StateFlow 选择目标模型和实例，并转发至配置的推理后端。V1 只实现 `route_model`；KV、Sandbox 和工具动作需要相应组件提供执行接口后再接入。

## 代码结构

`stateflow/` 下只有四个功能模块：

| 目录 | 职责 |
| --- | --- |
| `interface/` | 对外 HTTP、Harness 请求标准化、后端代理；接收状态和执行反馈，校验并下发选中的动作。 |
| `state_manager/` | 程序事件、调度视图、目标实例状态及其有效期。 |
| `planner/` | 从状态、请求约束、路由策略和动作目录选择目标，不执行动作。 |
| `action_catalog/` | 声明动作、目标类型和必需参数，不执行动作。 |

`__main__.py` 仅作为启动入口。`docs/` 提供 [架构代码对照](docs/ARCHITECTURE.md) 和 [V1 设计说明](docs/CONTROL_PLANE_V1.md)。

## 接入

在 [`examples/local_gateway.json`](examples/local_gateway.json) 中配置 OpenAI 兼容后端的 `base_url` 和实际提供的 `model_id`，然后启动：

```bash
python -m stateflow --config examples/local_gateway.json --host 127.0.0.1 --port 8080
```

Harness 把模型客户端的 base URL 设为 `http://127.0.0.1:8080/v1`。需要跨轮共享程序状态时传 `x-stateflow-program-id`；未提供时每个请求独立处理。

```bash
curl http://127.0.0.1:8080/v1/chat/completions \
  -H 'content-type: application/json' -H 'x-stateflow-program-id: run-1' \
  -d '{"model":"logical-agent","messages":[{"role":"user","content":"hello"}]}'
```

## 统一控制接口

| API | 行为 |
| --- | --- |
| `POST /v1/control/state` | 接收程序状态、执行反馈或全局 `TARGET_UPDATED` 实例状态。 |
| `GET /v1/control/state?program_id=run-1` | 查询程序调度视图。 |
| `POST /v1/control/decisions` | 返回路由决策和动作，不执行。 |
| `GET /v1/control/actions` | 查询支持的动作。 |
| `POST /v1/chat/completions` | 决策、下发、代理模型请求；支持 Chat SSE。 |

例如 Harness 上报一次工具失败：

```bash
curl http://127.0.0.1:8080/v1/control/state \
  -H 'content-type: application/json' \
  -d '{"program_id":"run-1","event_type":"TOOL_FAILED","source":"harness","payload":{"error":"test failed"}}'
```

`/v1/responses` 与 `/v1/messages` 仅在目标后端支持同一协议时透传普通请求，不做协议翻译。状态、决策和动作去重目前均保存在内存中，重启后丢失；对外部署需在网络边界配置鉴权。

运行测试：`python -m unittest discover -s tests -q`。
