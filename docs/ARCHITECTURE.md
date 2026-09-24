# 第一版代码结构

## 请求如何通过四个模块

1. `gateway.normalizer` 把 Harness 的请求转换为 `ProviderNeutralRequest`，并以 `program_id` 关联程序。
2. `interface.UnifiedStateInterface.ingest()` 把请求事件和组件反馈交给 `state_manager.StateManager`。State Manager 包装既有的内存事件存储；目标的健康覆盖值带 TTL。
3. `planner.Planner.plan_route()` 从 State Manager 获取一次调度视图，叠加本次请求的硬约束，只考虑 Action Catalog 声明的 `route_model`，复用 `scheduler` 中的 SuccessFirst 策略生成 `RoutingDecision` 和 `Action`。Planner 不调用后端。
4. `interface.UnifiedStateInterface.dispatch()` 校验动作、在进程内按决策 ID 去重，调用 Gateway 传入的后端适配器，并将 `ACTION_DISPATCHED` / `ACTION_SUCCEEDED` / `ACTION_FAILED` 当作事件写回 State Manager。流式转发在消费结束后记录反馈。

`control_plane.ControlPlane` 是上述模块的装配入口。`gateway.RequestGateway` 负责 HTTP 代理所需的协议筛选、请求和响应处理，不能定义动作与策略。`backend` 的适配器在 Gateway 触发后执行请求；其接口不会依赖具体的 Harness。

## 边界与现状

- `route_model` 的下发由 Gateway 向目标后端发请求；`/v1/control/decisions` 只返回动作，外部组件可以自行执行。没有 KV、Sandbox、工具或集群动作的执行适配器。
- `stateflow/state/` 中的 `InMemoryStateStore` 是 V1 状态实现；其中的旧状态平面 API 与 `adapters/`、`control/`、`prediction/`、`rpc/`、`harness/` 保留作为既有实验能力，不通过四模块的对外合同暴露。历史说明放在 [`legacy/`](legacy/README.md)。
- 状态和动作去重使用进程内内存。部署为多个副本、保障重启后的回放或认证均需要额外实现。

外部调用只需要 [`README.md`](../README.md) 中的 Gateway 与统一控制 API；扩展新动作时先在 Action Catalog 定义，再接入 Planner 与目标组件的下发适配器，并让执行反馈通过 Interface 回流。
