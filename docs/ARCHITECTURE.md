# StateFlow V1：四模块代码对照

| 模块 | 核心文件 | 输入与输出 |
| --- | --- | --- |
| Unified State Interface | `stateflow/interface/service.py`, `gateway.py`, `http.py` | 接收 Harness 和组件状态；下发动作到适配器，将执行反馈写回状态。`request.py` 处理不同请求格式，`backend.py` 定义后端适配器边界。 |
| State Manager | `stateflow/state_manager/service.py`, `store.py`, `targets.py` | 从事件生成程序调度视图，维护带 TTL 的实例状态。 |
| Planner | `stateflow/planner/service.py`, `scheduler.py` | 根据请求约束、状态、Action Catalog 和 SuccessFirst 策略生成路由动作；不调用后端。 |
| Action Catalog | `stateflow/action_catalog/service.py` | 声明 `route_model` 的目标类型与必填参数，并校验动作。 |

## 一次请求的闭环

1. `interface/request.py` 将 Harness 请求归一化，并关联 `program_id`。
2. Interface 接收请求事件；State Manager 保存事件并提供调度视图。
3. Planner 应用请求硬约束，从可用目标中选择模型与实例，生成 `route_model` 动作。
4. Interface 校验并下发动作；Gateway 通过 OpenAI 兼容后端适配器代理请求，成功或失败作为状态事件回流。

`POST /v1/control/decisions` 仅执行第 1–3 步，供自行执行动作的外部组件调用。当前只有模型路由闭环具备组件适配器；新增动作需要同时定义其目录项、规划规则与下发接口。状态、决策和去重记录目前仅存于单进程内存。
