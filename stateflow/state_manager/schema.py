"""Core Agent State and scheduler hot-view data structures.

The module intentionally uses standard-library dataclasses.  StateFlow's
wire/API adapters can serialize these objects without coupling the scheduling
policy to a particular protobuf, HTTP framework, or inference engine.
"""

from __future__ import annotations

from dataclasses import dataclass, field, fields, is_dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
import math
from typing import Any, Generic, Mapping, TypeVar
import uuid


T = TypeVar("T")


def utcnow() -> datetime:
    """Return a timezone-aware UTC timestamp."""

    return datetime.now(timezone.utc)


def clamp(value: float, lower: float = 0.0, upper: float = 1.0) -> float:
    return max(lower, min(upper, float(value)))


class AgentPhase(str, Enum):
    CREATED = "CREATED"
    RUNNABLE = "RUNNABLE"
    CONTEXT_BUILDING = "CONTEXT_BUILDING"
    MODEL_QUEUED = "MODEL_QUEUED"
    MODEL_PREFILL = "MODEL_PREFILL"
    MODEL_DECODE = "MODEL_DECODE"
    PLANNING = "PLANNING"
    REPLANNING = "REPLANNING"
    TOOL_PREPARING = "TOOL_PREPARING"
    TOOL_QUEUED = "TOOL_QUEUED"
    TOOL_RUNNING = "TOOL_RUNNING"
    TOOL_WAITING = "TOOL_WAITING"
    TOOL_RESULT_READY = "TOOL_RESULT_READY"
    CONTEXT_UPDATING = "CONTEXT_UPDATING"
    CHECKPOINTING = "CHECKPOINTING"
    BLOCKED = "BLOCKED"
    PAUSED = "PAUSED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    CANCELLED = "CANCELLED"


class NextAction(str, Enum):
    MODEL = "MODEL"
    TOOL = "TOOL"
    MEMORY = "MEMORY"
    SUB_AGENT = "SUB_AGENT"
    TERMINATE = "TERMINATE"


class ToolStatus(str, Enum):
    PREPARING = "PREPARING"
    QUEUED = "QUEUED"
    RUNNING = "RUNNING"
    WAITING_IO = "WAITING_IO"
    RESULT_READY = "RESULT_READY"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"


class KVCacheTier(str, Enum):
    GPU_HBM = "GPU_HBM"
    CPU_DRAM = "CPU_DRAM"
    LOCAL_SSD = "LOCAL_SSD"
    REMOTE_DRAM = "REMOTE_DRAM"
    REMOTE_SSD = "REMOTE_SSD"
    OBJECT_STORAGE = "OBJECT_STORAGE"


class DecisionReason(str, Enum):
    CRITICAL_OVERRIDE = "CRITICAL_OVERRIDE"
    SUCCESS_DOMINANT = "SUCCESS_DOMINANT"
    COST_DOMINANT = "COST_DOMINANT"
    KV_AFFINITY = "KV_AFFINITY"
    LOAD_BALANCE = "LOAD_BALANCE"
    LATENCY_TIEBREAK = "LATENCY_TIEBREAK"
    HYSTERESIS_HOLD = "HYSTERESIS_HOLD"
    FALLBACK = "FALLBACK"


@dataclass
class StateField(Generic[T]):
    """Value plus freshness and provenance metadata from Agent State v0.1."""

    value: T
    observed_at: datetime = field(default_factory=utcnow)
    source: str = "unknown"
    confidence: float = 1.0
    ttl: timedelta | None = None
    authoritative: bool = False
    version: int = 0

    def is_fresh(self, now: datetime | None = None) -> bool:
        if self.ttl is None:
            return True
        now = now or utcnow()
        observed_at = self.observed_at
        if observed_at.tzinfo is None:
            observed_at = observed_at.replace(tzinfo=timezone.utc)
        if now.tzinfo is None:
            now = now.replace(tzinfo=timezone.utc)
        return now <= observed_at + self.ttl

    def effective_confidence(self, now: datetime | None = None) -> float:
        return clamp(self.confidence) if self.is_fresh(now) else 0.0


@dataclass
class StateHeader:
    schema_version: str = "0.1"
    state_id: str = field(default_factory=lambda: f"state-{uuid.uuid4().hex[:16]}")
    session_id: str = ""
    task_id: str = "default-task"
    turn_id: int = 0
    step_id: str = ""
    request_id: str = ""
    attempt_id: str = ""
    parent_step_id: str = ""
    trace_id: str = ""
    epoch: int = 0
    sequence_number: int = 0
    created_at: datetime = field(default_factory=utcnow)
    updated_at: datetime = field(default_factory=utcnow)
    update_source: str = "stateflow"
    dirty_fields: set[str] = field(default_factory=set)
    ttl: timedelta | None = None


@dataclass
class AgentIdentity:
    tenant_id: str = "default"
    namespace: str = "default"
    harness_type: str = "generic-proxy"
    harness_version: str = "0"
    agent_type: str = "unknown"
    agent_version: str = "0"
    workflow_type: str = "unknown"
    workload_class: str = "UNKNOWN"
    sandbox: str = ""
    workspace_id: str = ""
    required_capabilities: set[str] = field(default_factory=set)


@dataclass
class LifecycleState:
    phase: AgentPhase = AgentPhase.CREATED
    previous_phase: AgentPhase | None = None
    phase_enter_time: datetime = field(default_factory=utcnow)
    runnable: bool = False
    blocked_on: str = ""
    preemptible: bool = True
    resumable: bool = True
    progress: float = 0.0
    current_turn: int = 0
    current_step: str = ""
    active_compute_seconds: float = 0.0
    blocked_seconds: float = 0.0


@dataclass
class TaskState:
    task_type: str = "unknown"
    estimated_complexity: float = 0.5
    planning_mode: str = "REACTIVE"
    estimated_remaining_steps: int = 0
    estimated_remaining_model_calls: int = 0
    estimated_remaining_tool_calls: int = 0
    completion_probability: float = 0.0
    requires_long_context: bool = False
    requires_reasoning: bool = False
    requires_tool_use: bool = False
    requires_multimodal: bool = False


@dataclass
class ExecutionNode:
    node_id: str
    node_type: str = "MODEL"
    status: str = "PENDING"
    estimated_duration_seconds: float = 0.0
    critical: bool = False


@dataclass
class ExecutionState:
    nodes: list[ExecutionNode] = field(default_factory=list)
    edges: list[tuple[str, str]] = field(default_factory=list)
    runnable_nodes: list[str] = field(default_factory=list)
    blocked_nodes: list[str] = field(default_factory=list)
    current_node_id: str = ""
    critical_path_nodes: list[str] = field(default_factory=list)
    estimated_remaining_critical_path_seconds: float = 0.0


@dataclass
class ContextSegment:
    segment_id: str
    segment_type: str = "conversation"
    token_start: int = 0
    token_end: int = 0
    token_count: int = 0
    fingerprint: str = ""
    immutable: bool = False
    cacheable: bool = True
    producer_step_id: str = ""
    sensitivity_class: str = "default"


@dataclass
class ContextState:
    total_tokens: int = 0
    system_tokens: int = 0
    user_tokens: int = 0
    assistant_tokens: int = 0
    tool_tokens: int = 0
    memory_tokens: int = 0
    artifact_tokens: int = 0
    prompt_tokens: int = 0
    immutable_prefix_tokens: int = 0
    mutable_suffix_tokens: int = 0
    context_fingerprint: str = ""
    prefix_fingerprint: str = ""
    segments: list[ContextSegment] = field(default_factory=list)
    context_growth_tokens_per_turn: float = 0.0
    predicted_next_context_tokens: int = 0
    reusable_prefix_tokens: int = 0
    reusable_prefix_ratio: float = 0.0
    compaction_needed: bool = False
    truncation_risk: float = 0.0


@dataclass
class ModelState:
    logical_model: str = ""
    selected_model: str = ""
    selected_endpoint: str = ""
    selected_replica: str = ""
    prompt_tokens: int = 0
    predicted_output_tokens: int = 0
    adapter_id: str = ""
    tokenizer_id: str = ""
    model_revision: str = ""
    quantization: str = ""
    batch_compatibility_key: str = ""


@dataclass
class ToolExecution:
    tool_id: str
    tool_name: str
    tool_type: str = "unknown"
    status: ToolStatus = ToolStatus.PREPARING
    side_effecting: bool = False
    idempotent: bool = False
    retryable: bool = False
    parallelizable: bool = False
    elapsed_seconds: float = 0.0
    latency_p50_seconds: float = 0.0
    latency_p95_seconds: float = 0.0
    remaining_time_seconds: float = 0.0
    predicted_output_bytes: int = 0
    predicted_output_tokens: int = 0
    result_cacheable: bool = False
    result_cached: bool = False
    execution_location: str = ""


@dataclass
class ToolState:
    active_tools: list[ToolExecution] = field(default_factory=list)
    predicted_time_until_next_model_call_seconds: float = 0.0
    last_tool_name: str = ""


@dataclass
class InferenceEndpointState:
    endpoint_id: str
    replica_id: str = ""
    model_id: str = ""
    engine: str = "unknown"
    cluster: str = ""
    node: str = ""
    available: bool = True
    queue_position: int = 0
    queue_latency_seconds: float = 0.0
    batch_size: int = 0
    prefill_tokens_per_second: float = 0.0
    decode_tokens_per_second: float = 0.0
    gpu_memory_pressure: float = 0.0
    sm_pressure: float = 0.0
    bandwidth_pressure: float = 0.0


@dataclass
class InferenceState:
    engine: str = ""
    serving_mode: str = "co-located"
    endpoints: list[InferenceEndpointState] = field(default_factory=list)
    request_status: str = ""
    queue_position: int = 0
    batch_id: str = ""
    batch_size: int = 0
    prefilled_tokens: int = 0
    decoded_tokens: int = 0
    prefill_tokens_per_second: float = 0.0
    decode_tokens_per_second: float = 0.0
    ttft_seconds: float = 0.0
    tpot_seconds: float = 0.0


@dataclass
class KVSegment:
    token_start: int
    token_end: int
    layer_start: int = 0
    layer_end: int = 0
    fingerprint: str = ""
    bytes: int = 0
    tier: KVCacheTier = KVCacheTier.GPU_HBM
    replicas: list[str] = field(default_factory=list)
    reuse_probability: float = 0.0
    predicted_next_access: datetime | None = None
    recompute_cost: float = 0.0
    eviction_score: float = 0.0
    kv_dtype: str = ""
    compression_format: str = ""

    @property
    def token_count(self) -> int:
        return max(0, self.token_end - self.token_start)


@dataclass
class KVCacheState:
    context_fingerprint: str = ""
    model_id: str = ""
    segments: list[KVSegment] = field(default_factory=list)
    local_reusable_tokens: int = 0
    remote_reusable_tokens: int = 0
    hit_ratio: float = 0.0
    recompute_cost: float = 0.0
    fetch_cost: float = 0.0
    eviction_priority: float = 0.0
    pinned: bool = False
    preferred_replica: str = ""


@dataclass
class PredictionState:
    next_action_probabilities: dict[str, float] = field(default_factory=dict)
    continuation_probability: float = 0.0
    predicted_inter_request_gap_seconds: float = 0.0
    predicted_next_prompt_tokens: int = 0
    predicted_next_decode_tokens: int = 0
    predicted_context_growth: float = 0.0
    predicted_kv_reuse_ratio: float = 0.0
    predicted_tool_wait_seconds: float = 0.0
    predicted_remaining_turns: int = 0
    predictor_version: str = "heuristic-0.1"
    confidence: float = 0.0
    observed_at: datetime = field(default_factory=utcnow)
    ttl: timedelta | None = timedelta(seconds=30)


@dataclass
class RoutingHistory:
    current_tier: str = ""
    current_model: str = ""
    current_endpoint: str = ""
    current_replica: str = ""
    stable_steps: int = 0
    failure_streak: int = 0
    escalation_count: int = 0
    last_tier_change: datetime | None = None
    last_change_reason: str = ""


@dataclass
class SchedulingState:
    base_priority: float = 0.5
    dynamic_priority: float = 0.5
    session_sticky: bool = False
    preferred_endpoint: str = ""
    preferred_replica: str = ""
    allow_migration: bool = True
    allow_speculation: bool = False
    allow_prefetch: bool = True
    batch_compatibility_key: str = ""
    routing_history: RoutingHistory = field(default_factory=RoutingHistory)
    decision_history: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class QoSState:
    task_deadline: datetime | None = None
    turn_deadline: datetime | None = None
    step_deadline: datetime | None = None
    ttft_target_seconds: float | None = None
    tpot_target_seconds: float | None = None
    task_latency_target_seconds: float | None = None
    cost_per_token_budget: float | None = None
    task_cost_budget: float | None = None
    objective: str = "SUCCESS_COST_LATENCY"


@dataclass
class AffinityState:
    kv_affinity: str = ""
    model_affinity: str = ""
    tool_affinity: str = ""
    sandbox_affinity: str = ""
    data_affinity: str = ""
    preferred_region: str = ""
    preferred_zone: str = ""
    preferred_node: str = ""
    preferred_device: str = ""
    migration_penalty: float = 0.0


@dataclass
class FailureState:
    retry_count: int = 0
    failure_streak: int = 0
    failed_component: str = ""
    error_type: str = ""
    error_severity: float = 0.0
    retry_safe: bool = True
    fallback_models: list[str] = field(default_factory=list)
    fallback_tools: list[str] = field(default_factory=list)
    side_effect_committed: bool = False
    critical_error: bool = False


@dataclass
class ObservabilityState:
    queue_seconds: float = 0.0
    llm_seconds: float = 0.0
    tool_seconds: float = 0.0
    prefill_seconds: float = 0.0
    decode_seconds: float = 0.0
    kv_hit_tokens: int = 0
    kv_fetch_tokens: int = 0
    kv_miss_tokens: int = 0
    kv_transfer_bytes: int = 0
    estimated_cost: float = 0.0
    trace_id: str = ""


@dataclass
class SecurityState:
    tenant_id: str = "default"
    security_domain: str = "default"
    allowed_models: set[str] = field(default_factory=set)
    allowed_tools: set[str] = field(default_factory=set)
    allowed_regions: set[str] = field(default_factory=set)
    allowed_cache_domains: set[str] = field(default_factory=set)
    remote_kv_allowed: bool = True
    cache_shareability: str = "tenant"
    security_sensitive: bool = False


@dataclass
class AgentProgressSignals:
    exploring_score: float = 0.0
    production_score: float = 0.0
    error_severity: float = 0.0
    spinning_score: float = 0.0
    recovery_score: float = 0.0
    verification_score: float = 0.0
    plan_stability: float = 0.0
    progress_velocity: float = 0.0
    consecutive_failures: int = 0
    consecutive_no_progress_steps: int = 0


@dataclass
class StateAvailability:
    harness_native_state: bool = False
    tool_lifecycle: bool = False
    task_progress: bool = False
    execution_dag: bool = False
    context_state: bool = False
    inference_state: bool = False
    kv_state: bool = False
    state_completeness: float = 0.0


@dataclass
class TargetCandidate:
    """A target/replica description consumed by the scheduler.

    Cost fields are in the configured cost unit (USD by convention).  Latency
    fields are seconds.  The scheduler never mutates the target object; all
    computed values are emitted in CandidateEvaluation.
    """

    model_id: str
    endpoint_id: str
    replica_id: str
    tier: str = "efficient"
    capabilities: set[str] = field(default_factory=set)
    context_window_tokens: int = 128_000
    region: str = "default"
    security_domain: str = "default"
    cache_domain: str = "default"
    adapter_id: str = ""
    tokenizer_id: str = ""
    model_revision: str = ""
    quantization: str = ""
    backend_key: str = "default"
    available: bool = True
    healthy: bool = True
    supports_remote_kv: bool = True
    compatible: bool = True

    # Predictor prior.  A production predictor can replace these fields.
    base_success: float = 0.8
    base_uncertainty: float = 0.03

    # Current request and fallback cost.
    inference_cost: float = 0.0
    input_cost_per_token: float = 0.0
    output_cost_per_token: float = 0.0
    kv_fetch_cost_per_token: float = 0.0
    kv_recompute_cost_per_token: float = 0.0
    fallback_path_cost: float = 0.0
    routing_overhead_cost: float = 0.0

    # Replica/runtime state.
    queue_latency_seconds: float = 0.0
    kv_fetch_latency_per_token: float = 0.0
    kv_recompute_latency_per_token: float = 0.0
    prefill_tokens_per_second: float = 10_000.0
    decode_tokens_per_second: float = 100.0
    load_balance_score: float = 0.0
    local_kv_tokens: int = 0
    remote_kv_tokens: int = 0
    remote_kv_reuse_factor: float = 0.5

    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass
class AgentState:
    header: StateHeader = field(default_factory=StateHeader)
    identity: AgentIdentity = field(default_factory=AgentIdentity)
    lifecycle: LifecycleState = field(default_factory=LifecycleState)
    task: TaskState = field(default_factory=TaskState)
    execution: ExecutionState = field(default_factory=ExecutionState)
    context: ContextState = field(default_factory=ContextState)
    model: ModelState = field(default_factory=ModelState)
    tools: ToolState = field(default_factory=ToolState)
    inference: InferenceState = field(default_factory=InferenceState)
    kv_cache: KVCacheState = field(default_factory=KVCacheState)
    scheduling: SchedulingState = field(default_factory=SchedulingState)
    prediction: PredictionState = field(default_factory=PredictionState)
    qos: QoSState = field(default_factory=QoSState)
    affinity: AffinityState = field(default_factory=AffinityState)
    failure: FailureState = field(default_factory=FailureState)
    observability: ObservabilityState = field(default_factory=ObservabilityState)
    security: SecurityState = field(default_factory=SecurityState)
    progress_signals: AgentProgressSignals = field(default_factory=AgentProgressSignals)
    field_meta: dict[str, StateField[Any]] = field(default_factory=dict)
    extensions: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


@dataclass
class HarnessSchedulingView:
    """Small, materialized projection used by the request hot path."""

    session_id: str
    task_id: str = "default-task"
    step_id: str = ""
    state_id: str = ""
    state_version: int = 0
    tenant_id: str = "default"
    security_domain: str = "default"
    security_sensitive: bool = False
    session_turn: int = 0
    phase: AgentPhase = AgentPhase.CREATED
    runnable: bool = False
    logical_model: str = ""
    required_capabilities: set[str] = field(default_factory=set)
    allowed_models: set[str] = field(default_factory=set)
    allowed_regions: set[str] = field(default_factory=set)
    allowed_cache_domains: set[str] = field(default_factory=set)
    remote_kv_allowed: bool = True

    task_type: str = "unknown"
    task_progress: float = 0.0
    progress_velocity: float = 0.0
    exploring_score: float = 0.0
    production_score: float = 0.0
    error_severity: float = 0.0
    spinning_score: float = 0.0
    recovery_score: float = 0.0
    verification_score: float = 0.0
    plan_stability: float = 0.0
    consecutive_failures: int = 0
    consecutive_no_progress_steps: int = 0
    failure_streak: int = 0
    critical_error: bool = False
    critical_step: bool = False
    critical_path_urgency: float = 0.0
    context_compaction_needed: bool = False
    truncation_risk: float = 0.0

    prompt_tokens: int = 0
    predicted_output_tokens: int = 0
    reusable_kv_tokens: int = 0
    local_kv_tokens: int = 0
    remote_kv_tokens: int = 0
    predicted_time_until_next_model_call_seconds: float = 0.0
    continuation_probability: float = 0.0
    kv_reuse_probability: float = 0.0
    predicted_remaining_turns: int = 0
    next_action_probabilities: dict[str, float] = field(default_factory=dict)

    remaining_deadline_seconds: float | None = None
    cost_budget: float | None = None
    preferred_endpoint: str = ""
    preferred_replica: str = ""
    current_model: str = ""
    current_tier: str = ""
    stable_steps: int = 0
    migration_penalty: float = 0.0
    state_completeness: float = 0.0
    state_freshness: float = 0.0
    state_availability: StateAvailability = field(default_factory=StateAvailability)
    candidates: list[TargetCandidate] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return to_jsonable(self)


def to_jsonable(value: Any) -> Any:
    """Convert schema objects to JSON-compatible values without dependencies."""

    if isinstance(value, Enum):
        return value.value
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, timedelta):
        return value.total_seconds()
    if is_dataclass(value):
        return {item.name: to_jsonable(getattr(value, item.name)) for item in fields(value)}
    if isinstance(value, Mapping):
        return {str(key): to_jsonable(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [to_jsonable(item) for item in value]
    if isinstance(value, (set, frozenset)):
        return sorted(to_jsonable(item) for item in value)
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    return value


def new_agent_state(
    session_id: str,
    task_id: str = "default-task",
    *,
    tenant_id: str = "default",
    harness_type: str = "generic-proxy",
) -> AgentState:
    """Create a state with the minimum identity needed for a proxy request."""

    state = AgentState()
    state.header.session_id = session_id
    state.header.task_id = task_id
    state.identity.tenant_id = tenant_id
    state.identity.harness_type = harness_type
    state.security.tenant_id = tenant_id
    return state


def build_scheduling_view(state: AgentState, now: datetime | None = None) -> HarnessSchedulingView:
    """Materialize the scheduler hot view from a full AgentState snapshot."""

    now = now or utcnow()
    signals = state.progress_signals
    prediction = state.prediction
    lifecycle = state.lifecycle
    failure = state.failure
    context = state.context
    history = state.scheduling.routing_history

    deadline_seconds: float | None = None
    if state.qos.task_deadline is not None:
        deadline = state.qos.task_deadline
        if deadline.tzinfo is None:
            deadline = deadline.replace(tzinfo=timezone.utc)
        deadline_seconds = max(0.0, (deadline - now).total_seconds())

    critical_step = (
        state.execution.current_node_id in state.execution.critical_path_nodes
        if state.execution.current_node_id
        else bool(state.execution.critical_path_nodes and lifecycle.runnable)
    )
    urgency = 0.0
    if deadline_seconds is not None and state.qos.task_latency_target_seconds:
        urgency = clamp(1.0 - deadline_seconds / max(state.qos.task_latency_target_seconds, 1e-6))
    urgency = max(urgency, clamp(state.extensions.get("critical_path_urgency", 0.0)))
    if critical_step:
        urgency = max(urgency, 0.75)

    availability = _derive_availability(state, now)
    local_kv = state.kv_cache.local_reusable_tokens
    remote_kv = state.kv_cache.remote_reusable_tokens
    reusable = max(
        state.context.reusable_prefix_tokens,
        state.context.immutable_prefix_tokens,
        local_kv + remote_kv,
    )
    predicted_gap = max(
        0.0,
        prediction.predicted_inter_request_gap_seconds,
        state.tools.predicted_time_until_next_model_call_seconds,
        prediction.predicted_tool_wait_seconds,
    )

    return HarnessSchedulingView(
        session_id=state.header.session_id,
        task_id=state.header.task_id,
        step_id=state.header.step_id or lifecycle.current_step,
        state_id=state.header.state_id,
        state_version=state.header.sequence_number,
        tenant_id=state.identity.tenant_id,
        security_domain=state.security.security_domain,
        security_sensitive=state.security.security_sensitive,
        session_turn=max(state.header.turn_id, lifecycle.current_turn),
        phase=lifecycle.phase,
        runnable=lifecycle.runnable,
        logical_model=state.model.logical_model,
        required_capabilities=set(state.identity.required_capabilities),
        allowed_models=set(state.security.allowed_models),
        allowed_regions=set(state.security.allowed_regions),
        allowed_cache_domains=set(state.security.allowed_cache_domains),
        remote_kv_allowed=state.security.remote_kv_allowed,
        task_type=state.task.task_type,
        task_progress=clamp(lifecycle.progress),
        progress_velocity=signals.progress_velocity,
        exploring_score=clamp(signals.exploring_score),
        production_score=clamp(signals.production_score),
        error_severity=clamp(max(signals.error_severity, failure.error_severity)),
        spinning_score=clamp(signals.spinning_score),
        recovery_score=clamp(signals.recovery_score),
        verification_score=clamp(signals.verification_score),
        plan_stability=clamp(signals.plan_stability),
        consecutive_failures=signals.consecutive_failures,
        consecutive_no_progress_steps=signals.consecutive_no_progress_steps,
        failure_streak=max(failure.failure_streak, history.failure_streak),
        critical_error=failure.critical_error,
        critical_step=critical_step,
        critical_path_urgency=urgency,
        context_compaction_needed=context.compaction_needed,
        truncation_risk=clamp(context.truncation_risk),
        prompt_tokens=max(context.prompt_tokens, state.model.prompt_tokens, context.total_tokens),
        predicted_output_tokens=max(
            state.model.predicted_output_tokens,
            prediction.predicted_next_decode_tokens,
        ),
        reusable_kv_tokens=reusable,
        local_kv_tokens=local_kv,
        remote_kv_tokens=remote_kv,
        predicted_time_until_next_model_call_seconds=predicted_gap,
        continuation_probability=clamp(prediction.continuation_probability),
        kv_reuse_probability=clamp(
            max(prediction.predicted_kv_reuse_ratio, context.reusable_prefix_ratio)
        ),
        predicted_remaining_turns=max(0, prediction.predicted_remaining_turns),
        next_action_probabilities=dict(prediction.next_action_probabilities),
        remaining_deadline_seconds=deadline_seconds,
        cost_budget=state.qos.task_cost_budget,
        preferred_endpoint=state.scheduling.preferred_endpoint,
        preferred_replica=state.scheduling.preferred_replica or state.kv_cache.preferred_replica,
        current_model=history.current_model or state.model.selected_model,
        current_tier=history.current_tier,
        stable_steps=history.stable_steps,
        migration_penalty=max(0.0, state.affinity.migration_penalty),
        state_completeness=availability.state_completeness,
        state_freshness=_state_freshness(state, now),
        state_availability=availability,
    )


def _state_freshness(state: AgentState, now: datetime) -> float:
    if not state.field_meta:
        return 0.0
    values = [field.effective_confidence(now) for field in state.field_meta.values()]
    return sum(values) / len(values) if values else 0.0


def _derive_availability(state: AgentState, now: datetime) -> StateAvailability:
    """Derive L0/L1/L2 visibility while respecting field TTLs."""

    def available(name: str, fallback: bool = False) -> bool:
        candidates = [
            field
            for field_name, field in state.field_meta.items()
            if field_name == name or field_name.startswith(name + ".")
        ]
        if candidates:
            return any(
                field.is_fresh(now) and field.effective_confidence(now) > 0.0
                for field in candidates
            )
        return fallback

    native = state.identity.harness_type not in {"", "unknown", "generic-proxy"}
    tool = bool(state.tools.active_tools) or available("tools", False)
    task = state.lifecycle.progress > 0 or available("task.progress", False)
    dag = bool(state.execution.nodes or state.execution.critical_path_nodes) or available(
        "execution", False
    )
    context = bool(state.context.prompt_tokens or state.context.total_tokens) or available(
        "context", False
    )
    inference = bool(state.inference.endpoints) or available("inference", False)
    kv = bool(state.kv_cache.segments or state.kv_cache.local_reusable_tokens) or available(
        "kv_cache", False
    )
    flags = [native, tool, task, dag, context, inference, kv]
    completeness = sum(1 for flag in flags if flag) / len(flags)
    # Freshness of explicit dynamic metadata reduces availability; stale state
    # must not masquerade as a complete, safe observation.
    if state.field_meta:
        freshness = _state_freshness(state, now)
        completeness *= 0.5 + 0.5 * freshness
    return StateAvailability(
        harness_native_state=native,
        tool_lifecycle=tool,
        task_progress=task,
        execution_dag=dag,
        context_state=context,
        inference_state=inference,
        kv_state=kv,
        state_completeness=clamp(completeness),
    )
