from dataclasses import dataclass, field

@dataclass
class StartTrainModelEvent:
    traceparent: str = field(default="")
    tracestate: str = field(default="")