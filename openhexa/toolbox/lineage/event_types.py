from enum import Enum

class EventType(Enum):
    EMIT_RUN_START = "emit_run_start"
    EMIT_RUN_COMPLETE = "emit_run_complete"