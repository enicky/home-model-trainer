from typing import Optional

from pydantic import BaseModel, Field


class TraceableEvent(BaseModel):
    TraceParent: Optional[str] = Field("", alias="TraceParent")
    TraceState: Optional[str] = Field("", alias="TraceState")

class StartDownloadDataEvent(TraceableEvent):
    pass
