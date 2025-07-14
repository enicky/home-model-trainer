from datetime import datetime
from pydantic import BaseModel

class StartUploadModel(BaseModel):

    def __init__(self):
        super().__init__()
        self.model_path = ""
        self.trigger_moment = datetime.now()

    def __init__(self, model_path: str = "", trigger_moment: datetime = None):
        super().__init__()
        self.model_path = model_path
        self.trigger_moment = trigger_moment or datetime.now()
