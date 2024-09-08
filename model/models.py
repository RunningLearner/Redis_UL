# Pydantic 모델을 사용하여 요청 바디를 정의
from pydantic import BaseModel


class SetModel(BaseModel):
    key: str
    value: str

    
class SetTTLModel(BaseModel):
    key: str
    value: str
    ttl: int
    
class SetScoreModel(BaseModel):
    user_id: str
    score: str