from fastapi import Depends, FastAPI
import redis.asyncio as aioredis
from contextlib import asynccontextmanager

from model.models import SetModel, SetTTLModel

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Redis 클라이언트 초기화
    redis = await aioredis.from_url("redis://localhost", decode_responses=True)
    app.state.redis = redis
    
    yield  # 여기에 있는 동안은 애플리케이션이 실행 중

    # 애플리케이션 종료 시 클라이언트 종료
    await redis.close()

# Redis 클라이언트를 의존성으로 주입할 함수
async def get_redis():
    return app.state.redis

app = FastAPI(lifespan=lifespan)
# 이 시점에는 아직 app 이 초기화 되지 않아 오류가 남
# 내부적으로는 비동기적으로 이것저것을 설정하기 때문 
# redis = app.state.redis

@app.get("/get/{key}")
async def get_value(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.get(key)
    return {key: val}

@app.post("/set")
async def set_value(body: SetModel, redis: aioredis.Redis = Depends(get_redis)):
    await redis.set(body.key, body.value)
    val = await redis.get("my-key")
    return {"message": "값 입력 성공!"}

@app.post("/visit")
async def increase_page_view(redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.incr("visit")
    return {"message": "페이지를 방문했습니다!"}

@app.get("/visit")
async def get_page_view(redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.get("visit")
    return {"visit_views": val}

@app.delete("/{key}")
async def delete_key(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.delete(key)
    if val == 0: return {"result": "존재하지 않는 키입니다."}
    return {"result": f"키 : {key} 를 제거하였습니다."}

@app.get("/search/{key}")
async def key(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.exists(key)
    if val == 0: return {"result": "존재하지 않는 키입니다."}
    return {"result": f"키 : {key} 가 존재합니다."}

@app.post("/expire")
async def increase_page_view(body: SetTTLModel, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.setex(body.key, body.ttl, body.value)
    return {"message": f"Key '{body.key}' 의 수명이 {body.ttl} 초로 설정되었습니다."}

@app.get("/expire/{key}")
async def increase_page_view(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.ttl(key)
    if val == -2: return {"message": f"키 '{key}' 의 수명이 {val} 남아있지 않습니다."}
    return {"message": f"키 '{key}' 의 수명이 {val} 초 남았습니다."}