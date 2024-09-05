from fastapi import FastAPI
import redis.asyncio as aioredis
from contextlib import asynccontextmanager

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Redis 클라이언트 초기화
    redis = await aioredis.create_redis_pool("redis://localhost")
    app.state.redis = redis
    
    yield  # 여기에 있는 동안은 애플리케이션이 실행 중

    # 애플리케이션 종료 시 클라이언트 종료
    redis.close()
    await redis.wait_closed()

app = FastAPI(lifespan=lifespan)

app = FastAPI()

@app.get("/")
async def read_root():
    redis = app.state.redis
    await redis.set("my-key", "value")
    val = await redis.get("my-key", encoding="utf-8")
    return {"my-key": val}

@app.get("/items/{item_id}")
async def read_item(item_id: int):
    redis = app.state.redis
    await redis.set(f"item-{item_id}", f"Item {item_id}")
    item_value = await redis.get(f"item-{item_id}", encoding="utf-8")
    return {"item_id": item_id, "value": item_value}