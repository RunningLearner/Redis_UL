from fastapi import Depends, FastAPI
import redis.asyncio as aioredis
from contextlib import asynccontextmanager

from model.models import SetLikedModel, SetModel, SetScoreModel, SetTTLModel
from database.users import Base
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import text

# SQLite 비동기 엔진 생성
DATABASE_URL = "sqlite+aiosqlite:///./test.db"

# 비동기 SQLAlchemy 엔진 및 세션 설정
engine = create_async_engine(DATABASE_URL, echo=True)
AsyncSessionLocal = sessionmaker(
    autocommit=False, autoflush=False, bind=engine, class_=AsyncSession
)


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        # 테이블을 생성할 때 명시적으로 'bind=conn'을 전달
        await conn.run_sync(Base.metadata.create_all)

    # Redis 클라이언트 초기화
    redis = await aioredis.from_url("redis://localhost", decode_responses=True)
    app.state.redis = redis

    yield  # 여기에 있는 동안은 애플리케이션이 실행 중

    # 애플리케이션 종료 시 클라이언트 종료
    await redis.close()


# Redis 클라이언트를 의존성으로 주입할 함수
async def get_redis():
    return app.state.redis


# 의존성으로 SQLite 세션 주입
async def get_db():
    async with AsyncSessionLocal() as session:
        yield session


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
    if val == 0:
        return {"result": "존재하지 않는 키입니다."}
    return {"result": f"키 : {key} 를 제거하였습니다."}


@app.get("/search/{key}")
async def key(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.exists(key)
    if val == 0:
        return {"result": "존재하지 않는 키입니다."}
    return {"result": f"키 : {key} 가 존재합니다."}


@app.post("/expire")
async def increase_page_view(
    body: SetTTLModel, redis: aioredis.Redis = Depends(get_redis)
):
    val = await redis.setex(body.key, body.ttl, body.value)
    return {"message": f"Key '{body.key}' 의 수명이 {body.ttl} 초로 설정되었습니다."}


@app.get("/expire/{key}")
async def increase_page_view(key: str, redis: aioredis.Redis = Depends(get_redis)):
    val = await redis.ttl(key)
    if val == -2:
        return {"message": f"키 '{key}' 의 수명이 {val} 남아있지 않습니다."}
    return {"message": f"키 '{key}' 의 수명이 {val} 초 남았습니다."}


@app.post("/set_score")
async def set_score(body: SetScoreModel, redis: aioredis.Redis = Depends(get_redis)):
    # 이미 사용자 기록이 존재하면 누적, 없으면 기록
    new_score = await redis.zadd("score_board", {body.user_id: body.score}, incr=True)

    if new_score is not None:
        return {
            "message": f"사용자 '{body.user_id}'의 점수가 {new_score}로 기록되었습니다.",
            "score": new_score,
        }
    else:
        return {"message": f"사용자 '{body.user_id}'의 점수 기록에 실패했습니다."}


@app.get("/get_ranker/{n}")
async def get_ranker(n: int, redis: aioredis.Redis = Depends(get_redis)):
    # 상위 N명의 사용자 점수 조회
    top_users = await redis.zrevrange("score_board", 0, n - 1, withscores=True)

    # 사용자와 점수를 보기 쉽게 변환
    rank_list = [
        {"user_id": user.encode("utf-8"), "score": score} for user, score in top_users
    ]

    return {"message": f"상위 {n}명의 사용자와 점수입니다.", "rankers": rank_list}


@app.get("/get_user_rank/{userId}")
async def get_ranker(userId: str, redis: aioredis.Redis = Depends(get_redis)):
    # 상위 N명의 사용자 점수 조회
    user_rank = await redis.zrevrank("score_board", userId, withscore=True)
    rank, score = user_rank

    return {"message": f"사용자 '{userId}'의 순위는 {rank + 1}, 점수는 {score}입니다."}


# 사용자가 최근에 좋아요를 누른 tag (Redis와 DB 동기화)
# Write-Through 패턴
@app.put("/liked_tag_wt")
async def update_user(
    body: SetLikedModel,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    # Redis에 캐시 업데이트
    await redis.hset(f"user:{body.user_id}", mapping={"liked_tag": body.liked_tag})

    query = text("UPDATE users SET liked_tag = :liked_tag WHERE id = :user_id")
    # 데이터베이스에 동기화
    result = await db.execute(
        query, {"liked_tag": body.liked_tag, "user_id": body.user_id}
    )
    await db.commit()

    return {"message": f"{body.user_id}님이 좋아한 최근 태그 {body.liked_tag}"}


# 사용자가 최근에 좋아요를 누른 tag (Redis와 DB 동기화)
# Cache-Aside 패턴
@app.put("/liked_tag_ca")
async def update_user(
    body: SetLikedModel,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    # Redis에 캐시 업데이트
    await redis.hset(f"user:{body.user_id}", mapping={"liked_tag": body.liked_tag})

    query = text("UPDATE users SET liked_tag = :liked_tag WHERE id = :user_id")
    # 데이터베이스에 동기화
    result = await db.execute(
        query, {"liked_tag": body.liked_tag, "user_id": body.user_id}
    )
    await db.commit()

    return {"message": f"{body.user_id}님이 좋아한 최근 태그 {body.liked_tag}"}


# 사용자가 최근에 좋아요를 누른 tag (Redis와 DB 동기화)
# Write-Behind 패턴
@app.put("/liked_tag_ca")
async def update_user(
    body: SetLikedModel,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    # Redis에 캐시 업데이트
    await redis.hset(f"user:{body.user_id}", mapping={"liked_tag": body.liked_tag})

    query = text("UPDATE users SET liked_tag = :liked_tag WHERE id = :user_id")
    # 데이터베이스에 동기화
    result = await db.execute(
        query, {"liked_tag": body.liked_tag, "user_id": body.user_id}
    )
    await db.commit()

    return {"message": f"{body.user_id}님이 좋아한 최근 태그 {body.liked_tag}"}


# 사용자 데이터 조회 엔드포인트 (캐시 조회 및 데이터베이스 조회)
@app.get("/liked_tag/{user_id}")
async def get_user(
    user_id: str,
    db: AsyncSession = Depends(get_db),
    redis: aioredis.Redis = Depends(get_redis),
):
    # Redis에서 캐시된 사용자 데이터 조회
    cached_user = await redis.hgetall(f"user:{user_id}")

    if cached_user:
        return {"source": "Redis", "data": cached_user}

    # Redis에 데이터가 없다면, 데이터베이스에서 조회
    result = await db.execute(
        "SELECT id, name FROM users WHERE id = :user_id", {"user_id": user_id}
    )
    user = result.fetchone()

    if user:
        # 데이터베이스에서 찾은 후, Redis에 캐시
        await redis.hset(f"user:{user_id}", mapping={"name": user.name})
        return {"source": "Database", "data": {"id": user.id, "name": user.name}}

    return {"message": "User not found"}
