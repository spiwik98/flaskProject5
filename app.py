from fastapi import FastAPI, HTTPException, Depends, Request
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy import Column, Integer, String, Text
from pydantic import BaseModel
from celery import Celery
from fastapi.templating import Jinja2Templates
from fastapi.staticfiles import StaticFiles

# FastAPI aplikacja
fastapi_app = FastAPI()
templates = Jinja2Templates(directory="templates")

DATABASE_URL = "mysql+aiomysql://root:root123@localhost:3306/message"
engine = create_async_engine(DATABASE_URL, echo=True)

async_session = sessionmaker(
    bind=engine,
    class_=AsyncSession,
    expire_on_commit=False
)

Base = declarative_base()

class Message(Base):
    __tablename__ = "messages"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100))
    subject = Column(String(100))
    message = Column(Text)

# Celery konfiguracja
celery = Celery(
    __name__,
    broker="redis://localhost:6379/0",
    backend="redis://localhost:6379/0"
)

@celery.task
def save_message_task(name: str, subject: str, message: str):
    import asyncio
    from sqlalchemy.ext.asyncio import create_async_engine
    from sqlalchemy.orm import sessionmaker

    DATABASE_URL = "mysql+aiomysql://root:root123@localhost:3306/message"
    engine = create_async_engine(DATABASE_URL, echo=True)
    async_session = sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False
    )

    async def save_message():
        async with async_session() as session:
            new_message = Message(name=name, subject=subject, message=message)
            session.add(new_message)
            await session.commit()
            await session.refresh(new_message)
        await engine.dispose()

    asyncio.run(save_message())

class MessageIn(BaseModel):
    name: str
    subject: str
    message: str

async def get_db():
    async with async_session() as session:
        yield session

@fastapi_app.post("/submit_sync_message/")
async def submit_sync_message(message: MessageIn, db: AsyncSession = Depends(get_db)):
    db_message = Message(**message.dict())
    db.add(db_message)
    await db.commit()
    await db.refresh(db_message)
    return db_message

@fastapi_app.post("/submit_async_message/")
async def submit_async_message(message: MessageIn):
    save_message_task.delay(message.name, message.subject, message.message)
    return {"message": "Your message will be processed asynchronously."}

@fastapi_app.get("/")
async def index(request: Request):
    return templates.TemplateResponse("index2.html", {"request": request})

@fastapi_app.get("/async_form")
async def show_async_form(request: Request):
    return templates.TemplateResponse("async_form.html", {"request": request})

@fastapi_app.on_event("startup")
async def on_startup():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

fastapi_app.mount("/static", StaticFiles(directory="static"), name="static")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(fastapi_app, host="127.0.0.1", port=8000, log_level="info")
