import asyncio
import logging
import time
import os
import asyncpg
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GRAPE_REWARD = 5
COOLDOWN_SECONDS = 60

logging.basicConfig(level=logging.INFO)

pool = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute('''
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                last_collect INTEGER DEFAULT 0
            )
        ''')
    logging.info("✅ PostgreSQL подключена!")

async def get_user(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow(
            'SELECT balance, last_collect FROM users WHERE user_id = $1',
            user_id
        )
        return (row['balance'], row['last_collect']) if row else (0, 0)

async def add_user(user_id):
    async with pool.acquire() as conn:
        await conn.execute(
            'INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING',
            user_id
        )

async def update_balance
  
