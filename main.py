import asyncio
import logging
import sqlite3
import time
import os
from aiogram import Bot, Dispatcher
from aiogram.filters import Command
from aiogram.types import Message

BOT_TOKEN = os.getenv("BOT_TOKEN")
GRAPE_REWARD = 5
COOLDOWN_SECONDS = 60

logging.basicConfig(level=logging.INFO)

def init_db():
    conn = sqlite3.connect('grapes.db')
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id BIGINT PRIMARY KEY,
            balance INTEGER DEFAULT 0,
            last_collect INTEGER DEFAULT 0
        )
    ''')
    conn.commit()
    conn.close()
    logging.info("✅ База данных SQLite готова!")

def get_user(user_id):
    conn = sqlite3.connect('grapes.db')
    cursor = conn.cursor()
    cursor.execute('SELECT balance, last_collect FROM users WHERE user_id = ?', (user_id,))
    result = cursor.fetchone()
    conn.close()
    return result if result else (0, 0)

def add_user(user_id):
    conn = sqlite3.connect('grapes.db')
    cursor = conn.cursor()
    cursor.execute('INSERT OR IGNORE INTO users (user_id) VALUES (?)', (user_id,))
    conn.commit()
    conn.close()

def update_balance(user_id, amount):
    conn = sqlite3.connect('grapes.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET balance = balance + ? WHERE user_id = ?', (amount, user_id))
    conn.commit()
    conn.close()

def update_time(user_id, timestamp):
    conn = sqlite3.connect('grapes.db')
    cursor = conn.cursor()
    cursor.execute('UPDATE users SET last_collect = ? WHERE user_id = ?', (timestamp, user_id))
    conn.commit()
    conn.close()

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    add_user(message.from_user.id)
    await message.answer(
        f"🍇 Привет, {message.from_user.first_name}!\n\n"
        f"Собирай виноград:\n"
        f"/сбор — собрать (+{GRAPE_REWARD}🍇)\n"
        f"/баланс — проверить"
    )

@dp.message(Command("сбор", "collect"))
async def cmd_collect(message: Message):
    user_id = message.from_user.id
    now = int(time.time())
    balance, last_time = get_user(user_id)
    
    if now - last_time < COOLDOWN_SECONDS:
        wait = COOLDOWN_SECONDS - (now - last_time)
        await message.answer(f"⏳ Подожди {wait} сек")
        return
    
    update_balance(user_id, GRAPE_REWARD)
    update_time(user_id, now)
    await message.answer(
        f"🍇 +{GRAPE_REWARD} винограда!\n"
        f"Всего: {balance + GRAPE_REWARD}"
    )

@dp.message(Command("баланс", "balance"))
async def cmd_balance(message: Message):
    balance, _ = get_user(message.from_user.id)
    await message.answer(f"🍇 **Ваш баланс**: {balance} 🍇")

async def main():
    init_db()
    logging.info("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
