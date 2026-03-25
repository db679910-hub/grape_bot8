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
BONUS_HOURS = 4
BONUS_AMOUNT = 50

logging.basicConfig(level=logging.INFO)
pool = None

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                last_collect INTEGER DEFAULT 0,
                last_bonus INTEGER DEFAULT 0
            )
        """)
    logging.info("✅ PostgreSQL подключена!")

async def get_user(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return row if row else None

async def add_user(user_id):
    async with pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO users (user_id) VALUES ($1) ON CONFLICT (user_id) DO NOTHING",
            user_id
        )

async def update_balance(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
            amount, user_id
        )

async def update_collect_time(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_collect = $1 WHERE user_id = $2",
            timestamp, user_id
        )

async def update_bonus_time(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_bonus = $1 WHERE user_id = $2",
            timestamp, user_id
        )

async def get_top_users(limit=10):
    async with pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT $1",
            limit
        )
        return rows

async def get_total_users():
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row['count']

async def get_total_grapes():
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT SUM(balance) FROM users")
        return row['sum'] if row['sum'] else 0

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(Command("start"))
async def cmd_start(message: Message):
    await add_user(message.from_user.id)
    await message.answer(
        f"🍇 **Привет, {message.from_user.first_name}!**\n\n"
        f"Собирай виноград и стань лучшим!\n\n"
        f"📋 **Команды:**\n"
        f"/сбор — собрать виноград (+5🍇)\n"
        f"/баланс — проверить баланс\n"
        f"/топ — рейтинг игроков\n"
        f"/бонус — получить бонус (+50🍇 каждые 4 часа)\n"
        f"/помощь — справка\n"
        f"/статистика — статистика бота"
    )

@dp.message(Command("сбор"))
async def cmd_collect(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    last_time = user['last_collect'] if user else 0
    
    if now - last_time < COOLDOWN_SECONDS:
        wait = COOLDOWN_SECONDS - (now - last_time)
        await message.answer(f"⏳ Виноград ещё растёт! Подожди {wait} сек")
        return
    
    await update_balance(user_id, GRAPE_REWARD)
    await update_collect_time(user_id, now)
    new_user = await get_user(user_id)
    await message.answer(
        f"🍇 +{GRAPE_REWARD} винограда!\n"
        f"Всего: {new_user['balance']}"
    )

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    user = await get_user(user_id)
    balance = user['balance'] if user else 0
    await message.answer(
        f"🍇 **Ваш баланс**\n\n"
        f"💰 Виноград: {balance} 🍇\n\n"
        f"Собирай больше командой /сбор"
    )

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    top_users = await get_top_users(10)
    
    if not top_users:
        await message.answer("📊 Пока нет игроков")
        return
    
    text = "🏆 **Топ игроков**\n\n"
    for i, row in enumerate(top_users, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        user_id = row['user_id']
        balance = row['balance']
        
        try:
            user = await bot.get_chat(user_id)
            name = user.first_name[:20]
        except:
            name = f"User{user_id}"
        
        text += f"{medal} {name} — {balance} 🍇\n"
    
    await message.answer(text)

@dp.message(Command("бонус"))
async def cmd_bonus(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    last_bonus = user['last_bonus'] if user else 0
    
    # 4 часа = 14400 секунд
    bonus_cooldown = BONUS_HOURS * 3600
    
    if now - last_bonus < bonus_cooldown:
        remaining = bonus_cooldown - (now - last_bonus)
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        seconds = remaining % 60
        await message.answer(
            f"⏰ Бонус уже получен!\n\n"
            f"Следующий бонус через:\n"
            f"{hours}ч {minutes}м {seconds}с"
        )
        return
    
    await update_balance(user_id, BONUS_AMOUNT)
    await update_bonus_time(user_id, now)
    new_user = await get_user(user_id)
    await message.answer(
        f"🎁 **Бонус получен!**\n\n"
        f"+{BONUS_AMOUNT} винограда 🍇\n"
        f"Всего: {new_user['balance']}\n\n"
        f"Следующий бонус через {BONUS_HOURS} часа!"
    )

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    await message.answer(
        "📚 **Справка по командам**\n\n"
        "🍇 **Основные:**\n"
        "/сбор — собрать виноград (+5🍇)\n"
        "⏱ Кулдаун: 60 секунд\n\n"
        "/баланс — проверить свой баланс\n\n"
        "/топ — рейтинг лучших игроков\n\n"
        "/бонус — получить бонус (+50🍇)\n"
        "⏱ Кулдаун: 4 часа\n\n"
        "/помощь — эта справка\n"
        "/статистика — статистика бота\n\n"
        "Удачи в сборе винограда! 🍇"
    )

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    total_users = await get_total_users()
    total_grapes = await get_total_grapes()
    
    await message.answer(
        f"📊 **Статистика бота**\n\n"
        f"👥 Игроков: {total_users}\n"
        f"🍇 Всего собрано: {total_grapes}\n"
        f"🎮 Бот работает с марта 2026"
    )

async def main():
    await init_db()
    logging.info("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
