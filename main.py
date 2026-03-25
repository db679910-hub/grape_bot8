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
DAILY_BONUS = 50

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
                last_daily INTEGER DEFAULT 0
            )
        """)
    logging.info("✅ PostgreSQL connected!")

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

async def update_daily_time(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET last_daily = $1 WHERE user_id = $2",
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

# 🎬 START
@dp.message(Command("start"))
async def cmd_start(message: Message):
    await add_user(message.from_user.id)
    await message.answer(
        f"🍇 **Hello, {message.from_user.first_name}!**\n\n"
        f"Collect grapes and become the best!\n\n"
        f"📋 **Commands:**\n"
        f"/collect — collect grapes (+5🍇)\n"
        f"/balance — check your balance\n"
        f"/top — player rating\n"
        f"/daily — daily bonus (+50🍇)\n"
        f"/help — command list\n\n"
        f"🇷🇺 **Русский:**\n"
        f"/сбор — собрать виноград\n"
        f"/баланс — проверить баланс\n"
        f"/топ — рейтинг\n"
        f"/ежедневно — бонус\n"
        f"/помощь — справка"
    )

# 🍇 COLLECT
@dp.message(Command("сбор", "collect", "grapes", "farm"))
async def cmd_collect(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    last_time = user['last_collect'] if user else 0
    
    if now - last_time < COOLDOWN_SECONDS:
        wait = COOLDOWN_SECONDS - (now - last_time)
        await message.answer(f"⏳ Grapes are still growing! Wait {wait} sec")
        return
    
    await update_balance(user_id, GRAPE_REWARD)
    await update_collect_time(user_id, now)
    new_user = await get_user(user_id)
    await message.answer(
        f"🍇 +{GRAPE_REWARD} grapes!\n"
        f"Total: {new_user['balance']}"
    )

# 💰 BALANCE
@dp.message(Command("баланс", "balance", "bal", "wallet"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    user = await get_user(user_id)
    balance = user['balance'] if user else 0
    await message.answer(
        f"🍇 **Your Balance**\n\n"
        f"💰 Grapes: {balance} 🍇\n\n"
        f"Collect more with /collect"
    )

# 🏆 TOP
@dp.message(Command("топ", "top", "leaders", "rating"))
async def cmd_top(message: Message):
    top_users = await get_top_users(10)
    
    if not top_users:
        await message.answer("📊 No players yet")
        return
    
    text = "🏆 **Top Players**\n\n"
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

# 🎁 DAILY
@dp.message(Command("ежедневно", "daily", "bonus", "reward"))
async def cmd_daily(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    last_daily = user['last_daily'] if user else 0
    
    if now - last_daily < 86400:
        hours = (86400 - (now - last_daily)) // 3600
        minutes = ((86400 - (now - last_daily)) % 3600) // 60
        await message.answer(
            f"⏰ Daily bonus already claimed!\n"
            f"Next bonus in: {hours}h {minutes}m"
        )
        return
    
    await update_balance(user_id, DAILY_BONUS)
    await update_daily_time(user_id, now)
    await message.answer(
        f"🎁 **Daily Bonus!**\n\n"
        f"+{DAILY_BONUS} grapes 🍇\n"
        f"Come back tomorrow!"
    )

# 📚 HELP
@dp.message(Command("помощь", "help", "info", "commands"))
async def cmd_help(message: Message):
    await message.answer(
        "📚 **Help & Commands**\n\n"
        "🍇 **Main:**\n"
        "/collect, /сбор — collect grapes (+5🍇)\n"
        "/balance, /баланс — check balance\n"
        "/top, /топ — player rating\n"
        "/daily, /ежедневно — daily bonus (+50🍇)\n"
        "/help, /помощь — this help\n"
        "/stats, /статистика — bot statistics\n\n"
        "⏱ Collect cooldown: 60 seconds\n"
        "🎁 Daily bonus: every 24 hours"
    )

# 📊 STATS
@dp.message(Command("статистика", "stats", "statistics", "info"))
async def cmd_stats(message: Message):
    total_users = await get_total_users()
    total_grapes = await get_total_grapes()
    
    await message.answer(
        f"📊 **Bot Statistics**\n\n"
        f"👥 Total Players: {total_users}\n"
        f"🍇 Total Grapes: {total_grapes}\n"
        f" Bot running since March 2026"
    )

async def main():
    await init_db()
    logging.info("✅ Bot started!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
