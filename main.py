import asyncio
import logging
import time
import os
import asyncpg
import random
import string
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Настройки сбора
GRAPE_REWARD = 15
COOLDOWN_HOURS = 4
COOLDOWN_SECONDS = COOLDOWN_HOURS * 3600

# Настройки бонусов
BONUS_AMOUNT = 50
REFERRAL_BONUS = 100
REFERRAL_PERCENT = 10

logging.basicConfig(level=logging.INFO)
pool = None

SHOP_ITEMS = {
    "auto_collect": {"name": "🔄 Авто-сбор", "price": 500, "desc": "Сбор без кулдауна", "type": "upgrade"},
    "double_grapes": {"name": "📈 Умножение x2", "price": 1000, "desc": "Сбор x2 винограда", "type": "upgrade"},
    "bonus_2h": {"name": "⏰ Бонус 2ч", "price": 300, "desc": "Бонус каждые 2 часа", "type": "upgrade"},
    "skin_wine": {"name": "🍷 Скин Вино", "price": 200, "desc": "Меняет эмодзи на вино", "type": "skin"},
    "skin_diamond": {"name": "💎 Скин Алмаз", "price": 500, "desc": "Меняет эмодзи на алмаз", "type": "skin"},
    "restore": {"name": "💚 Восстановление", "price": 100, "desc": "Сброс кулдауна", "type": "consumable"}
}

def generate_ref_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

async def init_db():
    global pool
    pool = await asyncpg.create_pool(DATABASE_URL)
    async with pool.acquire() as conn:
        await conn.execute("""
            CREATE TABLE IF NOT EXISTS users (
                user_id BIGINT PRIMARY KEY,
                balance INTEGER DEFAULT 0,
                last_collect INTEGER DEFAULT 0,
                last_bonus INTEGER DEFAULT 0,
                auto_collect BOOLEAN DEFAULT FALSE,
                double_grapes BOOLEAN DEFAULT FALSE,
                bonus_2h BOOLEAN DEFAULT FALSE,
                skin VARCHAR(20) DEFAULT 'grape',
                ref_code VARCHAR(20),
                invited_by BIGINT,
                total_invited INTEGER DEFAULT 0,
                passive_income INTEGER DEFAULT 0
            )
        """)
    logging.info("База данных готова!")

async def get_user(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return row

async def create_user(user_id, ref_code=None):
    async with pool.acquire() as conn:
        my_ref_code = generate_ref_code()
        
        inviter_id = None
        if ref_code:
            inviter = await conn.fetchrow("SELECT user_id FROM users WHERE ref_code = $1", ref_code)
            if inviter:
                inviter_id = inviter['user_id']
        
        await conn.execute(
            "INSERT INTO users (user_id, ref_code, invited_by) VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING",
            user_id, my_ref_code, inviter_id
        )
        
        if inviter_id:
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", REFERRAL_BONUS, inviter_id)
            await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", REFERRAL_BONUS, user_id)
        
        return await get_user(user_id)

async def add_grapes(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def set_last_collect(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_collect = $1 WHERE user_id = $2", timestamp, user_id)

async def set_last_bonus(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_bonus = $1 WHERE user_id = $2", timestamp, user_id)

async def buy_upgrade(user_id, item_id):
    async with pool.acquire() as conn:
        if item_id == "restore":
            await conn.execute("UPDATE users SET last_collect = 0 WHERE user_id = $1", user_id)
        elif item_id == "auto_collect":
            await conn.execute("UPDATE users SET auto_collect = TRUE WHERE user_id = $1", user_id)
        elif item_id == "double_grapes":
            await conn.execute("UPDATE users SET double_grapes = TRUE WHERE user_id = $1", user_id)
        elif item_id == "bonus_2h":
            await conn.execute("UPDATE users SET bonus_2h = TRUE WHERE user_id = $1", user_id)
        elif item_id == "skin_wine":
            await conn.execute("UPDATE users SET skin = 'wine' WHERE user_id = $1", user_id)
        elif item_id == "skin_diamond":
            await conn.execute("UPDATE users SET skin = 'diamond' WHERE user_id = $1", user_id)

def get_emoji(skin):
    if skin == "wine":
        return "🍷"
    elif skin == "diamond":
        return "💎"
    return "🍇"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    ref_code = args[1] if len(args) > 1 else None
    
    user = await create_user(user_id, ref_code)
    
    if user and user['invited_by'] and user['balance'] >= REFERRAL_BONUS:
        await message.answer(
            f"🎉 Добро пожаловать!\n\n"
            f"Вам и другу начислено по {REFERRAL_BONUS} 🍇!\n\n"
            f"Пишите /сбор чтобы собрать виноград!"
        )
    else:
        await message.answer(
            f"🍇 Привет, {message.from_user.first_name}!\n\n"
            f"Собирай виноград каждые 4 часа!\n\n"
            f"Команды:\n"
            f"/сбор — собрать 15 🍇\n"
            f"/баланс — проверить\n"
            f"/магазин — улучшения\n"
            f"/бонус — бонус\n"
            f"/пригласить — ссылка\n"
            f"/помощь — справка"
        )

@dp.message(Command("сбор"))
async def cmd_collect(message: Message):
    user_id = message.from_user.id
    user = await create_user(user_id)
    
    now = int(time.time())
    last = user['last_collect'] if user else 0
    
    auto = user['auto_collect'] if user else False
    cooldown = 0 if auto else COOLDOWN_SECONDS
    
    if now - last < cooldown:
        hours = (cooldown - (now - last)) // 3600
        minutes = ((cooldown - (now - last)) % 3600) // 60
        await message.answer(f"⏳ Подожди {hours}ч {minutes}м")
        return
    
    double = user['double_grapes'] if user else False
    skin = user['skin'] if user else 'grape'
    emoji = get_emoji(skin)
    
    reward = GRAPE_REWARD * (2 if double else 1)
    
    await add_grapes(user_id, reward)
    await set_last_collect(user_id, now)
    
    new_user = await get_user(user_id)
    
    await message.answer(f"{emoji} +{reward} винограда!\nВсего: {new_user['balance']} 🍇")

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    user = await create_user(user_id)
    
    auto = "✅" if user['auto_collect'] else "❌"
    double = "✅" if user['double_grapes'] else "❌"
    
    await message.answer(
        f"💰 Ваш баланс\n\n"
        f"🍇 Виноград: {user['balance']}\n\n"
        f"🔄 Авто-сбор: {auto}\n"
        f"📈 x2: {double}\n\n"
        f"/сбор — собрать\n"
        f"/магазин — купить"
    )

@dp.message(Command("магазин"))
async def cmd_shop(message: Message):
    user = await get_user(message.from_user.id)
    balance = user['balance'] if user else 0
    
    keyboard = InlineKeyboardBuilder()
    for item_id, item in SHOP_ITEMS.items():
        keyboard.button(text=f"{item['name']} — {item['price']} 🍇", callback_data=f"buy_{item_id}")
    keyboard.adjust(1)
    
    await message.answer(
        f"🏪 Магазин\n\n"
        f"Баланс: {balance} 🍇\n\n"
        f"🔄 Авто-сбор — 500 🍇\n"
        f"📈 x2 — 1000 🍇\n"
        f"⏰ Бонус 2ч — 300 🍇\n"
        f"🍷 Скин Вино — 200 🍇\n"
        f"💎 Скин Алмаз — 500 🍇\n"
        f"💚 Сброс — 100 🍇\n\n"
        f"Нажми на товар!",
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback):
    user_id = callback.from_user.id
    item_id = callback.data.replace("buy_", "")
    
    if item_id not in SHOP_ITEMS:
        await callback.answer("❌ Не найдено", show_alert=True)
        return
    
    item = SHOP_ITEMS[item_id]
    user = await get_user(user_id)
    
    if user['balance'] < item['price']:
        await callback.answer("❌ Недостаточно 🍇", show_alert=True)
        return
    
    if item['type'] == "upgrade":
        if item_id == "auto_collect" and user['auto_collect']:
            await callback.answer("❌ Уже есть", show_alert=True)
            return
        if item_id == "double_grapes" and user['double_grapes']:
            await callback.answer("❌ Уже есть", show_alert=True)
            return
        if item_id == "bonus_2h" and user['bonus_2h']:
            await callback.answer("❌ Уже есть", show_alert=True)
            return
    
    await add_grapes(user_id, -item['price'])
    await buy_upgrade(user_id, item_id)
    
    await callback.answer(f"✅ Куплено!", show_alert=True)

@dp.message(Command("бонус"))
async def cmd_bonus(message: Message):
    user_id = message.from_user.id
    user = await create_user(user_id)
    
    now = int(time.time())
    last = user['last_bonus'] if user else 0
    
    bonus_hours = 2 if user['bonus_2h'] else BONUS_HOURS
    cooldown = bonus_hours * 3600
    
    if now - last < cooldown:
        hours = (cooldown - (now - last)) // 3600
        await message.answer(f"⏰ Бонус готов через {hours}ч")
        return
    
    await add_grapes(user_id, BONUS_AMOUNT)
    await set_last_bonus(user_id, now)
    
    new_user = await get_user(user_id)
    await message.answer(f"🎁 +{BONUS_AMOUNT} 🍇\nВсего: {new_user['balance']}")

@dp.message(Command("пригласить"))
async def cmd_invite(message: Message):
    user_id = message.from_user.id
    user = await create_user(user_id)
    
    if not user['ref_code']:
        async with pool.acquire() as conn:
            ref_code = generate_ref_code()
            await conn.execute("UPDATE users SET ref_code = $1 WHERE user_id = $2", ref_code, user_id)
            user = await get_user(user_id)
    
    bot_username = (await bot.get_me()).username
    link = f"https://t.me/{bot_username}?start={user['ref_code']}"
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text="🔗 Копировать", url=link)
    keyboard.adjust(1)
    
    await message.answer(
        f"👥 Пригласи друзей!\n\n"
        f"Ссылка:\n{link}\n\n"
        f"+{REFERRAL_BONUS} 🍇 за друга\n"
        f"+{REFERRAL_PERCENT}% от сбора друга\n\n"
        f"Приглашено: {user['total_invited']}",
        reply_markup=keyboard.as_markup()
    )

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    await message.answer(
        f"📚 Справка\n\n"
        f"/сбор — собрать 15 🍇 (каждые 4 часа)\n"
        f"/баланс — проверить баланс\n"
        f"/магазин — купить улучшения\n"
        f"/бонус — бонус каждые 4 часа\n"
        f"/пригласить — реферальная ссылка\n"
        f"/помощь — эта справка\n"
        f"/статистика — статистика бота"
    )

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    async with pool.acquire() as conn:
        total = await conn.fetchval("SELECT COUNT(*) FROM users")
        grapes = await conn.fetchval("SELECT SUM(balance) FROM users")
    
    await message.answer(f"📊 Статистика\n\n👥 Игроков: {total}\n🍇 Всего: {grapes or 0}")

async def main():
    await init_db()
    logging.info("Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
