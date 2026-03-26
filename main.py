import asyncio
import logging
import time
import os
import asyncpg
import random
import string
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")
GRAPE_REWARD = 5
COOLDOWN_SECONDS = 60
BONUS_HOURS = 4
BONUS_AMOUNT = 50
REFERRAL_BONUS = 100  # Бонус за приглашённого друга
REFERRAL_PERCENT = 10  # % от сбора друга (пассивный доход)

logging.basicConfig(level=logging.INFO)
pool = None

SHOP_ITEMS = {
    "auto_collect": {"name": "🔄 Авто-сбор", "price": 500, "desc": "Сбор без кулдауна", "type": "upgrade"},
    "double_grapes": {"name": "📈 Умножение x2", "price": 1000, "desc": "Сбор x2 винограда", "type": "upgrade"},
    "bonus_2h": {"name": "⏰ Бонус 2ч", "price": 300, "desc": "Бонус каждые 2 часа", "type": "upgrade"},
    "skin_wine": {"name": "🍷 Скин Вино", "price": 200, "desc": "Меняет 🍇 на 🍷", "type": "skin"},
    "skin_diamond": {"name": "💎 Скин Алмаз", "price": 500, "desc": "Меняет 🍇 на ", "type": "skin"},
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
                ref_code VARCHAR(20) UNIQUE,
                invited_by BIGINT,
                total_invited INTEGER DEFAULT 0,
                passive_income INTEGER DEFAULT 0
            )
        """)
        # Добавляем колонки если их нет
        try:
            await conn.execute("ALTER TABLE users ADD COLUMN ref_code VARCHAR(20)")
            await conn.execute("ALTER TABLE users ADD COLUMN invited_by BIGINT")
            await conn.execute("ALTER TABLE users ADD COLUMN total_invited INTEGER DEFAULT 0")
            await conn.execute("ALTER TABLE users ADD COLUMN passive_income INTEGER DEFAULT 0")
        except:
            pass
    logging.info("✅ PostgreSQL подключена!")

async def get_user(user_id):
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        return row if row else None

async def add_user(user_id, ref_code=None):
    async with pool.acquire() as conn:
        # Проверяем, есть ли уже пользователь
        existing = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
        if existing:
            return existing
        
        # Генерируем реф-код для нового пользователя
        my_ref_code = generate_ref_code()
        
        # Находим кто пригласил
        inviter_id = None
        if ref_code:
            inviter = await conn.fetchrow("SELECT user_id FROM users WHERE ref_code = $1", ref_code)
            if inviter:
                inviter_id = inviter['user_id']
        
        await conn.execute(
            """INSERT INTO users (user_id, ref_code, invited_by) 
               VALUES ($1, $2, $3) ON CONFLICT (user_id) DO NOTHING""",
            user_id, my_ref_code, inviter_id
        )
        
        # Если есть пригласивший — начисляем бонусы
        if inviter_id:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, total_invited = total_invited + 1 WHERE user_id = $2",
                REFERRAL_BONUS, inviter_id
            )
            await conn.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
                REFERRAL_BONUS, user_id
            )
        
        return await get_user(user_id)

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

async def add_passive_income(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute(
            "UPDATE users SET balance = balance + $1, passive_income = passive_income + $1 WHERE user_id = $2",
            amount, user_id
        )

async def buy_item(user_id, item_id):
    async with pool.acquire() as conn:
        if item_id == "restore":
            await conn.execute("UPDATE users SET last_collect = 0 WHERE user_id = $1", user_id)
        elif item_id == "auto_collect":
            await conn.execute("UPDATE users SET auto_collect = TRUE WHERE user_id = $1", user_id)
        elif item_id == "double_grapes":
            await conn.execute("UPDATE users SET double_grapes = TRUE WHERE user_id = $1", user_id)
        elif item_id == "bonus_2h":
            await conn.execute("UPDATE users SET bonus_2h = TRUE WHERE user_id = $1", user_id)
        elif item_id in ["skin_wine", "skin_diamond"]:
            skin = "wine" if item_id == "skin_wine" else "diamond"
            await conn.execute("UPDATE users SET skin = $1 WHERE user_id = $2", skin, user_id)

async def get_skin_emoji(skin):
    return {"grape": "🍇", "wine": "🍷", "diamond": "💎"}.get(skin, "🍇")

async def get_top_users(limit=10):
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT $1", limit)

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

@dp.message(CommandStart())
async def cmd_start(message: Message):
    user_id = message.from_user.id
    args = message.text.split()
    ref_code = args[1] if len(args) > 1 else None
    
    user = await add_user(user_id, ref_code)
    
    # Проверяем, новый ли пользователь
    is_new = user and user['invited_by'] and user['balance'] >= REFERRAL_BONUS
    
    if is_new:
        await message.answer(
            f"🎉 **Добро пожаловать!**\n\n"
            f"Вам и вашему другу начислено по **{REFERRAL_BONUS} 🍇**!\n\n"
            f"Собирай виноград и приглашай друзей!"
        )
    else:
        await message.answer(
            f"🍇 **С возвращением, {message.from_user.first_name}!**\n\n"
            f"Собирай виноград и покупай улучшения!\n\n"
            f"📋 **Команды:**\n"
            f"/сбор — собрать виноград\n"
            f"/баланс — проверить баланс\n"
            f"/магазин — открыть магазин\n"
            f"/пригласить — получить ссылку\n"
            f"/бонус — бонус каждые 4 часа\n"
            f"/топ — рейтинг\n"
            f"/помощь — справка"
        )

@dp.message(Command("сбор"))
async def cmd_collect(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    
    auto = user['auto_collect'] if user else False
    double = user['double_grapes'] if user else False
    skin = user['skin'] if user else 'grape'
    emoji = await get_skin_emoji(skin)
    
    last_time = user['last_collect'] if user else 0
    cooldown = 0 if auto else COOLDOWN_SECONDS
    
    if now - last_time < cooldown:
        wait = cooldown - (now - last_time)
        await message.answer(f"⏳ {emoji} ещё растут! Подожди {wait} сек")
        return
    
    reward = GRAPE_REWARD * (2 if double else 1)
    await update_balance(user_id, reward)
    await update_collect_time(user_id, now)
    new_user = await get_user(user_id)
    
    # Пассивный доход для пригласившего
    if user and user['invited_by']:
        passive = int(reward * REFERRAL_PERCENT / 100)
        if passive > 0:
            await add_passive_income(user['invited_by'], passive)
    
    await message.answer(
        f"{emoji} +{reward} винограда!\n"
        f"Всего: {new_user['balance']}"
    )

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    user = await get_user(user_id)
    balance = user['balance'] if user else 0
    
    auto = "✅" if user and user['auto_collect'] else "❌"
    double = "✅" if user and user['double_grapes'] else "❌"
    bonus = "✅" if user and user['bonus_2h'] else "❌"
    invited = user['total_invited'] if user else 0
    passive = user['passive_income'] if user else 0
    
    await message.answer(
        f"💰 **Ваш баланс**\n\n"
        f"Виноград: {balance} 🍇\n\n"
        f"🔄 Авто-сбор: {auto}\n"
        f"📈 Умножение x2: {double}\n"
        f"⏰ Бонус 2ч: {bonus}\n\n"
        f"👥 Приглашено: {invited}\n"
        f"💵 Пассивный доход: {passive} 🍇\n\n"
        f"/магазин — купить улучшения\n"
        f"/пригласить — пригласить друга"
    )

@dp.message(Command("магазин"))
async def cmd_shop(message: Message):
    user = await get_user(message.from_user.id)
    balance = user['balance'] if user else 0
    
    keyboard = InlineKeyboardBuilder()
    for item_id, item in SHOP_ITEMS.items():
        keyboard.button(text=f"{item['name']} — {item['price']}🍇", callback_data=f"buy_{item_id}")
    keyboard.adjust(1)
    
    await message.answer(
        f"🏪 **Магазин**\n\n"
        f"Ваш баланс: {balance} 🍇\n\n"
        f"**Улучшения:**\n"
        f"🔄 Авто-сбор (500🍇)\n"
        f"📈 Умножение x2 (1000🍇)\n"
        f"⏰ Бонус 2ч (300🍇)\n\n"
        f"**Скины:**\n"
        f"🍷 Скин Вино (200🍇)\n"
        f"💎 Скин Алмаз (500🍇)\n\n"
        f"**Расходники:**\n"
        f"💚 Восстановление (100🍇)\n\n"
        f"Нажмите для покупки!",
        reply_markup=keyboard.as_markup()
    )

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback):
    user_id = callback.from_user.id
    item_id = callback.data.replace("buy_", "")
    
    if item_id not in SHOP_ITEMS:
        await callback.answer("❌ Товар не найден", show_alert=True)
        return
    
    item = SHOP_ITEMS[item_id]
    user = await get_user(user_id)
    balance = user['balance'] if user else 0
    
    if balance < item['price']:
        await callback.answer(f"❌ Недостаточно! Нужно {item['price']}", show_alert=True)
        return
    
    if item['type'] == "upgrade":
        if item_id == "auto_collect" and user and user['auto_collect']:
            await callback.answer("❌ Уже куплено!", show_alert=True)
            return
        if item_id == "double_grapes" and user and user['double_grapes']:
            await callback.answer("❌ Уже куплено!", show_alert=True)
            return
        if item_id == "bonus_2h" and user and user['bonus_2h']:
            await callback.answer("❌ Уже куплено!", show_alert=True)
            return
    
    await update_balance(user_id, -item['price'])
    await buy_item(user_id, item_id)
    
    await callback.answer(f"✅ {item['name']} куплен!", show_alert=True)

@dp.message(Command("пригласить"))
async def cmd_invite(message: Message):
    user_id = message.from_user.id
    user = await get_user(user_id)
    
    if not user or not user['ref_code']:
        # Генерируем код если нет
        async with pool.acquire() as conn:
            ref_code = generate_ref_code()
            await conn.execute("UPDATE users SET ref_code = $1 WHERE user_id = $2", ref_code, user_id)
            user = await get_user(user_id)
    
    ref_code = user['ref_code']
    bot_username = (await bot.get_me()).username
    invite_link = f"https://t.me/{bot_username}?start={ref_code}"
    
    invited = user['total_invited'] if user else 0
    passive = user['passive_income'] if user else 0
    
    keyboard = InlineKeyboardBuilder()
    keyboard.button(text=" Копировать ссылку", url=invite_link)
    keyboard.adjust(1)
    
    await message.answer(
        f"👥 **Пригласи друзей!**\n\n"
        f"Твоя реферальная ссылка:\n"
        f"`{invite_link}`\n\n"
        f"🎁 **Бонусы:**\n"
        f"+{REFERRAL_BONUS} 🍇 за каждого друга\n"
        f"+{REFERRAL_PERCENT}% от сбора друга (пассивно)\n\n"
        f"📊 **Твоя статистика:**\n"
        f"Приглашено: {invited}\n"
        f"Пассивный доход: {passive} 🍇\n\n"
        f"Отправь ссылку друзьям!",
        reply_markup=keyboard.as_markup()
    )

@dp.message(Command("бонус"))
async def cmd_bonus(message: Message):
    user_id = message.from_user.id
    await add_user(user_id)
    now = int(time.time())
    user = await get_user(user_id)
    last_bonus = user['last_bonus'] if user else 0
    
    bonus_cooldown = (2 if user and user['bonus_2h'] else BONUS_HOURS) * 3600
    
    if now - last_bonus < bonus_cooldown:
        remaining = bonus_cooldown - (now - last_bonus)
        hours = remaining // 3600
        minutes = (remaining % 3600) // 60
        await message.answer(f"⏰ Бонус уже получен!\nСледующий через: {hours}ч {minutes}м")
        return
    
    await update_balance(user_id, BONUS_AMOUNT)
    await update_bonus_time(user_id, now)
    new_user = await get_user(user_id)
    await message.answer(f"🎁 **Бонус!**\n+{BONUS_AMOUNT} 🍇\nВсего: {new_user['balance']}")

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    top_users = await get_top_users(10)
    if not top_users:
        await message.answer("📊 Пока нет игроков")
        return
    
    text = "🏆 **Топ игроков**\n\n"
    for i, row in enumerate(top_users, 1):
        medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
        try:
            user = await bot.get_chat(row['user_id'])
            name = user.first_name[:20]
        except:
            name = f"User{row['user_id']}"
        text += f"{medal} {name} — {row['balance']} 🍇\n"
    await message.answer(text)

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    await message.answer(
        "📚 **Справка**\n\n"
        "/сбор — собрать виноград\n"
        "/баланс — проверить баланс\n"
        "/магазин — купить улучшения\n"
        "/пригласить — реферальная ссылка\n"
        "/бонус — бонус каждые 4 часа\n"
        "/топ — рейтинг\n"
        "/помощь — справка\n"
        "/статистика — статистика бота"
    )

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    total_users = await get_total_users()
    total_grapes = await get_total_grapes()
    await message.answer(f"📊 **Статистика**\n\n👥 Игроков: {total_users}\n🍇 Всего собрано: {total_grapes}")

async def main():
    await init_db()
    logging.info("✅ Бот запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
