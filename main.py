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
BONUS_HOURS = 4
REFERRAL_BONUS = 100
REFERRAL_PERCENT = 10

# Настройки казино
CASINO_MIN_BET = 10
CASINO_MAX_BET = 10000

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
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        logging.info("✅ Подключение к базе данных успешно!")
        
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
                    passive_income INTEGER DEFAULT 0,
                    casino_wins INTEGER DEFAULT 0,
                    casino_losses INTEGER DEFAULT 0,
                    casino_total_won INTEGER DEFAULT 0,
                    casino_total_lost INTEGER DEFAULT 0
                )
            """)
            
            columns_to_add = [
                ("ref_code", "VARCHAR(20)"),
                ("invited_by", "BIGINT"),
                ("total_invited", "INTEGER DEFAULT 0"),
                ("passive_income", "INTEGER DEFAULT 0"),
                ("auto_collect", "BOOLEAN DEFAULT FALSE"),
                ("double_grapes", "BOOLEAN DEFAULT FALSE"),
                ("bonus_2h", "BOOLEAN DEFAULT FALSE"),
                ("skin", "VARCHAR(20) DEFAULT 'grape'"),
                ("casino_wins", "INTEGER DEFAULT 0"),
                ("casino_losses", "INTEGER DEFAULT 0"),
                ("casino_total_won", "INTEGER DEFAULT 0"),
                ("casino_total_lost", "INTEGER DEFAULT 0")
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    await conn.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                    logging.info(f"✅ Колонка {col_name} добавлена")
                except Exception as e:
                    logging.info(f"ℹ️ Колонка {col_name} уже существует")
        
        logging.info("✅ База данных готова!")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации БД: {e}")
        raise

async def get_user(user_id):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            return row if row else None
    except Exception as e:
        logging.error(f"❌ Ошибка get_user: {e}")
        return None

async def add_user(user_id, ref_code=None):
    try:
        async with pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if existing:
                return existing
            
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
                await conn.execute(
                    "UPDATE users SET balance = balance + $1, total_invited = total_invited + 1 WHERE user_id = $2",
                    REFERRAL_BONUS, inviter_id
                )
                await conn.execute(
                    "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
                    REFERRAL_BONUS, user_id
                )
            
            return await get_user(user_id)
    except Exception as e:
        logging.error(f"❌ Ошибка add_user: {e}")
        return None

async def update_balance(user_id, amount):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1 WHERE user_id = $2",
                amount, user_id
            )
    except Exception as e:
        logging.error(f"❌ Ошибка update_balance: {e}")

async def update_collect_time(user_id, timestamp):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_collect = $1 WHERE user_id = $2",
                timestamp, user_id
            )
    except Exception as e:
        logging.error(f"❌ Ошибка update_collect_time: {e}")

async def update_bonus_time(user_id, timestamp):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET last_bonus = $1 WHERE user_id = $2",
                timestamp, user_id
            )
    except Exception as e:
        logging.error(f"❌ Ошибка update_bonus_time: {e}")

async def add_passive_income(user_id, amount):
    try:
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, passive_income = passive_income + $1 WHERE user_id = $2",
                amount, user_id
            )
    except Exception as e:
        logging.error(f"❌ Ошибка add_passive_income: {e}")

async def buy_item(user_id, item_id):
    try:
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
    except Exception as e:
        logging.error(f"❌ Ошибка buy_item: {e}")

async def update_casino_stats(user_id, won, amount):
    try:
        async with pool.acquire() as conn:
            if won:
                await conn.execute(
                    "UPDATE users SET casino_wins = casino_wins + 1, casino_total_won = casino_total_won + $1 WHERE user_id = $2",
                    amount, user_id
                )
            else:
                await conn.execute(
                    "UPDATE users SET casino_losses = casino_losses + 1, casino_total_lost = casino_total_lost + $1 WHERE user_id = $2",
                    amount, user_id
                )
    except Exception as e:
        logging.error(f"❌ Ошибка update_casino_stats: {e}")

async def get_skin_emoji(skin):
    return {"grape": "🍇", "wine": "🍷", "diamond": "💎"}.get(skin, "🍇")

async def get_top_users(limit=10):
    try:
        async with pool.acquire() as conn:
            return await conn.fetch("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT $1", limit)
    except Exception as e:
        logging.error(f"❌ Ошибка get_top_users: {e}")
        return []

async def get_total_users():
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT COUNT(*) FROM users")
            return row['count']
    except:
        return 0

async def get_total_grapes():
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT SUM(balance) FROM users")
            return row['sum'] if row['sum'] else 0
    except:
        return 0

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

@dp.message(CommandStart())
async def cmd_start(message: Message):
    try:
        logging.info(f"📩 /start от пользователя {message.from_user.id}")
        user_id = message.from_user.id
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code)
        
        is_new = user and user.get('invited_by') and user.get('balance', 0) >= REFERRAL_BONUS
        
        if is_new:
            await message.answer(
                f"🎉 Добро пожаловать!\n\n"
                f"Вам и вашему другу начислено по {REFERRAL_BONUS} 🍇!\n\n"
                f"Собирай виноград и приглашай друзей!"
            )
        else:
            await message.answer(
                f"🍇 С возвращением, {message.from_user.first_name}!\n\n"
                f"Собирай виноград каждые 4 часа по 15 🍇!\n\n"
                f"📋 Команды:\n"
                f"/сбор — собрать 15 🍇 (каждые 4 часа)\n"
                f"/баланс — проверить баланс\n"
                f"/казино — играть в казино\n"
                f"/магазин — открыть магазин\n"
                f"/пригласить — получить ссылку\n"
                f"/бонус — бонус каждые 4 часа\n"
                f"/топ — рейтинг\n"
                f"/помощь — справка"
            )
        logging.info(f"✅ /start обработан для {user_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /start: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")

@dp.message(Command("сбор"))
async def cmd_collect(message: Message):
    try:
        logging.info(f"📩 /сбор от пользователя {message.from_user.id}")
        user_id = message.from_user.id
        
        user = await add_user(user_id)
        if not user:
            await message.answer("❌ Ошибка доступа к базе данных")
            return
        
        now = int(time.time())
        
        auto = user.get('auto_collect', False)
        double = user.get('double_grapes', False)
        skin = user.get('skin', 'grape')
        emoji = await get_skin_emoji(skin)
        
        last_time = user.get('last_collect', 0)
        cooldown = 0 if auto else COOLDOWN_SECONDS
        
        logging.info(f"🕐 last_collect={last_time}, now={now}, cooldown={cooldown}")
        
        if now - last_time < cooldown:
            wait = cooldown - (now - last_time)
            hours = wait // 3600
            minutes = (wait % 3600) // 60
            await message.answer(f"⏳ {emoji} ещё растут! Подожди {hours}ч {minutes}м")
            return
        
        reward = GRAPE_REWARD * (2 if double else 1)
        
        await update_balance(user_id, reward)
        await update_collect_time(user_id, now)
        
        new_user = await get_user(user_id)
        new_balance = new_user.get('balance', 0) if new_user else reward
        
        logging.info(f"✅ Сбор успешен: +{reward}, баланс={new_balance}")
        
        if user.get('invited_by'):
            passive = int(reward * REFERRAL_PERCENT / 100)
            if passive > 0:
                await add_passive_income(user['invited_by'], passive)
        
        await message.answer(
            f"{emoji} +{reward} винограда!\n"
            f"Всего: {new_balance} 🍇"
        )
        logging.info(f"✅ /сбор обработан для {user_id}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /сбор: {e}")
        await message.answer("❌ Произошла ошибка при сборе. Попробуйте позже.")

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    try:
        user_id = message.from_user.id
        await add_user(user_id)
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        auto = "✅" if user and user.get('auto_collect') else "❌"
        double = "✅" if user and user.get('double_grapes') else "❌"
        bonus = "✅" if user and user.get('bonus_2h') else "❌"
        invited = user.get('total_invited', 0) if user else 0
        passive = user.get('passive_income', 0) if user else 0
        
        await message.answer(
            f"💰 Ваш баланс\n\n"
            f"🍇 Виноград: {balance}\n\n"
            f"🔄 Авто-сбор: {auto}\n"
            f"📈 Умножение x2: {double}\n"
            f"⏰ Бонус 2ч: {bonus}\n\n"
            f"👥 Приглашено: {invited}\n"
            f"💵 Пассивный доход: {passive}\n\n"
            f"/магазин — купить улучшения\n"
            f"/пригласить — пригласить друга"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /баланс: {e}")
        await message.answer("❌ Произошла ошибка.")

@dp.message(Command("магазин"))
async def cmd_shop(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in SHOP_ITEMS.items():
            keyboard.button(text=f"{item['name']} — {item['price']} 🍇", callback_data=f"buy_{item_id}")
        keyboard.adjust(1)
        
        await message.answer(
            f"🏪 Магазин\n\n"
            f"Ваш баланс: {balance} 🍇\n\n"
            f"🔧 Улучшения:\n"
            f"🔄 Авто-сбор (500 🍇) — без кулдауна\n"
            f"📈 Умножение x2 (1000 🍇) — сбор x2\n"
            f"⏰ Бонус 2ч (300 🍇) — бонус чаще\n\n"
            f"🎨 Скины:\n"
            f"🍷 Скин Вино (200 🍇)\n"
            f"💎 Скин Алмаз (500 🍇)\n\n"
            f"💚 Расходники:\n"
            f"💚 Восстановление (100 🍇) — сброс кулдауна\n\n"
            f"Нажмите для покупки!",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /магазин: {e}")
        await message.answer("❌ Ошибка магазина.")

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback):
    try:
        user_id = callback.from_user.id
        item_id = callback.data.replace("buy_", "")
        
        if item_id not in SHOP_ITEMS:
            await callback.answer("❌ Товар не найден", show_alert=True)
            return
        
        item = SHOP_ITEMS[item_id]
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        if balance < item['price']:
            await callback.answer(f"❌ Недостаточно! Нужно {item['price']}", show_alert=True)
            return
        
        if item['type'] == "upgrade":
            if item_id == "auto_collect" and user and user.get('auto_collect'):
                await callback.answer("❌ Уже куплено!", show_alert=True)
                return
            if item_id == "double_grapes" and user and user.get('double_grapes'):
                await callback.answer("❌ Уже куплено!", show_alert=True)
                return
            if item_id == "bonus_2h" and user and user.get('bonus_2h'):
                await callback.answer("❌ Уже куплено!", show_alert=True)
                return
        
        await update_balance(user_id, -item['price'])
        await buy_item(user_id, item_id)
        
        await callback.answer(f"✅ {item['name']} куплен!", show_alert=True)
        
        await callback.message.answer(
            f"✅ Покупка успешна!\n\n"
            f"{item['name']}\n"
            f"{item['desc']}\n\n"
            f"Списано: {item['price']} 🍇"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_buy: {e}")

@dp.message(Command("казино"))
async def cmd_casino(message: Message):
    try:
        user_id = message.from_user.id
        await add_user(user_id)
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🎰 Слоты", callback_data="casino_slots")
        keyboard.button(text="🎲 Кубики", callback_data="casino_dice")
        keyboard.button(text="🪙 Монетка", callback_data="casino_coin")
        keyboard.button(text="📊 Моя статистика", callback_data="casino_stats")
        keyboard.adjust(2)
        
        await message.answer(
            f"🎰 **Казино**\n\n"
            f"Ваш баланс: {balance} 🍇\n\n"
            f"🎮 Игры:\n"
            f"🎰 Слоты — x3 выигрыш (33% шанс)\n"
            f"🎲 Кубики — x2 выигрыш (50% шанс)\n"
            f"🪙 Монетка — x2 выигрыш (50% шанс)\n\n"
            f"Минимальная ставка: {CASINO_MIN_BET} 🍇\n"
            f"Максимальная ставка: {CASINO_MAX_BET} 🍇\n\n"
            f"Выберите игру!",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /казино: {e}")
        await message.answer("❌ Ошибка казино.")

@dp.callback_query(lambda c: c.data.startswith("casino_"))
async def callback_casino(callback):
    try:
        user_id = callback.from_user.id
        action = callback.data.replace("casino_", "")
        
        if action == "stats":
            user = await get_user(user_id)
            wins = user.get('casino_wins', 0) if user else 0
            losses = user.get('casino_losses', 0) if user else 0
            total_won = user.get('casino_total_won', 0) if user else 0
            total_lost = user.get('casino_total_lost', 0) if user else 0
            
            await callback.message.answer(
                f"📊 **Ваша статистика в казино**\n\n"
                f"🏆 Побед: {wins}\n"
                f"❌ Поражений: {losses}\n"
                f"💰 Всего выиграно: {total_won} 🍇\n"
                f"💸 Всего проиграно: {total_lost} 🍇\n\n"
                f"Баланс: {total_won - total_lost} 🍇"
            )
            await callback.answer()
            return
        
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        if balance < CASINO_MIN_BET:
            await callback.answer(f"❌ Минимальная ставка {CASINO_MIN_BET} 🍇", show_alert=True)
            return
        
        keyboard = InlineKeyboardBuilder()
        bets = [10, 50, 100, 500, 1000]
        for bet in bets:
            if bet <= balance:
                keyboard.button(text=f"{bet} 🍇", callback_data=f"bet_{action}_{bet}")
        keyboard.button(text="Своя сумма", callback_data=f"bet_{action}_custom")
        keyboard.adjust(3)
        
        game_names = {"slots": "🎰 Слоты", "dice": "🎲 Кубики", "coin": "🪙 Монетка"}
        
        await callback.message.answer(
            f"{game_names.get(action, 'Игра')}\n\n"
            f"Ваш баланс: {balance} 🍇\n\n"
            f"Выберите ставку:",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_casino (выбор): {e}")

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def callback_bet(callback):
    try:
        user_id = callback.from_user.id
        parts = callback.data.replace("bet_", "").split("_")
        game = parts[0]
        bet_amount = parts[1]
        
        if bet_amount == "custom":
            await callback.message.answer(
                f"Введите сумму ставки (от {CASINO_MIN_BET} до {CASINO_MAX_BET}):"
            )
            await callback.answer()
            return
        
        bet_amount = int(bet_amount)
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        if bet_amount < CASINO_MIN_BET or bet_amount > CASINO_MAX_BET:
            await callback.answer(f"❌ Ставка от {CASINO_MIN_BET} до {CASINO_MAX_BET}", show_alert=True)
            return
        
        if balance < bet_amount:
            await callback.answer("❌ Недостаточно винограда", show_alert=True)
            return
        
        await update_balance(user_id, -bet_amount)
        
        if game == "slots":
            win_chance = 0.33
            win_multiplier = 3
        elif game == "dice":
            win_chance = 0.5
            win_multiplier = 2
        elif game == "coin":
            win_chance = 0.5
            win_multiplier = 2
        else:
            win_chance = 0.5
            win_multiplier = 2
        
        won = random.random() < win_chance
        
        if won:
            win_amount = bet_amount * win_multiplier
            await update_balance(user_id, win_amount)
            await update_casino_stats(user_id, True, bet_amount)
            
            emojis = {"slots": "🎰", "dice": "🎲", "coin": "🪙"}
            await callback.message.answer(
                f"{emojis.get(game, '🎰')} **ПОБЕДА!**\n\n"
                f"Ставка: {bet_amount} 🍇\n"
                f"Выигрыш: {win_amount} 🍇\n"
                f"Прибыль: +{win_amount - bet_amount} 🍇\n\n"
                f"Поздравляю! 🎉"
            )
        else:
            await update_casino_stats(user_id, False, bet_amount)
            
            emojis = {"slots": "🎰", "dice": "🎲", "coin": "🪙"}
            await callback.message.answer(
                f"{emojis.get(game, '🎰')} **ПРОИГРЫШ**\n\n"
                f"Ставка: {bet_amount} 🍇\n"
                f"Попробуй ещё раз! 🍀"
            )
        
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_bet: {e}")
        await callback.message.answer("❌ Ошибка игры. Попробуйте позже.")

@dp.message(Command("пригласить"))
async def cmd_invite(message: Message):
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        
        if not user or not user.get('ref_code'):
            async with pool.acquire() as conn:
                ref_code = generate_ref_code()
                await conn.execute("UPDATE users SET ref_code = $1 WHERE user_id = $2", ref_code, user_id)
                user = await get_user(user_id)
        
        ref_code = user.get('ref_code', '')
        bot_username = (await bot.get_me()).username
        invite_link = f"https://t.me/{bot_username}?start={ref_code}"
        
        invited = user.get('total_invited', 0) if user else 0
        passive = user.get('passive_income', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔗 Копировать ссылку", url=invite_link)
        keyboard.adjust(1)
        
        await message.answer(
            f"👥 Пригласи друзей!\n\n"
            f"Твоя реферальная ссылка:\n"
            f"{invite_link}\n\n"
            f"🎁 Бонусы:\n"
            f"+{REFERRAL_BONUS} 🍇 за каждого друга\n"
            f"+{REFERRAL_PERCENT}% от сбора друга (пассивно)\n\n"
            f"📊 Твоя статистика:\n"
            f"Приглашено: {invited}\n"
            f"Пассивный доход: {passive} 🍇\n\n"
            f"Отправь ссылку друзьям!",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /пригласить: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("бонус"))
async def cmd_bonus(message: Message):
    try:
        user_id = message.from_user.id
        await add_user(user_id)
        now = int(time.time())
        user = await get_user(user_id)
        last_bonus = user.get('last_bonus', 0) if user else 0
        
        bonus_cooldown = (2 if user and user.get('bonus_2h') else BONUS_HOURS) * 3600
        
        if now - last_bonus < bonus_cooldown:
            remaining = bonus_cooldown - (now - last_bonus)
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            await message.answer(f"⏰ Бонус уже получен!\nСледующий через: {hours}ч {minutes}м")
            return
        
        await update_balance(user_id, BONUS_AMOUNT)
        await update_bonus_time(user_id, now)
        new_user = await get_user(user_id)
        await message.answer(f"🎁 Бонус!\n+{BONUS_AMOUNT} 🍇\nВсего: {new_user.get('balance', 0) if new_user else BONUS_AMOUNT}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /бонус: {e}")
        await message.answer("❌ Ошибка бонуса.")

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    try:
        top_users = await get_top_users(10)
        if not top_users:
            await message.answer("📊 Пока нет игроков")
            return
        
        text = "🏆 Топ игроков\n\n"
        for i, row in enumerate(top_users, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            try:
                user = await bot.get_chat(row['user_id'])
                name = user.first_name[:20]
            except:
                name = f"User{row['user_id']}"
            text += f"{medal} {name} — {row['balance']} 🍇\n"
        await message.answer(text)
    except Exception as e:
        logging.error(f"❌ Ошибка в /топ: {e}")
        await message.answer("❌ Ошибка топа.")

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    try:
        await message.answer(
            f"📚 Справка\n\n"
            f"🍇 Основные команды:\n"
            f"/сбор — собрать 15 🍇 (каждые 4 часа)\n"
            f"/баланс — проверить баланс\n"
            f"/казино — играть в казино\n"
            f"/магазин — купить улучшения\n"
            f"/бонус — бонус каждые 4 часа (+50 🍇)\n"
            f"/пригласить — реферальная ссылка\n"
            f"/топ — рейтинг игроков\n"
            f"/помощь — эта справка\n"
            f"/статистика — статистика бота\n\n"
            f"🔧 Улучшения в магазине:\n"
            f"🔄 Авто-сбор — без кулдауна\n"
            f"📈 x2 — удвоение сбора\n"
            f"⏰ Бонус 2ч — бонус чаще\n\n"
            f"🎰 Казино:\n"
            f"🎰 Слоты — x3 (33% шанс)\n"
            f"🎲 Кубики — x2 (50% шанс)\n"
            f"🪙 Монетка — x2 (50% шанс)\n\n"
            f"Удачи в сборе винограда! 🍇"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /помощь: {e}")

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    try:
        total_users = await get_total_users()
        total_grapes = await get_total_grapes()
        await message.answer(f"📊 Статистика\n\n👥 Игроков: {total_users}\n🍇 Всего собрано: {total_grapes}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /статистика: {e}")
        await message.answer("❌ Ошибка статистики.")

async def main():
    try:
        await init_db()
        logging.info("✅ Бот запущен!")
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"❌ Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
