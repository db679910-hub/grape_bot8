import asyncio
import logging
import time
import os
import asyncpg
import random
import string
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
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

# Честные шансы казино
CASINO_GAMES = {
    "slots": {
        "name": "🎰 Слоты",
        "win_chance": 0.35,
        "multiplier": 3,
        "description": "3 совпадающих символа"
    },
    "dice": {
        "name": "🎲 Кубики",
        "win_chance": 0.50,
        "multiplier": 2,
        "description": "Угадай число"
    },
    "coin": {
        "name": "🪙 Монетка",
        "win_chance": 0.50,
        "multiplier": 2,
        "description": "Орёл или Решка"
    },
    "wheel": {
        "name": "🎡 Колесо",
        "win_chance": 0.40,
        "multiplier": 2.5,
        "description": "Крути колесо удачи"
    },
    "blackjack": {
        "name": "🃏 Блэкджек",
        "win_chance": 0.45,
        "multiplier": 2,
        "description": "Набери 21 очко"
    }
}

# Эмодзи для анимаций
SLOT_EMOJIS = ["🍇", "🍒", "🍋", "🍊", "🔔", "", "7️"]
DICE_EMOJIS = ["⚀", "⚁", "", "⚃", "", "⚅"]
WHEEL_SEGMENTS = ["🍇", "🍇", "🍇", "🍇", "", "", "", "", "💎", "❌"]
COIN_SIDES = ["🦅 Орёл", "🪙 Решка"]

logging.basicConfig(level=logging.INFO)
pool = None

SHOP_ITEMS = {
    "auto_collect": {"name": "🔄 Авто-сбор", "price": 500, "desc": "Сбор без кулдауна", "type": "upgrade"},
    "double_grapes": {"name": "📈 Умножение x2", "price": 1000, "desc": "Сбор x2 винограда", "type": "upgrade"},
    "bonus_2h": {"name": "⏰ Бонус 2ч", "price": 300, "desc": "Бонус каждые 2 часа", "type": "upgrade"},
    "skin_wine": {"name": "🍷 Скин Вино", "price": 200, "desc": "Меняет эмодзи на вино", "type": "skin"},
    "skin_diamond": {"name": "💎 Скин Алмаз", "price": 500, "desc": "Меняет эмодзи на алмаз", "type": "skin"},
    "restore": {"name": "💚 Восстановление", "price": 100, "desc": "Сброс кулдауна", "type": "consumable"},
    "luck_charm": {"name": "🍀 Талич удачи", "price": 250, "desc": "+5% к шансу в казино", "type": "consumable"}
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
                    casino_total_lost INTEGER DEFAULT 0,
                    luck_charm INTEGER DEFAULT 0,
                    level INTEGER DEFAULT 1,
                    experience INTEGER DEFAULT 0,
                    total_games INTEGER DEFAULT 0
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
                ("casino_total_lost", "INTEGER DEFAULT 0"),
                ("luck_charm", "INTEGER DEFAULT 0"),
                ("level", "INTEGER DEFAULT 1"),
                ("experience", "INTEGER DEFAULT 0"),
                ("total_games", "INTEGER DEFAULT 0")
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    await conn.execute(f"ALTER TABLE users ADD COLUMN {col_name} {col_type}")
                    logging.info(f"✅ Колонка {col_name} добавлена")
                except:
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
            elif item_id == "luck_charm":
                await conn.execute("UPDATE users SET luck_charm = luck_charm + 1 WHERE user_id = $1", user_id)
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
                    "UPDATE users SET casino_wins = casino_wins + 1, casino_total_won = casino_total_won + $1, experience = experience + $2, total_games = total_games + 1 WHERE user_id = $3",
                    amount, int(amount * 0.1), user_id
                )
            else:
                await conn.execute(
                    "UPDATE users SET casino_losses = casino_losses + 1, casino_total_lost = casino_total_lost + $1, experience = experience + $2, total_games = total_games + 1 WHERE user_id = $3",
                    amount, int(amount * 0.05), user_id
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
        user_id = message.from_user.id
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code)
        
        if user and user.get('invited_by') and user.get('balance', 0) >= REFERRAL_BONUS:
            await message.answer(
                f"🎉 Добро пожаловать!\n\n"
                f"Вам и другу начислено по {REFERRAL_BONUS} 🍇!\n\n"
                f"Собирай виноград и играй в казино!"
            )
        else:
            await message.answer(
                f"🍇 Привет, {message.from_user.first_name}!\n\n"
                f"Собирай виноград каждые 4 часа по 15 🍇!\n\n"
                f"📋 Команды:\n"
                f"/сбор — собрать 15 🍇\n"
                f"/баланс — проверить баланс\n"
                f"/казино — играть в казино\n"
                f"/магазин — улучшения\n"
                f"/бонус — бонус каждые 4 часа\n"
                f"/пригласить — реферальная ссылка\n"
                f"/топ — рейтинг\n"
                f"/помощь — справка"
            )
    except Exception as e:
        logging.error(f"❌ Ошибка в /start: {e}")
        await message.answer("❌ Произошла ошибка.")

@dp.message(Command("сбор"))
async def cmd_collect(message: Message):
    try:
        user_id = message.from_user.id
        user = await add_user(user_id)
        
        now = int(time.time())
        auto = user.get('auto_collect', False) if user else False
        double = user.get('double_grapes', False) if user else False
        skin = user.get('skin', 'grape') if user else 'grape'
        emoji = await get_skin_emoji(skin)
        last_time = user.get('last_collect', 0) if user else 0
        cooldown = 0 if auto else COOLDOWN_SECONDS
        
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
        
        if user and user.get('invited_by'):
            passive = int(reward * REFERRAL_PERCENT / 100)
            if passive > 0:
                await add_passive_income(user['invited_by'], passive)
        
        await message.answer(f"{emoji} +{reward} винограда!\nВсего: {new_user['balance']} 🍇")
    except Exception as e:
        logging.error(f"❌ Ошибка в /сбор: {e}")
        await message.answer("❌ Ошибка сбора.")

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
        luck = user.get('luck_charm', 0) if user else 0
        level = user.get('level', 1) if user else 1
        exp = user.get('experience', 0) if user else 0
        
        await message.answer(
            f"💰 Ваш баланс\n\n"
            f"🍇 Виноград: {balance}\n"
            f"⭐ Уровень: {level}\n"
            f"✨ Опыт: {exp}\n\n"
            f"🔄 Авто-сбор: {auto}\n"
            f"📈 x2: {double}\n"
            f"⏰ Бонус 2ч: {bonus}\n"
            f"🍀 Талисман: {luck}\n\n"
            f"/сбор — собрать\n"
            f"/магазин — купить"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /баланс: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("магазин"))
async def cmd_shop(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in SHOP_ITEMS.items():
            keyboard.button(text=f"{item['name']} — {item['price']} 🍇", callback_data=f"buy_{item_id}")
        keyboard.adjust(2)
        
        await message.answer(
            f"🏪 Магазин\n\n"
            f"Баланс: {balance} 🍇\n\n"
            f"🔧 Улучшения:\n"
            f"🔄 Авто-сбор — 500 🍇\n"
            f"📈 x2 — 1000 🍇\n"
            f"⏰ Бонус 2ч — 300 🍇\n\n"
            f"🎨 Скины:\n"
            f"🍷 Скин Вино — 200 🍇\n"
            f"💎 Скин Алмаз — 500 🍇\n\n"
            f"🍀 Казино:\n"
            f"🍀 Талисман удачи — 250 🍇\n\n"
            f"💚 Расходники:\n"
            f"💚 Сброс — 100 🍇\n\n"
            f"Нажми на товар!",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /магазин: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback: CallbackQuery):
    try:
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
        
        await update_balance(user_id, -item['price'])
        await buy_item(user_id, item_id)
        await callback.answer(f"✅ {item['name']} куплен!", show_alert=True)
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_buy: {e}")

@dp.message(Command("казино"))
async def cmd_casino(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        luck = user.get('luck_charm', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🎰 Слоты", callback_data="casino_slots")
        keyboard.button(text="🎲 Кубики", callback_data="casino_dice")
        keyboard.button(text="🪙 Монетка", callback_data="casino_coin")
        keyboard.button(text="🎡 Колесо", callback_data="casino_wheel")
        keyboard.button(text="🃏 Блэкджек", callback_data="casino_blackjack")
        keyboard.button(text="📊 Статистика", callback_data="casino_stats")
        keyboard.adjust(2)
        
        text = f"🎰 Казино\n\n"
        text += f"Баланс: {balance} 🍇\n"
        text += f"🍀 Талисман: {luck} шт (+{luck * 5}% к шансу)\n\n"
        text += f"🎮 Игры:\n"
        for game_id, game in CASINO_GAMES.items():
            text += f"{game['name']} — x{game['multiplier']} ({game['win_chance']*100:.0f}% шанс)\n"
        text += f"\nМин: {CASINO_MIN_BET} 🍇 | Макс: {CASINO_MAX_BET} 🍇\n\n"
        text += f"Выберите игру!"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"❌ Ошибка в /казино: {e}")

@dp.callback_query(lambda c: c.data.startswith("casino_"))
async def callback_casino(callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        action = callback.data.replace("casino_", "")
        
        if action == "stats":
            user = await get_user(user_id)
            wins = user.get('casino_wins', 0) if user else 0
            losses = user.get('casino_losses', 0) if user else 0
            total_won = user.get('casino_total_won', 0) if user else 0
            total_lost = user.get('casino_total_lost', 0) if user else 0
            total_games = user.get('total_games', 0) if user else 0
            
            win_rate = (wins / total_games * 100) if total_games > 0 else 0
            
            await callback.message.answer(
                f"📊 Статистика казино\n\n"
                f"🎮 Всего игр: {total_games}\n"
                f"🏆 Побед: {wins}\n"
                f"❌ Поражений: {losses}\n"
                f"📈 Процент побед: {win_rate:.1f}%\n"
                f"💰 Выиграно: {total_won} 🍇\n"
                f"💸 Проиграно: {total_lost} 🍇\n\n"
                f"Итог: {total_won - total_lost} 🍇"
            )
            await callback.answer()
            return
        
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        if balance < CASINO_MIN_BET:
            await callback.answer(f"❌ Мин {CASINO_MIN_BET} 🍇", show_alert=True)
            return
        
        keyboard = InlineKeyboardBuilder()
        bets = [10, 50, 100, 500, 1000, 5000]
        for bet in bets:
            if bet <= balance:
                keyboard.button(text=f"{bet} 🍇", callback_data=f"bet_{action}_{bet}")
        keyboard.button(text="Своя сумма", callback_data=f"bet_{action}_custom")
        keyboard.adjust(3)
        
        game = CASINO_GAMES.get(action, {})
        luck = user.get('luck_charm', 0) if user else 0
        actual_chance = game.get('win_chance', 0.5) + (luck * 0.05)
        
        await callback.message.answer(
            f"{game.get('name', 'Игра')}\n\n"
            f"Шанс победы: {actual_chance*100:.1f}%\n"
            f"Выигрыш: x{game.get('multiplier', 2)}\n"
            f"Описание: {game.get('description', '')}\n\n"
            f"Баланс: {balance} 🍇\n\n"
            f"Выберите ставку:",
            reply_markup=keyboard.as_markup()
        )
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_casino: {e}")

@dp.callback_query(lambda c: c.data.startswith("bet_"))
async def callback_bet(callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        parts = callback.data.replace("bet_", "").split("_")
        game = parts[0]
        bet_amount = parts[1]
        
        if bet_amount == "custom":
            await callback.message.answer("Введите сумму (10-10000):")
            await callback.answer()
            return
        
        bet_amount = int(bet_amount)
        user = await get_user(user_id)
        balance = user.get('balance', 0) if user else 0
        
        if bet_amount < CASINO_MIN_BET or bet_amount > CASINO_MAX_BET:
            await callback.answer(f"❌ Ставка {CASINO_MIN_BET}-{CASINO_MAX_BET}", show_alert=True)
            return
        
        if balance < bet_amount:
            await callback.answer("❌ Недостаточно 🍇", show_alert=True)
            return
        
        await update_balance(user_id, -bet_amount)
        
        game_config = CASINO_GAMES.get(game, {})
        win_chance = game_config.get('win_chance', 0.5)
        win_multiplier = game_config.get('multiplier', 2)
        luck = user.get('luck_charm', 0) if user else 0
        
        actual_chance = win_chance + (luck * 0.05)
        
        if game == "slots":
            await animate_slots(callback.message, bet_amount, actual_chance, win_multiplier, user_id, luck)
        elif game == "dice":
            await animate_dice(callback.message, bet_amount, actual_chance, win_multiplier, user_id, luck)
        elif game == "coin":
            await animate_coin(callback.message, bet_amount, actual_chance, win_multiplier, user_id, luck)
        elif game == "wheel":
            await animate_wheel(callback.message, bet_amount, actual_chance, win_multiplier, user_id, luck)
        elif game == "blackjack":
            await animate_blackjack(callback.message, bet_amount, actual_chance, win_multiplier, user_id, luck)
        
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_bet: {e}")
        await callback.message.answer("❌ Ошибка игры.")

async def animate_slots(message, bet, win_chance, multiplier, user_id, luck):
    frames = 15
    delay = 0.3
    
    await message.answer("🎰 Запускаем слоты...\n\n| ❓ |  | ❓ |")
    
    for i in range(frames):
        r1 = random.choice(SLOT_EMOJIS)
        r2 = random.choice(SLOT_EMOJIS)
        r3 = random.choice(SLOT_EMOJIS)
        
        progress = "▰" * (i // 3) + "▱" * (5 - i // 3)
        
        await asyncio.sleep(delay)
        try:
            await message.edit_text(f"🎰 Крутим барабаны...\n\n| {r1} | {r2} | {r3} |\n\n{progress}")
        except:
            pass
    
    won = random.random() < win_chance
    
    if won:
        symbol = random.choice(SLOT_EMOJIS)
        win_amount = int(bet * multiplier)
        await update_balance(user_id, win_amount)
        await update_casino_stats(user_id, True, bet)
        
        await message.edit_text(
            f"🎰 | {symbol} | {symbol} | {symbol} |\n\n"
            f"🎉 ПОБЕДА! 🎉\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Выигрыш: {win_amount} 🍇\n"
            f"Прибыль: +{win_amount - bet} 🍇\n\n"
            f"🍀 Талисман: +{luck * 5}% к шансу"
        )
    else:
        r1 = random.choice(SLOT_EMOJIS)
        r2 = random.choice(SLOT_EMOJIS)
        r3 = random.choice(SLOT_EMOJIS)
        await update_casino_stats(user_id, False, bet)
        
        await message.edit_text(
            f"🎰 | {r1} | {r2} | {r3} |\n\n"
            f"❌ Проигрыш\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Попробуй ещё! 🍀"
        )

async def animate_dice(message, bet, win_chance, multiplier, user_id, luck):
    frames = 12
    delay = 0.35
    
    await message.answer("🎲 Бросаем кубики...\n\n⚀")
    
    for i in range(frames):
        dice = random.choice(DICE_EMOJIS)
        progress = "▰" * (i // 2) + "▱" * (6 - i // 2)
        
        await asyncio.sleep(delay)
        try:
            await message.edit_text(f"🎲 Кубик крутится...\n\n{dice}\n\n{progress}")
        except:
            pass
    
    won = random.random() < win_chance
    dice = random.choice(DICE_EMOJIS)
    
    if won:
        win_amount = int(bet * multiplier)
        await update_balance(user_id, win_amount)
        await update_casino_stats(user_id, True, bet)
        
        await message.edit_text(
            f"🎲 {dice}\n\n"
            f"🎉 ПОБЕДА! 🎉\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Выигрыш: {win_amount} 🍇\n"
            f"Прибыль: +{win_amount - bet} 🍇"
        )
    else:
        await update_casino_stats(user_id, False, bet)
        
        await message.edit_text(
            f"🎲 {dice}\n\n"
            f"❌ Проигрыш\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Попробуй ещё! 🍀"
        )

async def animate_coin(message, bet, win_chance, multiplier, user_id, luck):
    frames = 10
    delay = 0.4
    
    await message.answer("🪙 Подбрасываем монетку...\n\n🔄")
    
    for i in range(frames):
        side = random.choice(COIN_SIDES)
        progress = "▰" * (i // 2) + "▱" * (5 - i // 2)
        
        await asyncio.sleep(delay)
        try:
            await message.edit_text(f"🪙 Монетка летит...\n\n{side}\n\n{progress}")
        except:
            pass
    
    won = random.random() < win_chance
    result = random.choice(COIN_SIDES)
    
    if won:
        win_amount = int(bet * multiplier)
        await update_balance(user_id, win_amount)
        await update_casino_stats(user_id, True, bet)
        
        await message.edit_text(
            f"🪙 {result}\n\n"
            f"🎉 ПОБЕДА! 🎉\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Выигрыш: {win_amount} 🍇\n"
            f"Прибыль: +{win_amount - bet} 🍇"
        )
    else:
        await update_casino_stats(user_id, False, bet)
        
        await message.edit_text(
            f"🪙 {result}\n\n"
            f"❌ Проигрыш\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Попробуй ещё! 🍀"
        )

async def animate_wheel(message, bet, win_chance, multiplier, user_id, luck):
    frames = 20
    delay = 0.25
    
    await message.answer("🎡 Крутим колесо...\n\n🍇")
    
    for i in range(frames):
        segment = random.choice(WHEEL_SEGMENTS)
        progress = "▰" * (i // 4) + "▱" * (5 - i // 4)
        
        await asyncio.sleep(delay)
        try:
            await message.edit_text(f"🎡 Колесо крутится...\n\n{segment}\n\n{progress}")
        except:
            pass
    
    won = random.random() < win_chance
    result = random.choice(WHEEL_SEGMENTS)
    
    if won:
        win_amount = int(bet * multiplier)
        await update_balance(user_id, win_amount)
        await update_casino_stats(user_id, True, bet)
        
        await message.edit_text(
            f"🎡 {result}\n\n"
            f"🎉 ПОБЕДА! 🎉\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Выигрыш: {win_amount} 🍇\n"
            f"Прибыль: +{win_amount - bet} 🍇"
        )
    else:
        await update_casino_stats(user_id, False, bet)
        
        await message.edit_text(
            f"🎡 {result}\n\n"
            f"❌ Проигрыш\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Попробуй ещё! 🍀"
        )

async def animate_blackjack(message, bet, win_chance, multiplier, user_id, luck):
    frames = 8
    delay = 0.5
    
    await message.answer("🃏 Раздаём карты...\n\n 🂠\n\nvs\n\n🂠")
    
    player_cards = []
    dealer_cards = []
    
    for i in range(frames):
        player_cards.append(random.choice(["🂡", "🂢", "", "🂤", "🂥", "🂦", "🂧", "", "🂩", "🂪", "🂫", "", "🂭"]))
        dealer_cards.append(random.choice(["🂡", "🂢", "", "🂤", "🂥", "🂦", "🂧", "", "🂩", "🂪", "🂫", "", "🂭"]))
        
        player_display = " ".join(player_cards[-3:]) if len(player_cards) > 3 else " ".join(player_cards)
        dealer_display = " ".join(dealer_cards[-2:]) if len(dealer_cards) > 2 else " ".join(dealer_cards)
        
        progress = "▰" * (i // 2) + "▱" * (4 - i // 2)
        
        await asyncio.sleep(delay)
        try:
            await message.edit_text(f"🃏 Блэкджек...\n\nИгрок: {player_display}\n\nДилер: {dealer_display}\n\n{progress}")
        except:
            pass
    
    won = random.random() < win_chance
    
    if won:
        win_amount = int(bet * multiplier)
        await update_balance(user_id, win_amount)
        await update_casino_stats(user_id, True, bet)
        
        await message.edit_text(
            f"🃏 Блэкджек!\n\n"
            f"🎉 ПОБЕДА! 🎉\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Выигрыш: {win_amount} 🍇\n"
            f"Прибыль: +{win_amount - bet} 🍇"
        )
    else:
        await update_casino_stats(user_id, False, bet)
        
        await message.edit_text(
            f"🃏 Блэкджек\n\n"
            f"❌ Проигрыш\n\n"
            f"Ставка: {bet} 🍇\n"
            f"Попробуй ещё! 🍀"
        )

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
        link = f"https://t.me/{bot_username}?start={ref_code}"
        
        invited = user.get('total_invited', 0) if user else 0
        passive = user.get('passive_income', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🔗 Копировать", url=link)
        keyboard.adjust(1)
        
        await message.answer(
            f"👥 Пригласи друзей!\n\n"
            f"Ссылка:\n{link}\n\n"
            f"+{REFERRAL_BONUS} 🍇 за друга\n"
            f"+{REFERRAL_PERCENT}% от сбора\n\n"
            f"Приглашено: {invited}\n"
            f"Пассивно: {passive} 🍇",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /пригласить: {e}")

@dp.message(Command("бонус"))
async def cmd_bonus(message: Message):
    try:
        user_id = message.from_user.id
        await add_user(user_id)
        now = int(time.time())
        user = await get_user(user_id)
        last = user.get('last_bonus', 0) if user else 0
        
        cooldown = (2 if user and user.get('bonus_2h') else BONUS_HOURS) * 3600
        
        if now - last < cooldown:
            hours = (cooldown - (now - last)) // 3600
            await message.answer(f"⏰ Бонус готов через {hours}ч")
            return
        
        await update_balance(user_id, BONUS_AMOUNT)
        await update_bonus_time(user_id, now)
        new_user = await get_user(user_id)
        await message.answer(f"🎁 +{BONUS_AMOUNT} 🍇\nВсего: {new_user['balance']}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /бонус: {e}")

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    try:
        top = await get_top_users(10)
        if not top:
            await message.answer("📊 Пока нет игроков")
            return
        
        text = "🏆 Топ игроков\n\n"
        for i, row in enumerate(top, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            try:
                u = await bot.get_chat(row['user_id'])
                name = u.first_name[:20]
            except:
                name = f"User{row['user_id']}"
            text += f"{medal} {name} — {row['balance']} 🍇\n"
        await message.answer(text)
    except Exception as e:
        logging.error(f"❌ Ошибка в /топ: {e}")

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    try:
        await message.answer(
            f"📚 Справка\n\n"
            f"🍇 Сбор:\n"
            f"/сбор — 15 🍇 каждые 4 часа\n"
            f"/баланс — проверить\n"
            f"/бонус — +50 🍇\n\n"
            f"🎰 Казино:\n"
            f"/казино — все игры\n"
            f"🎰 Слоты — x3 (35%)\n"
            f"🎲 Кубики — x2 (50%)\n"
            f"🪙 Монетка — x2 (50%)\n"
            f"🎡 Колесо — x2.5 (40%)\n"
            f"🃏 Блэкджек — x2 (45%)\n\n"
            f"👥 Другое:\n"
            f"/магазин — улучшения\n"
            f"/пригласить — ссылка\n"
            f"/топ — рейтинг\n"
            f"/помощь — справка"
        )
    except Exception as e:
        logging.error(f"❌ Ошибка в /помощь: {e}")

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    try:
        total = await get_total_users()
        grapes = await get_total_grapes()
        await message.answer(f"📊 Статистика\n\n👥 Игроков: {total}\n🍇 Всего: {grapes or 0}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /статистика: {e}")

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
