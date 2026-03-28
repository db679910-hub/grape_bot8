import asyncio
import logging
import time
import os
import asyncpg
import random
import string
import json
from aiogram import Bot, Dispatcher, F
from aiogram.filters import CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# Администраторы (замените на свой Telegram ID)
ADMIN_IDS = [123456789]  # Укажите ваш Telegram ID

GRAPE_REWARD = 15
COOLDOWN_HOURS = 4
COOLDOWN_SECONDS = COOLDOWN_HOURS * 3600
BONUS_AMOUNT = 50
BONUS_HOURS = 4
REFERRAL_BONUS = 100
REFERRAL_PERCENT = 10
GIFT_MIN = 10
GIFT_MAX = 10000
GIFT_COMMISSION = 5

CROPS = {
    "grape": {"name": "🍇 Виноград", "cost": 0, "reward": 15, "growth_time": 14400, "xp": 10},
    "strawberry": {"name": "🍓 Клубника", "cost": 50, "reward": 80, "growth_time": 7200, "xp": 25},
    "corn": {"name": "🌽 Кукуруза", "cost": 100, "reward": 180, "growth_time": 10800, "xp": 40},
    "tomato": {"name": "🍅 Томат", "cost": 200, "reward": 400, "growth_time": 18000, "xp": 60},
    "pumpkin": {"name": "🎃 Тыква", "cost": 500, "reward": 1200, "growth_time": 28800, "xp": 100},
    "melon": {"name": "🍈 Дыня", "cost": 1000, "reward": 2500, "growth_time": 43200, "xp": 200},
    "pineapple": {"name": "🍍 Ананас", "cost": 2000, "reward": 5500, "growth_time": 57600, "xp": 350},
    "coconut": {"name": "🥥 Кокос", "cost": 3500, "reward": 8000, "growth_time": 72000, "xp": 450},
    "diamond_grape": {"name": "💎 Алмазный виноград", "cost": 5000, "reward": 15000, "growth_time": 86400, "xp": 500},
    "golden_apple": {"name": "🍎 Золотое яблоко", "cost": 10000, "reward": 35000, "growth_time": 172800, "xp": 1000},
}

FARM_PLOTS = {
    1: {"level": 1, "plots": 3, "upgrade_cost": 0},
    2: {"level": 2, "plots": 5, "upgrade_cost": 500},
    3: {"level": 3, "plots": 7, "upgrade_cost": 1500},
    4: {"level": 4, "plots": 10, "upgrade_cost": 3000},
    5: {"level": 5, "plots": 15, "upgrade_cost": 6000},
    6: {"level": 6, "plots": 20, "upgrade_cost": 12000},
    7: {"level": 7, "plots": 25, "upgrade_cost": 25000},
    8: {"level": 8, "plots": 30, "upgrade_cost": 50000},
    9: {"level": 9, "plots": 40, "upgrade_cost": 100000},
}

TOOLS = {
    "hoe": {"name": "🔓 Мотыга", "price": 200, "effect": "growth_speed", "bonus": 1.1},
    "watering_can": {"name": "🚿 Лейка", "price": 500, "effect": "growth_speed", "bonus": 1.2},
    "fertilizer": {"name": "💩 Удобрение", "price": 1000, "effect": "growth_speed", "bonus": 1.3},
    "tractor": {"name": "🚜 Трактор", "price": 3000, "effect": "growth_speed", "bonus": 1.5},
    "harvester": {"name": "🌾 Комбайн", "price": 7500, "effect": "growth_speed", "bonus": 1.7},
    "drone": {"name": "🚁 Дрон", "price": 15000, "effect": "growth_speed", "bonus": 2.0},
    "ai_system": {"name": "🤖 ИИ Система", "price": 35000, "effect": "growth_speed", "bonus": 2.5},
}

GIFT_CATALOG = {
    "chocolate": {"name": "🍫 Шоколадка", "price": 50, "rarity": "common"},
    "flower": {"name": "🌹 Роза", "price": 100, "rarity": "common"},
    "balloon": {"name": "🎈 Шарик", "price": 75, "rarity": "common"},
    "cupcake": {"name": "🧁 Капкейк", "price": 150, "rarity": "common"},
    "teddy": {"name": "🧸 Мишка", "price": 300, "rarity": "rare"},
    "ring": {"name": "💍 Кольцо", "price": 500, "rarity": "rare"},
    "necklace": {"name": "📿 Ожерелье", "price": 750, "rarity": "rare"},
    "car": {"name": "🏎️ Спорткар", "price": 2000, "rarity": "epic"},
    "house": {"name": "🏡 Дом", "price": 5000, "rarity": "epic"},
    "yacht": {"name": "🛥️ Яхта", "price": 10000, "rarity": "epic"},
    "diamond": {"name": "💎 Алмаз", "price": 15000, "rarity": "legendary"},
    "crown": {"name": "👑 Корона", "price": 25000, "rarity": "legendary"},
    "rocket": {"name": "🚀 Ракета", "price": 50000, "rarity": "legendary"},
}

BOOSTERS = {
    "speed_1h": {"name": "⚡ Ускорение 1ч", "price": 50, "duration": 3600, "effect": "growth_speed", "bonus": 1.5},
    "speed_4h": {"name": "⚡ Ускорение 4ч", "price": 150, "duration": 14400, "effect": "growth_speed", "bonus": 1.5},
    "speed_12h": {"name": "⚡ Ускорение 12ч", "price": 350, "duration": 43200, "effect": "growth_speed", "bonus": 1.5},
    "speed_24h": {"name": "⚡ Ускорение 24ч", "price": 600, "duration": 86400, "effect": "growth_speed", "bonus": 1.5},
    "yield_1h": {"name": "📈 Урожай 1ч", "price": 75, "duration": 3600, "effect": "yield", "bonus": 1.3},
    "yield_4h": {"name": "📈 Урожай 4ч", "price": 200, "duration": 14400, "effect": "yield", "bonus": 1.3},
    "yield_12h": {"name": "📈 Урожай 12ч", "price": 500, "duration": 43200, "effect": "yield", "bonus": 1.3},
    "yield_24h": {"name": "📈 Урожай 24ч", "price": 800, "duration": 86400, "effect": "yield", "bonus": 1.3},
    "income_1h": {"name": "💰 Доход 1ч", "price": 120, "duration": 3600, "effect": "passive_income", "bonus": 2.0},
    "income_4h": {"name": "💰 Доход 4ч", "price": 350, "duration": 14400, "effect": "passive_income", "bonus": 2.0},
    "income_12h": {"name": "💰 Доход 12ч", "price": 800, "duration": 43200, "effect": "passive_income", "bonus": 2.0},
    "income_24h": {"name": "💰 Доход 24ч", "price": 1200, "duration": 86400, "effect": "passive_income", "bonus": 2.0},
    "super_booster_1h": {"name": "🌟 СУПЕР 1ч", "price": 500, "duration": 3600, "effect": "all", "bonus": 1.5},
    "super_booster_4h": {"name": "🌟 СУПЕР 4ч", "price": 1500, "duration": 14400, "effect": "all", "bonus": 1.5},
    "super_booster_24h": {"name": "🌟 СУПЕР 24ч", "price": 5000, "duration": 86400, "effect": "all", "bonus": 1.5},
}

HOUSES = {
    "tent": {"name": "⛺ Палатка", "level": 1, "price": 0, "passive_income": 1},
    "hut": {"name": "🛖 Хижина", "level": 2, "price": 1000, "passive_income": 5},
    "cottage": {"name": "🏡 Коттедж", "level": 3, "price": 5000, "passive_income": 20},
    "mansion": {"name": "🏰 Особняк", "level": 4, "price": 15000, "passive_income": 50},
    "castle": {"name": "🏯 Замок", "level": 5, "price": 50000, "passive_income": 150},
    "palace": {"name": "👑 Дворец", "level": 6, "price": 150000, "passive_income": 500},
    "sky_mansion": {"name": "☁️ Небесный особняк", "level": 7, "price": 500000, "passive_income": 1500},
    "dragon_castle": {"name": "🐉 Замок дракона", "level": 8, "price": 1500000, "passive_income": 5000},
    "god_palace": {"name": "✨ Дворец богов", "level": 9, "price": 5000000, "passive_income": 15000},
}

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
pool = None

SHOP_ITEMS = {
    "auto_collect": {"name": "🔄 Авто-сбор", "price": 1000, "desc": "Без кулдауна", "type": "upgrade"},
    "double_grapes": {"name": "📈 Умножение x2", "price": 2500, "desc": "x2 винограда", "type": "upgrade"},
    "triple_grapes": {"name": "📈 Умножение x3", "price": 7500, "desc": "x3 винограда", "type": "upgrade"},
    "bonus_2h": {"name": "⏰ Бонус 2ч", "price": 500, "desc": "Бонус каждые 2 часа", "type": "upgrade"},
    "bonus_1h": {"name": "⏰ Бонус 1ч", "price": 800, "desc": "Бонус каждые 1 час", "type": "upgrade"},
    "skin_wine": {"name": "🍷 Скин Вино", "price": 500, "desc": "Меняет эмодзи", "type": "skin"},
    "skin_diamond": {"name": "💎 Скин Алмаз", "price": 1500, "desc": "Меняет эмодзи", "type": "skin"},
    "skin_gold": {"name": "🏆 Скин Золото", "price": 3000, "desc": "Меняет эмодзи", "type": "skin"},
    "restore": {"name": "💚 Восстановление", "price": 200, "desc": "Сброс кулдауна", "type": "consumable"},
    "lucky_charm": {"name": "🍀 Талисман удачи", "price": 750, "desc": "+5% к шансу крита", "type": "consumable"},
}

def generate_ref_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

async def init_db():
    global pool
    try:
        pool = await asyncpg.create_pool(DATABASE_URL)
        logging.info("База данных подключена!")
        
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
                    username VARCHAR(100),
                    farm_level INTEGER DEFAULT 1,
                    farm_xp INTEGER DEFAULT 0,
                    farm_plots TEXT DEFAULT '["empty", "empty", "empty"]',
                    tools TEXT DEFAULT '[]',
                    house_level INTEGER DEFAULT 1,
                    house_xp INTEGER DEFAULT 0,
                    last_passive_claim INTEGER DEFAULT 0,
                    boosters TEXT DEFAULT '[]',
                    inventory TEXT DEFAULT '[]',
                    gifts_sent INTEGER DEFAULT 0,
                    gifts_received INTEGER DEFAULT 0,
                    total_harvest INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0
                )
            """)
            logging.info("Таблица users проверена!")
            
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS inventory TEXT DEFAULT '[]'")
                logging.info("Колонка inventory добавлена/проверена!")
            except Exception as e:
                logging.info(f"Колонка inventory уже существует: {e}")
            
            columns_to_add = [
                ("farm_plots", "TEXT DEFAULT '[\"empty\", \"empty\", \"empty\"]'"),
                ("tools", "TEXT DEFAULT '[]'"),
                ("boosters", "TEXT DEFAULT '[]'"),
                ("farm_level", "INTEGER DEFAULT 1"),
                ("farm_xp", "INTEGER DEFAULT 0"),
                ("house_level", "INTEGER DEFAULT 1"),
                ("house_xp", "INTEGER DEFAULT 0"),
                ("last_passive_claim", "INTEGER DEFAULT 0"),
                ("gifts_sent", "INTEGER DEFAULT 0"),
                ("gifts_received", "INTEGER DEFAULT 0"),
                ("total_harvest", "INTEGER DEFAULT 0"),
                ("total_earned", "INTEGER DEFAULT 0")
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    await conn.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                    logging.info(f"Колонка {col_name} добавлена/проверена!")
                except Exception as e:
                    logging.info(f"Колонка {col_name} уже существует: {e}")
        
        logging.info("База данных готова!")
    except Exception as e:
        logging.error(f"Ошибка БД: {e}")
        raise

async def reset_database():
    """Полный сброс базы данных"""
    try:
        async with pool.acquire() as conn:
            await conn.execute("DROP TABLE IF EXISTS users CASCADE")
            logging.warning("База данных сброшена!")
        await init_db()
        return True
    except Exception as e:
        logging.error(f"Ошибка сброса БД: {e}")
        return False

async def reset_user_progress(user_id):
    """Сброс прогресса конкретного пользователя"""
    try:
        async with pool.acquire() as conn:
            await conn.execute("""
                UPDATE users SET 
                    balance = 0,
                    last_collect = 0,
                    last_bonus = 0,
                    farm_level = 1,
                    farm_xp = 0,
                    farm_plots = '["empty", "empty", "empty"]',
                    house_level = 1,
                    house_xp = 0,
                    last_passive_claim = 0,
                    boosters = '[]',
                    inventory = '[]',
                    total_harvest = 0,
                    total_earned = 0
                WHERE user_id = $1
            """, user_id)
            return True
    except Exception as e:
        logging.error(f"Ошибка сброса пользователя: {e}")
        return False

async def get_user(user_id):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                result = dict(row)
                try:
                    result['farm_plots'] = json.loads(row['farm_plots']) if row['farm_plots'] else []
                    result['boosters'] = json.loads(row['boosters']) if row['boosters'] else []
                    result['inventory'] = json.loads(row['inventory']) if row['inventory'] else []
                except:
                    pass
                return result
            return None
    except Exception as e:
        logging.error(f"Ошибка get_user: {e}")
        return None

async def get_user_by_username(username):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username.lower())
            return dict(row) if row else None
    except:
        return None

async def add_user(user_id, ref_code=None, username=None):
    try:
        async with pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if existing:
                if username:
                    await conn.execute("UPDATE users SET username = $1 WHERE user_id = $2", username.lower(), user_id)
                return dict(existing)
            
            my_ref_code = generate_ref_code()
            inviter_id = None
            
            if ref_code:
                inviter = await conn.fetchrow("SELECT user_id FROM users WHERE ref_code = $1", ref_code)
                if inviter:
                    inviter_id = inviter['user_id']
            
            await conn.execute(
                "INSERT INTO users (user_id, ref_code, invited_by, username, farm_plots, tools, boosters, inventory) VALUES ($1, $2, $3, $4, $5, $6, $7, $8) ON CONFLICT (user_id) DO NOTHING",
                user_id, my_ref_code, inviter_id, username.lower() if username else None, 
                '["empty", "empty", "empty"]', '[]', '[]', '[]'
            )
            
            if inviter_id:
                await conn.execute("UPDATE users SET balance = balance + $1, total_invited = total_invited + 1 WHERE user_id = $2", REFERRAL_BONUS, inviter_id)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", REFERRAL_BONUS, user_id)
            
            return await get_user(user_id)
    except Exception as e:
        logging.error(f"Ошибка add_user: {e}")
        return None

async def update_balance(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def update_collect_time(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_collect = $1 WHERE user_id = $2", timestamp, user_id)

async def update_bonus_time(user_id, timestamp):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_bonus = $1 WHERE user_id = $2", timestamp, user_id)

async def add_passive_income(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

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
        elif item_id in ["skin_wine", "skin_diamond", "skin_gold"]:
            skin = "wine" if item_id == "skin_wine" else "diamond" if item_id == "skin_diamond" else "gold"
            await conn.execute("UPDATE users SET skin = $1 WHERE user_id = $2", skin, user_id)

async def get_active_boosters(user_id):
    user = await get_user(user_id)
    if not user:
        return []
    
    boosters = user.get('boosters', [])
    now = int(time.time())
    
    return [b for b in boosters if isinstance(b, dict) and b.get('expires_at', 0) > now]

async def get_booster_effect(user_id, effect_type):
    boosters = await get_active_boosters(user_id)
    multiplier = 1.0
    
    for booster in boosters:
        if booster.get('effect') == effect_type or booster.get('effect') == 'all':
            multiplier = max(multiplier, booster.get('bonus', 1.0))
    
    return multiplier

async def plant_crop(user_id, plot_index, crop_id):
    try:
        user = await get_user(user_id)
        if not user:
            logging.error(f"Пользователь {user_id} не найден")
            return False, "Пользователь не найден"
        
        plots = user.get('farm_plots', ["empty", "empty", "empty"])
        logging.info(f"Грядки до посадки: {plots}")
        
        if plot_index < 0 or plot_index >= len(plots):
            return False, f"Неверный номер грядки (доступно: 1-{len(plots)})"
        
        crop = CROPS.get(crop_id)
        if not crop:
            logging.error(f"Культура {crop_id} не найдена")
            return False, f"Культура не найдена! Доступные: {', '.join(CROPS.keys())}"
        
        if user['balance'] < crop['cost']:
            return False, f"Недостаточно винограда! Нужно {crop['cost']}"
        
        if plots[plot_index] != "empty" and isinstance(plots[plot_index], dict):
            return False, "Грядка занята! Сначала соберите урожай."
        
        plots[plot_index] = {
            "crop": crop_id,
            "planted_at": int(time.time()),
            "ready": False
        }
        
        logging.info(f"Грядки после посадки: {plots}")
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, farm_plots = $2 WHERE user_id = $3", 
                crop['cost'], json.dumps(plots), user_id
            )
        
        logging.info(f"Пользователь {user_id} посадил {crop_id} на грядку {plot_index}")
        return True, f"{crop['name']} посажен на грядку {plot_index + 1}!"
        
    except Exception as e:
        logging.error(f"Ошибка plant_crop: {e}")
        return False, f"Ошибка посадки: {str(e)}"

async def harvest_crop(user_id, plot_index):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        plots = user.get('farm_plots', [])
        
        if plot_index < 0 or plot_index >= len(plots):
            return False, "Неверный номер грядки"
        
        plot = plots[plot_index]
        if not plot or plot == "empty" or not isinstance(plot, dict):
            return False, "Грядка пуста"
        
        crop = CROPS.get(plot.get('crop'))
        if not crop:
            return False, "Ошибка культуры"
        
        now = int(time.time())
        speed_bonus = await get_booster_effect(user_id, 'growth_speed')
        growth_time = int(crop['growth_time'] / speed_bonus)
        ready_time = plot.get('planted_at', 0) + growth_time
        
        if now < ready_time:
            remaining = ready_time - now
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            return False, f"Ещё растёт! {hours}ч {minutes}м"
        
        reward = crop['reward']
        yield_bonus = await get_booster_effect(user_id, 'yield')
        reward = int(reward * yield_bonus)
        
        if random.random() < 0.1:
            reward = int(reward * 2)
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, total_harvest = total_harvest + 1, total_earned = total_earned + $1 WHERE user_id = $2", 
                reward, user_id
            )
            await conn.execute("UPDATE users SET farm_xp = farm_xp + $1 WHERE user_id = $2", crop['xp'], user_id)
            
            plots[plot_index] = "empty"
            await conn.execute("UPDATE users SET farm_plots = $1 WHERE user_id = $2", json.dumps(plots), user_id)
        
        return True, f"Собрано: {reward} (+{crop['xp']} XP)"
    except Exception as e:
        logging.error(f"Ошибка harvest_crop: {e}")
        return False, "Ошибка сбора"

async def upgrade_farm_level(user_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_level = user.get('farm_level', 1)
        
        if current_level >= 9:
            return False, "Максимальный уровень фермы!"
        
        next_level = current_level + 1
        upgrade_info = FARM_PLOTS.get(next_level)
        
        if not upgrade_info or user['balance'] < upgrade_info['upgrade_cost']:
            return False, f"Нужно {upgrade_info['upgrade_cost'] if upgrade_info else 0}"
        
        new_plots = ["empty"] * upgrade_info['plots']
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, farm_level = $2, farm_plots = $3 WHERE user_id = $4",
                upgrade_info['upgrade_cost'], next_level, json.dumps(new_plots), user_id
            )
        
        return True, f"Ферма улучшена до уровня {next_level}!"
    except Exception as e:
        logging.error(f"Ошибка upgrade_farm_level: {e}")
        return False, "Ошибка улучшения"

async def upgrade_house_level(user_id, house_id):
    try:
        house = HOUSES.get(house_id)
        if not house:
            return False, "Дом не найден"
        
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_level = user.get('house_level', 1)
        
        if house['level'] <= current_level:
            return False, "Нужно улучшить до следующего уровня!"
        
        if house['level'] != current_level + 1:
            return False, "Улучшайте по порядку!"
        
        if user['balance'] < house['price']:
            return False, f"Нужно {house['price']}"
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance - $1, house_level = $2, house_xp = house_xp + $3 WHERE user_id = $4",
                              house['price'], house['level'], house['level'] * 100, user_id)
        
        return True, f"Дом улучшен до {house['name']}!"
    except Exception as e:
        logging.error(f"Ошибка upgrade_house_level: {e}")
        return False, "Ошибка улучшения дома"

async def claim_passive_income(user_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        house_level = user.get('house_level', 1)
        last_claim = user.get('last_passive_claim', 0)
        
        house = None
        for h in HOUSES.values():
            if h['level'] == house_level:
                house = h
                break
        
        if not house:
            house = HOUSES['tent']
        
        now = int(time.time())
        hours_passed = (now - last_claim) // 3600
        
        if hours_passed < 1:
            minutes_left = 60 - ((now - last_claim) // 60)
            return False, f"Доход через {minutes_left} мин"
        
        base_income = house['passive_income'] * hours_passed
        income_bonus = await get_booster_effect(user_id, 'passive_income')
        total_income = int(base_income * income_bonus)
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance + $1, last_passive_claim = $2 WHERE user_id = $3",
                              total_income, now, user_id)
        
        return True, f"Получено: {total_income} (за {hours_passed}ч)"
    except Exception as e:
        logging.error(f"Ошибка claim_passive_income: {e}")
        return False, "Ошибка получения дохода"

async def add_to_inventory(user_id, item_id):
    try:
        logging.info(f"Добавляем {item_id} в инвентарь пользователя {user_id}")
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT inventory FROM users WHERE user_id = $1", user_id)
            
            if not row:
                logging.error(f"Пользователь {user_id} не найден")
                return False
            
            inventory_json = row['inventory']
            logging.info(f"Текущий инвентарь в БД: {inventory_json}")
            
            if inventory_json is None or inventory_json == '':
                inventory = []
            else:
                try:
                    inventory = json.loads(inventory_json)
                except json.JSONDecodeError as e:
                    logging.error(f"Ошибка парсинга JSON: {e}")
                    inventory = []
            
            logging.info(f"Инвентарь до добавления: {inventory}")
            
            found = False
            for item in inventory:
                if isinstance(item, dict) and item.get('item_id') == item_id:
                    old_qty = item.get('quantity', 1)
                    item['quantity'] = old_qty + 1
                    found = True
                    logging.info(f"Обновлён предмет {item_id}: {old_qty} -> {item['quantity']}")
                    break
            
            if not found:
                inventory.append({"item_id": item_id, "quantity": 1})
                logging.info(f"Добавлен новый предмет: {item_id}")
            
            new_inventory_json = json.dumps(inventory, ensure_ascii=False)
            logging.info(f"Новый инвентарь: {new_inventory_json}")
            
            await conn.execute(
                "UPDATE users SET inventory = $1 WHERE user_id = $2",
                new_inventory_json,
                user_id
            )
            
            logging.info(f"Инвентарь сохранён для пользователя {user_id}")
            return True
            
    except Exception as e:
        logging.error(f"Ошибка add_to_inventory: {e}")
        import traceback
        logging.error(traceback.format_exc())
        return False

async def send_gift(from_user_id, to_user_id, amount):
    async with pool.acquire() as conn:
        commission = int(amount * GIFT_COMMISSION / 100)
        received = amount - commission
        
        await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, from_user_id)
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", received, to_user_id)
        await conn.execute("UPDATE users SET gifts_sent = gifts_sent + 1, total_gifted = total_gifted + $1 WHERE user_id = $2", amount, from_user_id)
        await conn.execute("UPDATE users SET gifts_received = gifts_received + 1, total_received = total_received + $1 WHERE user_id = $2", received, to_user_id)
        
        return commission, received

async def get_skin_emoji(skin):
    return {"grape": "🍇", "wine": "🍷", "diamond": "💎", "gold": "🏆"}.get(skin, "🍇")

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

# =============================================================================
# КОМАНДЫ БЕЗ СЛЭША
# =============================================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code, username)
        
        if user:
            text = (
                f"🍇 Добро пожаловать в Виноградную Ферму! 🍇\n\n"
                f"Привет, {message.from_user.first_name}! 👋\n\n"
                f"🌟 Что ты можешь делать:\n"
                f"🌱 Выращивать виноград и другие культуры\n"
                f"🏠 Строить дома и получать пассивный доход\n"
                f"🎁 Покупать подарки и обмениваться с друзьями\n"
                f"🏆 Соревноваться с другими игроками\n\n"
                f"📚 Быстрый старт:\n"
                f"1. сбор - собери свой первый виноград\n"
                f"2. ферма - посади культуры\n"
                f"3. магазин - купи улучшения\n\n"
                f"💡 Совет: Начинай с винограда - он растёт быстрее всего!\n\n"
                f"🎮 Основные команды:\n"
                f"помощь - полная справка по боту"
            )
            await message.answer(text)
        else:
            await message.answer("❌ Произошла ошибка при регистрации. Попробуйте ещё раз!")
    except Exception as e:
        logging.error(f"Ошибка cmd_start: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")

@dp.message(F.text == "сброс")
async def cmd_reset(message: Message):
    """Сброс бота (только для админов)"""
    try:
        if message.from_user.id not in ADMIN_IDS:
            await message.answer("❌ Доступ только для администраторов!")
            return
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚠️ Сбросить ВСЮ базу данных", callback_data="reset_full_db")
        keyboard.button(text="🔄 Сбросить МОЙ прогресс", callback_data="reset_my_progress")
        keyboard.button(text="❌ Отмена", callback_data="reset_cancel")
        keyboard.adjust(1)
        
        await message.answer(
            "⚠️ **СБРОС ДАННЫХ**\n\n"
            "Выберите тип сброса:\n\n"
            "⚠️ **Сбросить ВСЮ базу данных** - удалит ВСЕХ пользователей и их прогресс\n"
            "🔄 **Сбросить МОЙ прогресс** - сбросит только ваш прогресс\n\n"
            "⚠️ Это действие НЕОБРАТИМО!",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"Ошибка cmd_reset: {e}")
        await message.answer("❌ Ошибка")

@dp.callback_query(lambda c: c.data == "reset_full_db")
async def callback_reset_full_db(callback: CallbackQuery):
    """Полный сброс базы данных"""
    try:
        if callback.from_user.id not in ADMIN_IDS:
            await callback.answer("❌ Доступ запрещён!", show_alert=True)
            return
        
        await callback.answer()
        success = await reset_database()
        
        if success:
            await callback.message.answer(
                "✅ **База данных полностью сброшена!**\n\n"
                "Все пользователи и их прогресс удалены.\n"
                "Бот готов к новому запуску! 🎉"
            )
        else:
            await callback.message.answer("❌ Ошибка при сбросе базы данных")
    except Exception as e:
        logging.error(f"Ошибка callback_reset_full_db: {e}")

@dp.callback_query(lambda c: c.data == "reset_my_progress")
async def callback_reset_my_progress(callback: CallbackQuery):
    """Сброс прогресса текущего пользователя"""
    try:
        await callback.answer()
        success = await reset_user_progress(callback.from_user.id)
        
        if success:
            await callback.message.answer(
                "✅ **Ваш прогресс сброшен!**\n\n"
                "Вы можете начать заново.\n"
                "Используйте команду: старт"
            )
        else:
            await callback.message.answer("❌ Ошибка при сбросе прогресса")
    except Exception as e:
        logging.error(f"Ошибка callback_reset_my_progress: {e}")

@dp.callback_query(lambda c: c.data == "reset_cancel")
async def callback_reset_cancel(callback: CallbackQuery):
    """Отмена сброса"""
    try:
        await callback.answer()
        await callback.message.answer("❌ Сброс отменён")
    except Exception as e:
        logging.error(f"Ошибка callback_reset_cancel: {e}")

@dp.message(F.text == "ферма")
async def cmd_farm(message: Message):
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала нажмите старт")
            return
        
        plots = user.get('farm_plots', ["empty", "empty", "empty"])
        farm_level = user.get('farm_level', 1)
        farm_xp = user.get('farm_xp', 0)
        balance = user.get('balance', 0)
        
        now = int(time.time())
        
        grid_text = ""
        for i, plot in enumerate(plots):
            grid_text += f"Грядка {i+1}: "
            
            if plot == "empty" or not plot or not isinstance(plot, dict):
                grid_text += "🟫 Пусто\n"
            else:
                crop = CROPS.get(plot.get('crop'))
                if crop:
                    planted = plot.get('planted_at', 0)
                    growth_time = crop['growth_time']
                    ready_time = planted + growth_time
                    
                    if now >= ready_time:
                        grid_text += f"{crop['name']} ✅ Готово!\n"
                    else:
                        remaining = ready_time - now
                        hours = remaining // 3600
                        minutes = (remaining % 3600) // 60
                        grid_text += f"{crop['name']} ⏳ {hours}ч {minutes}м\n"
                else:
                    grid_text += "🟫 Пусто\n"
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🌱 Посадить культуру", callback_data="farm_plant")
        keyboard.button(text="🚜 Улучшить ферму", callback_data="farm_upgrade")
        keyboard.button(text="📊 Информация", callback_data="farm_stats")
        keyboard.adjust(2)
        
        text = (
            f"🌾 **Ваша ферма**\n\n"
            f"👤 Уровень фермы: {farm_level}\n"
            f"✨ Опыт: {farm_xp}\n"
            f"💰 Баланс: {balance:,} 🍇\n\n"
            f"**Ваши грядки:**\n"
            f"{grid_text}\n"
            f"💡 Нажмите на кнопку, чтобы начать!"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_farm: {e}")
        await message.answer("❌ Ошибка при загрузке фермы.")

@dp.message(F.text == "сбор")
async def cmd_collect(message: Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        await add_user(user_id, username=username)
        
        now = int(time.time())
        user = await get_user(user_id)
        
        if not user:
            await message.answer("❌ Ошибка пользователя")
            return
        
        auto = user.get('auto_collect', False)
        double = user.get('double_grapes', False)
        skin = user.get('skin', 'grape')
        emoji = await get_skin_emoji(skin)
        last_time = user.get('last_collect', 0)
        cooldown = 0 if auto else COOLDOWN_SECONDS
        
        if now - last_time < cooldown:
            remaining = cooldown - (now - last_time)
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            seconds = remaining % 60
            
            if hours > 0:
                wait_text = f"{hours}ч {minutes}м"
            elif minutes > 0:
                wait_text = f"{minutes}м {seconds}с"
            else:
                wait_text = f"{seconds}с"
            
            text = (
                f"⏳ Виноград ещё растёт\n\n"
                f"🍇 Подождите: {wait_text}\n"
                f"📍 Следующий сбор: через {wait_text}\n\n"
                f"💡 Совет: купите авто-сбор в магазине!"
            )
            await message.answer(text)
            return
        
        reward = GRAPE_REWARD * (2 if double else 1)
        await update_balance(user_id, reward)
        await update_collect_time(user_id, now)
        
        new_user = await get_user(user_id)
        new_balance = new_user.get('balance', 0) if new_user else reward
        
        if user.get('invited_by'):
            passive = int(reward * REFERRAL_PERCENT / 100)
            if passive > 0:
                await add_passive_income(user['invited_by'], passive)
        
        text = (
            f"{emoji} **Сбор винограда!** {emoji}\n\n"
            f"🍇 Собрано: {reward:,}\n"
            f"💰 Ваш баланс: {new_balance:,} 🍇\n\n"
        )
        
        if double:
            text += "📈 Бонус: x2 активен!\n"
        
        if auto:
            text += "🔄 Авто-сбор: активен!\n"
        
        text += f"\n⏱ Следующий сбор: через {COOLDOWN_HOURS} ч"
        
        await message.answer(text)
        
        logging.info(f"Пользователь {user_id} собрал {reward} винограда")
        
    except Exception as e:
        logging.error(f"Ошибка cmd_collect: {e}")
        await message.answer("❌ Ошибка сбора. Попробуйте позже.")

@dp.message(F.text == "дом")
async def cmd_house(message: Message):
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user:
            await message.answer("❌ Сначала старт")
            return
        
        house_level = user.get('house_level', 1)
        house_xp = user.get('house_xp', 0)
        balance = user.get('balance', 0)
        last_claim = user.get('last_passive_claim', 0)
        
        house = HOUSES.get('tent')
        for h in HOUSES.values():
            if h['level'] == house_level:
                house = h
                break
        
        now = int(time.time())
        hours_passed = (now - last_claim) // 3600
        pending_income = house['passive_income'] * hours_passed
        
        income_bonus = await get_booster_effect(user_id, 'passive_income')
        pending_income = int(pending_income * income_bonus)
        
        next_house = None
        for h in HOUSES.values():
            if h['level'] == house_level + 1:
                next_house = h
                break
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💰 Забрать доход", callback_data="house_claim")
        if next_house:
            keyboard.button(text=f"🔨 Улучшить ({next_house['price']:,} 🍇)", callback_data=f"house_upgrade_{next_house['level']}")
        keyboard.button(text="📊 Информация", callback_data="house_stats")
        keyboard.adjust(2)
        
        text = (
            f"🏠 **{house['name']}**\n\n"
            f"📊 Уровень: {house_level}\n"
            f"✨ Опыт: {house_xp}\n"
            f"💰 Пассивный доход: {house['passive_income']:,} 🍇/час\n\n"
            f"💵 Ваш баланс: {balance:,} 🍇\n\n"
            f"🎁 **Доступно к получению:** {pending_income:,} 🍇\n"
        )
        
        if next_house:
            text += (
                f"\n🔜 **Следующий уровень:**\n"
                f"🏠 {next_house['name']}\n"
                f"💰 Стоимость: {next_house['price']:,} 🍇\n"
                f"📈 Доход: {next_house['passive_income']:,} 🍇/час"
            )
        
        text += "\n\n💡 Забирайте доход регулярно!"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_house: {e}")
        await message.answer("❌ Ошибка при загрузке информации о доме.")

@dp.message(F.text == "подарки")
async def cmd_gifts(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        
        for item_id, item in GIFT_CATALOG.items():
            keyboard.button(
                text=f"{item['name']} - {item['price']:,} 🍇", 
                callback_data=f"gift_{item_id}"
            )
        
        keyboard.adjust(2)
        
        text = (
            f"🎁 **Магазин подарков**\n\n"
            f"💰 Ваш баланс: {balance:,} 🍇\n\n"
            f"🎨 **Выберите подарок:**\n"
            f"Подарите другу радость! 🎉\n\n"
            f"💡 **Как использовать:**\n"
            f"1. Выберите подарок из списка\n"
            f"2. Купите его\n"
            f"3. Передайте другу командой передать"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_gifts: {e}")
        await message.answer("❌ Ошибка при загрузке магазина.")

@dp.message(F.text == "инвентарь")
async def cmd_inventory(message: Message):
    try:
        user_id = message.from_user.id
        logging.info(f"Пользователь {user_id} запросил инвентарь")
        
        user = await get_user(user_id)
        if not user:
            await message.answer("❌ Ошибка пользователя")
            return
        
        inventory = user.get('inventory', [])
        logging.info(f"Инвентарь пользователя: {inventory}")
        
        if not inventory or len(inventory) == 0:
            text = (
                "📦 Ваш инвентарь пуст\n\n"
                "💡 Как получить подарки:\n"
                "• Купите в подарки\n"
                "• Получите от других игроков"
            )
            await message.answer(text)
            return
        
        text = "📦 Ваш инвентарь\n\n"
        
        item_counts = {}
        for item in inventory:
            if isinstance(item, dict):
                item_id = item.get('item_id')
                quantity = item.get('quantity', 1)
                if item_id:
                    item_counts[item_id] = item_counts.get(item_id, 0) + quantity
        
        if not item_counts:
            await message.answer("📦 Инвентарь пуст")
            return
        
        for item_id, count in item_counts.items():
            item_info = GIFT_CATALOG.get(item_id)
            if item_info:
                text += f"{item_info['name']} x{count}\n"
            else:
                text += f"❓ {item_id} x{count}\n"
        
        text += f"\n💡 Используйте передать @user предмет чтобы подарить"
        
        await message.answer(text)
        logging.info("Инвентарь показан успешно")
        
    except Exception as e:
        logging.error(f"Ошибка cmd_inventory: {e}")
        import traceback
        logging.error(traceback.format_exc())
        await message.answer("❌ Ошибка просмотра инвентаря")

@dp.message(F.text == "бустеры")
async def cmd_boosters(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for booster_id, booster in BOOSTERS.items():
            keyboard.button(
                text=f"{booster['name']} - {booster['price']:,} 🍇", 
                callback_data=f"buy_booster_{booster_id}"
            )
        keyboard.adjust(2)
        
        text = (
            f"🚀 **Магазин бустеров**\n\n"
            f"💰 Ваш баланс: {balance:,} 🍇\n\n"
            f"⚡ **Категории бустеров:**\n\n"
            f"🕐 **Ускорение роста:**\n"
            f"• Увеличивает скорость роста культур в 1.5 раза\n\n"
            f"📈 **Увеличение урожая:**\n"
            f"• Добавляет +30% к награде за сбор\n\n"
            f"💰 **Увеличение дохода:**\n"
            f"• Удваивает пассивный доход от дома\n\n"
            f"🌟 **Супер бустеры:**\n"
            f"• Действуют на все улучшения сразу!\n\n"
            f"💡 **Совет:** Активируйте бустеры перед посадкой!"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_boosters: {e}")
        await message.answer("❌ Ошибка при загрузке магазина бустеров.")

@dp.message(F.text == "баланс")
async def cmd_balance(message: Message):
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала старт")
            return
        
        balance = user.get('balance', 0)
        farm_level = user.get('farm_level', 1)
        house_level = user.get('house_level', 1)
        total_harvest = user.get('total_harvest', 0)
        total_earned = user.get('total_earned', 0)
        
        text = (
            f"💰 **Ваш баланс**\n\n"
            f"🍇 **Виноград:** {balance:,}\n\n"
            f"📊 **Ваш прогресс:**\n"
            f"🌾 Уровень фермы: {farm_level} из 9\n"
            f"🏠 Уровень дома: {house_level} из 9\n\n"
            f"📈 **Статистика:**\n"
            f"🚜 Всего собрано урожаев: {total_harvest:,}\n"
            f"💵 Всего заработано: {total_earned:,} 🍇\n\n"
            f"💡 **Что дальше?**\n"
            f"• сбор - соберите виноград\n"
            f"• ферма - посадите культуры\n"
            f"• магазин - купите улучшения"
        )
        
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_balance: {e}")
        await message.answer("❌ Ошибка при загрузке баланса.")

@dp.message(F.text == "магазин")
async def cmd_shop(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in SHOP_ITEMS.items():
            keyboard.button(
                text=f"{item['name']} - {item['price']:,} 🍇", 
                callback_data=f"buy_{item_id}"
            )
        keyboard.adjust(2)
        
        text = (
            f"🏪 **Магазин улучшений**\n\n"
            f"💰 Ваш баланс: {balance:,} 🍇\n\n"
            f"🔧 **Улучшения:**\n"
            f"🔄 Авто-сбор - собирайте без ожидания\n"
            f"📈 Умножение x2/x3 - увеличьте награду\n"
            f"⏰ Бонус - чаще получайте бонусы\n\n"
            f"🎨 **Скины:**\n"
            f"🍷 Винный, 💎 Алмазный, 🏆 Золотой\n"
            f"Измените внешний вид винограда!\n\n"
            f"💚 **Расходники:**\n"
            f"💚 Восстановление - сброс кулдауна\n"
            f"🍀 Талисман удачи - +5% к крит. сбору\n\n"
            f"💡 Нажмите на товар для покупки!"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_shop: {e}")
        await message.answer("❌ Ошибка при загрузке магазина.")

@dp.message(F.text == "помощь")
async def cmd_help(message: Message):
    text = (
        f"📚 **Справка по боту**\n\n"
        
        f"🌾 **ФЕРМА:**\n"
        f"ферма - ваша ферма и грядки\n"
        f"посадить [номер] [культура] - посадить\n"
        f"собрать [номер] - собрать урожай\n"
        f"сбор - собрать виноград\n\n"
        
        f"🏠 **ДОМ:**\n"
        f"дом - ваш дом и доход\n"
        f"бустеры - магазин ускорений\n\n"
        
        f"🎁 **ПОДАРКИ:**\n"
        f"подарки - магазин подарков\n"
        f"инвентарь - ваши подарки\n"
        f"инвентарь @user - чужой инвентарь\n"
        f"передать @user предмет - подарить\n\n"
        
        f"💰 **БАЛАНС И МАГАЗИН:**\n"
        f"баланс - ваш баланс и прогресс\n"
        f"магазин - улучшения и скины\n\n"
        
        f"👥 **СООБЩЕСТВО:**\n"
        f"топ - рейтинг игроков\n"
        f"статистика - статистика бота\n\n"
        
        f"🔧 **АДМИНИСТРИРОВАНИЕ:**\n"
        f"сброс - сбросить прогресс (только админ)\n\n"
        
        f"💡 **Советы:**\n"
        f"• Начинайте с сбор\n"
        f"• Сажайте культуры на ферма\n"
        f"• Покупайте улучшения в магазин\n"
        f"• Стройте дом в дом\n"
        f"• Общайтесь с другими игроками!\n\n"
        
        f"🎮 **Удачи на ферме!** 🍇"
    )
    await message.answer(text)

@dp.message(F.text == "топ")
async def cmd_top(message: Message):
    try:
        top = await get_top_users(10)
        if not top:
            await message.answer("📊 Пока нет игроков. Будьте первыми!")
            return
        
        text = "🏆 **Топ игроков** 🏆\n\n"
        text += "🥇 **Лучшие фермеры:**\n\n"
        
        for i, row in enumerate(top, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            try:
                u = await bot.get_chat(row['user_id'])
                name = u.first_name[:20]
            except:
                name = f"Игрок {row['user_id']}"
            text += f"{medal} {name} - {row['balance']:,} 🍇\n"
        
        text += "\n💡 Попробуйте попасть в топ!"
        
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_top: {e}")
        await message.answer("❌ Ошибка при загрузке рейтинга.")

@dp.message(F.text == "статистика")
async def cmd_stats(message: Message):
    try:
        total = await get_total_users()
        grapes = await get_total_grapes()
        
        text = (
            "📊 **Статистика бота**\n\n"
            f"👥 **Всего игроков:** {total:,}\n"
            f"🍇 **Винограда собрано:** {grapes:,}\n\n"
            f"🌟 **Присоединяйтесь!**\n"
            f"Станьте частью нашего сообщества!"
        )
        
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_stats: {e}")
        await message.answer("❌ Ошибка при загрузке статистики.")

@dp.callback_query(lambda c: c.data.startswith("gift_"))
async def callback_gift_buy(callback: CallbackQuery):
    try:
        await callback.answer()
        
        user_id = callback.from_user.id
        item_id = callback.data.replace("gift_", "")
        
        logging.info(f"=== ПОКУПКА ПОДАРКА ===")
        logging.info(f"Пользователь: {user_id}")
        logging.info(f"Предмет: {item_id}")
        
        item = GIFT_CATALOG.get(item_id)
        if not item:
            logging.error(f"Предмет {item_id} не найден в каталоге")
            await callback.message.answer("❌ Подарок не найден")
            return
        
        user = await get_user(user_id)
        if not user:
            logging.error(f"Пользователь {user_id} не найден")
            await callback.message.answer("❌ Ошибка пользователя")
            return
        
        balance = user.get('balance', 0)
        price = item['price']
        
        logging.info(f"Баланс: {balance}, Цена: {price}")
        
        if balance < price:
            logging.warning(f"Недостаточно средств: {balance} < {price}")
            await callback.message.answer(f"❌ Недостаточно винограда! Нужно {price}, у вас {balance}")
            return
        
        new_balance = balance - price
        await update_balance(user_id, -price)
        logging.info(f"Баланс списан: {balance} -> {new_balance}")
        
        success = await add_to_inventory(user_id, item_id)
        logging.info(f"Результат добавления в инвентарь: {success}")
        
        if success:
            updated_user = await get_user(user_id)
            if updated_user:
                inv = updated_user.get('inventory', [])
                logging.info(f"Инвентарь после покупки: {inv}")
            
            await callback.message.answer(
                f"✅ {item['name']} куплен!\n\n"
                f"Списано: {price} 🍇\n"
                f"Остаток: {new_balance} 🍇\n"
                f"инвентарь - посмотреть подарки"
            )
            logging.info("Покупка успешна!")
        else:
            await update_balance(user_id, price)
            logging.error(f"Ошибка добавления в инвентарь. Деньги возвращены.")
            await callback.message.answer(
                "❌ Ошибка добавления в инвентарь!\n"
                "Деньги возвращены на баланс."
            )
        
        logging.info("=== КОНЕЦ ПОКУПКИ ===")
        
    except Exception as e:
        logging.error(f"Критическая ошибка callback_gift_buy: {e}")
        import traceback
        logging.error(traceback.format_exc())
        await callback.answer("❌ Ошибка покупки", show_alert=True)

@dp.callback_query(lambda c: c.data == "farm_plant")
async def callback_farm_plant(callback: CallbackQuery):
    try:
        await callback.answer()
        
        keyboard = InlineKeyboardBuilder()
        for crop_id, crop in CROPS.items():
            keyboard.button(
                text=f"{crop['name']} - {crop['cost']} 🍇", 
                callback_data=f"crop_{crop_id}"
            )
        keyboard.adjust(2)
        
        await callback.message.answer(
            "🌱 **Выберите культуру:**", 
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"Ошибка callback_farm_plant: {e}")

@dp.callback_query(lambda c: c.data.startswith("crop_"))
async def callback_select_crop(callback: CallbackQuery):
    try:
        await callback.answer()
        
        crop_id = callback.data.replace("crop_", "")
        crop = CROPS.get(crop_id)
        
        if not crop:
            await callback.message.answer("❌ Культура не найдена!")
            return
        
        user = await get_user(callback.from_user.id)
        if not user:
            await callback.message.answer("❌ Ошибка пользователя")
            return
        
        plots = user.get('farm_plots', [])
        
        if user['balance'] < crop['cost']:
            await callback.message.answer(
                f"❌ Недостаточно винограда!\n"
                f"Нужно: {crop['cost']} 🍇\n"
                f"У вас: {user['balance']} 🍇"
            )
            return
        
        keyboard = InlineKeyboardBuilder()
        available = []
        
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                available.append(i)
                keyboard.button(
                    text=f"🌾 Грядка {i + 1}",
                    callback_data=f"plant_{crop_id}_{i}"
                )
        
        if not available:
            await callback.message.answer(
                "❌ Все грядки заняты!\nСначала соберите урожай."
            )
            return
        
        keyboard.adjust(3)
        
        await callback.message.answer(
            f"🌱 **{crop['name']}**\n\n"
            f"💰 Цена: {crop['cost']} 🍇\n"
            f"⏱ Время: {crop['growth_time'] // 3600} ч\n"
            f"💵 Награда: {crop['reward']} 🍇\n\n"
            f"**Выберите грядку:**",
            reply_markup=keyboard.as_markup()
        )
    except Exception as e:
        logging.error(f"Ошибка callback_select_crop: {e}")

@dp.callback_query(lambda c: c.data.startswith("plant_"))
async def callback_plant_to_plot(callback: CallbackQuery):
    try:
        await callback.answer()
        
        parts = callback.data.replace("plant_", "").split("_")
        crop_id = parts[0]
        plot_index = int(parts[1])
        
        crop = CROPS.get(crop_id)
        if not crop:
            await callback.message.answer("❌ Культура не найдена!")
            return
        
        user_id = callback.from_user.id
        success, msg = await plant_crop(user_id, plot_index, crop_id)
        
        if success:
            await callback.message.answer(
                f"✅ **Посажено!**\n\n"
                f"🌱 {crop['name']}\n"
                f"📍 Грядка: {plot_index + 1}\n"
                f"⏱ Созреет через: {crop['growth_time'] // 3600} ч\n\n"
                f"ферма — посмотреть"
            )
        else:
            await callback.message.answer(f"❌ {msg}")
    except Exception as e:
        logging.error(f"Ошибка callback_plant_to_plot: {e}")

@dp.callback_query(lambda c: c.data == "farm_upgrade")
async def callback_farm_upgrade(callback: CallbackQuery):
    try:
        await callback.answer()
        success, msg = await upgrade_farm_level(callback.from_user.id)
        await callback.message.answer(msg)
    except Exception as e:
        logging.error(f"Ошибка callback_farm_upgrade: {e}")

@dp.callback_query(lambda c: c.data == "farm_stats")
async def callback_farm_stats(callback: CallbackQuery):
    try:
        await callback.answer()
        user = await get_user(callback.from_user.id)
        if user:
            text = "📊 Ферма\n\n"
            text += f"Уровень: {user.get('farm_level', 1)}\n"
            text += f"Опыт: {user.get('farm_xp', 0)}\n"
            text += f"Грядок: {len(user.get('farm_plots', []))}"
            await callback.message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка callback_farm_stats: {e}")

@dp.callback_query(lambda c: c.data.startswith("house_"))
async def callback_house(callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        action = callback.data.replace("house_", "")
        
        if action == "claim":
            success, msg = await claim_passive_income(user_id)
            await callback.message.answer(f"{'✅' if success else '⏳'} {msg}")
        elif action.startswith("upgrade_"):
            level = int(action.replace("upgrade_", ""))
            house_id = None
            for h_id, h in HOUSES.items():
                if h['level'] == level:
                    house_id = h_id
                    break
            
            if house_id:
                success, msg = await upgrade_house_level(user_id, house_id)
                await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
        elif action == "stats":
            user = await get_user(user_id)
            text = "📊 Статистика\n\n"
            text += f"Дом: ур. {user.get('house_level', 1)}\n"
            text += f"Опыт: {user.get('house_xp', 0)}\n"
            text += f"Ферма: ур. {user.get('farm_level', 1)}\n"
            text += f"Урожаев: {user.get('total_harvest', 0)}"
            await callback.message.answer(text)
        
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка callback_house: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_booster_"))
async def callback_buy_booster(callback: CallbackQuery):
    try:
        await callback.answer()
        
        booster_id = callback.data.replace("buy_booster_", "")
        booster = BOOSTERS.get(booster_id)
        
        if not booster:
            await callback.message.answer("❌ Не найдено")
            return
        
        user = await get_user(callback.from_user.id)
        
        if user['balance'] < booster['price']:
            await callback.message.answer("❌ Недостаточно")
            return
        
        await update_balance(callback.from_user.id, -booster['price'])
        await callback.message.answer(f"✅ {booster['name']} активирован!")
    except Exception as e:
        logging.error(f"Ошибка callback_buy_booster: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback: CallbackQuery):
    try:
        await callback.answer()
        
        item_id = callback.data.replace("buy_", "")
        item = SHOP_ITEMS.get(item_id)
        
        if not item:
            await callback.message.answer("❌ Не найдено")
            return
        
        user = await get_user(callback.from_user.id)
        
        if user['balance'] < item['price']:
            await callback.message.answer("❌ Недостаточно")
            return
        
        await update_balance(callback.from_user.id, -item['price'])
        await callback.message.answer(f"✅ {item['name']} куплен!")
    except Exception as e:
        logging.error(f"Ошибка callback_buy: {e}")

async def main():
    try:
        await init_db()
        logging.info("Бот запущен!")
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
