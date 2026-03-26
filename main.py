import asyncio
import logging
import time
import os
import asyncpg
import random
import string
import json
from aiogram import Bot, Dispatcher
from aiogram.filters import Command, CommandStart
from aiogram.types import Message, CallbackQuery
from aiogram.utils.keyboard import InlineKeyboardBuilder

BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

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

async def get_user(user_id):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                result = dict(row)
                try:
                    result['farm_plots'] = json.loads(row['farm_plots']) if row['farm_plots'] else []
                except:
                    result['farm_plots'] = ["empty", "empty", "empty"]
                try:
                    result['tools'] = json.loads(row['tools']) if row['tools'] else []
                except:
                    result['tools'] = []
                try:
                    result['boosters'] = json.loads(row['boosters']) if row['boosters'] else []
                except:
                    result['boosters'] = []
                try:
                    result['inventory'] = json.loads(row['inventory']) if row['inventory'] else []
                except:
                    result['inventory'] = []
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
    
    active = []
    for booster in boosters:
        if isinstance(booster, dict) and booster.get('expires_at', 0) > now:
            active.append(booster)
    
    return active

async def add_booster(user_id, booster_id):
    async with pool.acquire() as conn:
        user = await get_user(user_id)
        boosters = user.get('boosters', [])
        now = int(time.time())
        
        booster = BOOSTERS.get(booster_id)
        if not booster:
            return False, "Бустер не найден"
        
        new_booster = {
            "item_id": booster_id,
            "name": booster['name'],
            "effect": booster['effect'],
            "bonus": booster['bonus'],
            "expires_at": now + booster['duration']
        }
        
        boosters.append(new_booster)
        
        await conn.execute("UPDATE users SET boosters = $1 WHERE user_id = $2", json.dumps(boosters), user_id)
        return True, f"{booster['name']} активирован!"

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
            return False, f"Недостаточно винограда! Нужно {crop['cost']} 🍇"
        
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
                crop['cost'], 
                json.dumps(plots), 
                user_id
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
            await conn.execute("UPDATE users SET balance = balance + $1, total_harvest = total_harvest + 1, total_earned = total_earned + $1 WHERE user_id = $2", reward, user_id)
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
        
        if not upgrade_info:
            return False, "Ошибка улучшения"
        
        if user['balance'] < upgrade_info['upgrade_cost']:
            return False, f"Нужно {upgrade_info['upgrade_cost']}"
        
        new_plots = ["empty"] * upgrade_info['plots']
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance - $1, farm_level = $2, farm_plots = $3 WHERE user_id = $4",
                              upgrade_info['upgrade_cost'], next_level, json.dumps(new_plots), user_id)
        
        return True, f"Ферма улучшена до уровня {next_level}! Теперь {upgrade_info['plots']} грядок."
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
                logging.error(f"Пользователь {user_id} не найден в БД")
                return False
            
            try:
                inventory = json.loads(row['inventory']) if row['inventory'] else []
            except:
                inventory = []
            
            logging.info(f"Текущий инвентарь: {inventory}")
            
            found = False
            for item in inventory:
                if isinstance(item, dict) and item.get('item_id') == item_id:
                    item['quantity'] = item.get('quantity', 1) + 1
                    found = True
                    logging.info(f"Обновлено количество {item_id}: {item['quantity']}")
                    break
            
            if not found:
                inventory.append({"item_id": item_id, "quantity": 1})
                logging.info(f"Добавлен новый предмет: {item_id}")
            
            await conn.execute(
                "UPDATE users SET inventory = $1 WHERE user_id = $2", 
                json.dumps(inventory), 
                user_id
            )
            
            logging.info(f"Инвентарь сохранен: {inventory}")
            return True
            
    except Exception as e:
        logging.error(f"Ошибка add_to_inventory: {e}")
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

@dp.message(CommandStart())
async def cmd_start(message: Message):
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code, username)
        
        if user:
            text = "🍇 ДОБРО ПОЖАЛОВАТЬ! 🍇\n\n"
            text += f"Привет, {message.from_user.first_name}!\n\n"
            text += "Это бот-ферма! Выращивай виноград, строй дома и становись богатым!\n\n"
            text += "📋 Команды:\n"
            text += "/ферма - управлять фермой\n"
            text += "/дом - управлять домом\n"
            text += "/подарки - магазин подарков\n"
            text += "/бустеры - бустеры\n"
            text += "/баланс - баланс\n"
            text += "/помощь - справка"
            await message.answer(text)
        else:
            await message.answer("❌ Ошибка регистрации")
    except Exception as e:
        logging.error(f"Ошибка cmd_start: {e}")
        await message.answer("❌ Произошла ошибка.")

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
                f"/ферма — посмотреть"
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
            text = "📊 **Ферма**\n\n"
            text += f"Уровень: {user.get('farm_level', 1)}\n"
            text += f"Опыт: {user.get('farm_xp', 0)}\n"
            text += f"Грядок: {len(user.get('farm_plots', []))}"
            await callback.message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка callback_farm_stats: {e}")

@dp.message(Command("ферма"))
async def cmd_farm(message: Message):
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        
        if not user:
            await message.answer("❌ Сначала запустите бота /start")
            return
        
        farm_level = user.get('farm_level', 1)
        farm_xp = user.get('farm_xp', 0)
        plots = user.get('farm_plots', ["empty", "empty", "empty"])
        balance = user.get('balance', 0)
        
        plot_emojis = {
            "empty": "🟫",
            "grape": "🍇",
            "strawberry": "🍓",
            "corn": "🌽",
            "tomato": "🍅",
            "pumpkin": "🎃",
            "melon": "🍈",
            "pineapple": "🍍",
            "coconut": "🥥",
            "diamond_grape": "💎",
            "golden_apple": "🍎"
        }
        
        farm_display = []
        now = int(time.time())
        speed_bonus = await get_booster_effect(user_id, 'growth_speed')
        
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                farm_display.append("🟫")
            else:
                crop = CROPS.get(plot.get('crop'))
                if crop:
                    planted = plot.get('planted_at', 0)
                    growth_time = int(crop['growth_time'] / speed_bonus)
                    ready_time = planted + growth_time
                    
                    if now >= ready_time:
                        farm_display.append(f"✅{plot_emojis.get(crop.get('crop'), '🌱')}")
                    else:
                        remaining = ready_time - now
                        hours = remaining // 3600
                        minutes = (remaining % 3600) // 60
                        farm_display.append(f"⏳{plot_emojis.get(crop.get('crop'), '🌱')}")
                else:
                    farm_display.append("🟫")
        
        farm_grid = ""
        for i in range(0, len(farm_display), 3):
            row = farm_display[i:i+3]
            farm_grid += " ".join(row) + "\n"
        
        xp_needed = farm_level * 500
        xp_progress = farm_xp % xp_needed
        xp_percent = int((xp_progress / xp_needed) * 10) if xp_needed > 0 else 0
        xp_bar = "▰" * xp_percent + "▱" * (10 - xp_percent)
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🌱 Посадить", callback_data="farm_plant")
        keyboard.button(text="🚜 Улучшить ферму", callback_data="farm_upgrade")
        keyboard.button(text="📊 Статистика", callback_data="farm_stats")
        keyboard.adjust(2)
        
        text = "🌾 **ВАША ФЕРМА** 🌾\n\n"
        text += f"👤 Уровень: {farm_level}\n"
        text += f"📊 Опыт: {xp_progress}/{xp_needed}\n"
        text += f"{xp_bar}\n\n"
        text += f"💰 Баланс: {balance} 🍇\n\n"
        text += "📍 **Грядки:**\n"
        text += farm_grid
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_farm: {e}")
        await message.answer("❌ Ошибка фермы.")

@dp.message(Command("дом"))
async def cmd_house(message: Message):
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        
        if not user:
            await message.answer("❌ Сначала запустите бота /start")
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
            keyboard.button(text=f"🔨 Улучшить ({next_house['price']} 🍇)", callback_data=f"house_upgrade_{next_house['level']}")
        keyboard.button(text="📊 Статистика", callback_data="house_stats")
        keyboard.adjust(2)
        
        text = f"🏠 ВАШ ДОМ 🏠\n\n"
        text += f"{house['name']}\n"
        text += "─" * 20 + "\n\n"
        text += f"📊 Уровень: {house_level}\n"
        text += f"✨ Опыт: {house_xp}\n"
        text += f"💰 Доход: {house['passive_income']} 🍇/час\n\n"
        text += f"💵 Баланс: {balance} 🍇\n\n"
        text += "📈 Пассивный доход:\n"
        income_bar = "▰" * min(hours_passed, 10) + "▱" * max(0, 10 - hours_passed)
        text += f"{income_bar}\n"
        text += f"🎁 Доступно: {pending_income} 🍇\n"
        
        if next_house:
            text += f"\n🔜 Следующий дом:\n{next_house['name']}\n"
            text += f"💰 Цена: {next_house['price']} 🍇\n"
            text += f"📈 Доход: {next_house['passive_income']} 🍇/час"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_house: {e}")
        await message.answer("❌ Ошибка дома.")

@dp.message(Command("подарки"))
async def cmd_gifts(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in GIFT_CATALOG.items():
            keyboard.button(text=f"{item['name']} - {item['price']} 🍇", callback_data=f"gift_{item_id}")
        keyboard.adjust(2)
        
        text = "🎁 МАГАЗИН ПОДАРКОВ 🎁\n\n"
        text += f"💰 Ваш баланс: {balance} 🍇\n\n"
        text += "Выберите подарок:"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_gifts: {e}")

@dp.callback_query(lambda c: c.data.startswith("gift_"))
async def callback_gift_buy(callback: CallbackQuery):
    try:
        await callback.answer()
        
        user_id = callback.from_user.id
        item_id = callback.data.replace("gift_", "")
        
        item = GIFT_CATALOG.get(item_id)
        if not item:
            await callback.message.answer("❌ Подарок не найден")
            return
        
        user = await get_user(user_id)
        if not user:
            await callback.message.answer("❌ Ошибка пользователя")
            return
            
        if user['balance'] < item['price']:
            await callback.message.answer("❌ Недостаточно винограда 🍇")
            return
        
        await update_balance(user_id, -item['price'])
        
        success = await add_to_inventory(user_id, item_id)
        
        if success:
            await callback.message.answer(
                f"✅ {item['name']} куплен!\n\n"
                f"Списано: {item['price']} 🍇\n"
                f"/инвентарь - посмотреть подарки"
            )
        else:
            await update_balance(user_id, item['price'])
            await callback.message.answer("❌ Ошибка добавления в инвентарь! Деньги возвращены.")
        
    except Exception as e:
        logging.error(f"Ошибка callback_gift_buy: {e}")
        await callback.answer("❌ Ошибка покупки", show_alert=True)

@dp.message(Command("инвентарь"))
async def cmd_inventory(message: Message):
    try:
        args = message.text.split()
        user_id = message.from_user.id
        
        # Если указан @username - смотрим чужой инвентарь
        if len(args) > 1:
            target_username = args[1].replace('@', '')
            
            # Ищем пользователя
            target_user = await get_user_by_username(target_username)
            
            if not target_user:
                await message.answer(f"❌ Пользователь @{target_username} не найден!")
                return
            
            # Показываем публичный инвентарь (без точных количеств для редких предметов)
            inventory = target_user.get('inventory', [])
            
            if not inventory or len(inventory) == 0:
                await message.answer(f"📦 Инвентарь @{target_username} пуст")
                return
            
            text = f"📦 **Инвентарь @{target_username}** 📦\n\n"
            
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
                item = GIFT_CATALOG.get(item_id)
                if item:
                    # Показываем только наличие для редких предметов (баланс)
                    if item.get('rarity') in ['epic', 'legendary']:
                        text += f"{item['name']} ✅\n"
                    else:
                        text += f"{item['name']} x{min(count, 99)}\n"  # Ограничиваем показ количества
                else:
                    text += f"❓ Неизвестный предмет ✅\n"
            
            await message.answer(text)
            return
        
        # Если без @username - показываем свой инвентарь (полная информация)
        logging.info(f"Пользователь {user_id} запросил свой инвентарь")
        
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT inventory FROM users WHERE user_id = $1", user_id)
            
            if not row:
                await message.answer("❌ Ошибка пользователя")
                return
            
            try:
                inventory = json.loads(row['inventory']) if row['inventory'] else []
            except:
                inventory = []
            
            logging.info(f"Инвентарь из БД: {inventory}")
            
            if not inventory or len(inventory) == 0:
                await message.answer("📦 Ваш инвентарь пуст\n\n/подарки - купить подарки")
                return
            
            text = "📦 **ВАШ ИНВЕНТАРЬ** 📦\n\n"
            
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
                item = GIFT_CATALOG.get(item_id)
                if item:
                    text += f"{item['name']} x{count}\n"
                else:
                    text += f"❓ {item_id} x{count}\n"
            
            await message.answer(text)
            
    except Exception as e:
        logging.error(f"Ошибка cmd_inventory: {e}")
        await message.answer("❌ Ошибка просмотра инвентаря")

@dp.message(Command("бустеры"))
async def cmd_boosters(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for booster_id, booster in BOOSTERS.items():
            keyboard.button(text=f"{booster['name']} - {booster['price']} 🍇", callback_data=f"buy_booster_{booster_id}")
        keyboard.adjust(2)
        
        text = "🚀 БУСТЕРЫ 🚀\n\n"
        text += f"💰 Баланс: {balance} 🍇\n\n"
        text += "⚡ Скорость - x1.5 к скорости роста\n"
        text += "📈 Урожай - +30% к урожаю\n"
        text += "💰 Доход - x2 к пассивному доходу\n"
        text += "🌟 Особые - x1.5 ко всему\n\n"
        text += "Выберите бустер:"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_boosters: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_booster_"))
async def callback_buy_booster(callback: CallbackQuery):
    try:
        booster_id = callback.data.replace("buy_booster_", "")
        user_id = callback.from_user.id
        
        booster = BOOSTERS.get(booster_id)
        if not booster:
            await callback.answer("❌ Не найден", show_alert=True)
            return
        
        user = await get_user(user_id)
        if user['balance'] < booster['price']:
            await callback.answer("❌ Недостаточно", show_alert=True)
            return
        
        await update_balance(user_id, -booster['price'])
        success, msg = await add_booster(user_id, booster_id)
        
        await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
        await callback.answer()
    except Exception as e:
        logging.error(f"Ошибка callback_buy_booster: {e}")

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

@dp.message(Command("посадить"))
async def cmd_plant(message: Message):
    try:
        args = message.text.split()
        logging.info(f"Команда /посадить: {args}")
        
        if len(args) < 3:
            await message.answer(
                "🌱 Использование:\n"
                "/посадить [номер грядки] [культура]\n\n"
                "Примеры:\n"
                "/посадить 1 grape\n"
                "/посадить 2 strawberry\n\n"
                f"Доступные культуры: {', '.join(CROPS.keys())}"
            )
            return
        
        try:
            plot_index = int(args[1]) - 1
        except ValueError:
            await message.answer("❌ Номер грядки должен быть числом!")
            return
        
        crop_id = args[2]
        
        logging.info(f"Посадка: грядка {plot_index + 1}, культура {crop_id}")
        
        if crop_id not in CROPS:
            await message.answer(
                f"❌ Культура не найдена!\n\n"
                f"Доступные культуры:\n" + 
                "\n".join([f"{cid} - {c['name']}" for cid, c in CROPS.items()])
            )
            return
        
        success, msg = await plant_crop(message.from_user.id, plot_index, crop_id)
        
        logging.info(f"Результат посадки: {success}, {msg}")
        await message.answer(msg)
        
    except Exception as e:
        logging.error(f"Ошибка cmd_plant: {e}")
        await message.answer(f"❌ Ошибка: {str(e)}")

@dp.message(Command("собрать"))
async def cmd_harvest(message: Message):
    try:
        args = message.text.split()
        if len(args) < 2:
            await message.answer("🚜 /собрать [грядка]\nПример: /собрать 1")
            return
        
        plot_index = int(args[1]) - 1
        success, msg = await harvest_crop(message.from_user.id, plot_index)
        await message.answer(f"{'✅' if success else '⏳'} {msg}")
    except Exception as e:
        logging.error(f"Ошибка cmd_harvest: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала /start")
            return
        
        balance = user.get('balance', 0)
        farm_level = user.get('farm_level', 1)
        house_level = user.get('house_level', 1)
        total_harvest = user.get('total_harvest', 0)
        total_earned = user.get('total_earned', 0)
        
        text = "💰 ВАШ БАЛАНС 💰\n"
        text += "═" * 25 + "\n\n"
        text += f"🍇 Виноград: {balance:,}\n\n"
        text += "📊 Статистика:\n"
        text += f"🌾 Ферма: уровень {farm_level}\n"
        text += f"🏠 Дом: уровень {house_level}\n"
        text += f"🚜 Всего урожаев: {total_harvest}\n"
        text += f"💵 Всего заработано: {total_earned:,} 🍇\n\n"
        text += "🎯 Прогресс:\n"
        text += f"Ферма: {farm_level}/9 🌾\n"
        text += f"Дом: {house_level}/9 🏠"
        
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_balance: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("магазин"))
async def cmd_shop(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in SHOP_ITEMS.items():
            keyboard.button(text=f"{item['name']} - {item['price']} 🍇", callback_data=f"buy_{item_id}")
        keyboard.adjust(2)
        
        text = "🏪 МАГАЗИН 🏪\n\n"
        text += f"Баланс: {balance} 🍇\n\n"
        text += "🔧 Улучшения:\n"
        text += "🔄 Авто-сбор - 1000 🍇\n"
        text += "📈 x2 - 2500 🍇\n"
        text += "📈 x3 - 7500 🍇\n"
        text += "⏰ Бонус 2ч - 500 🍇\n"
        text += "⏰ Бонус 1ч - 800 🍇\n\n"
        text += "🎨 Скины:\n"
        text += "🍷 Вино - 500 🍇\n"
        text += "💎 Алмаз - 1500 🍇\n"
        text += "🏆 Золото - 3000 🍇\n\n"
        text += "💚 Сброс - 200 🍇\n"
        text += "🍀 Талисман - 750 🍇"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_shop: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback: CallbackQuery):
    try:
        await callback.answer()
        
        user_id = callback.from_user.id
        item_id = callback.data.replace("buy_", "")
        
        if item_id not in SHOP_ITEMS:
            await callback.message.answer("❌ Товар не найден")
            return
        
        item = SHOP_ITEMS[item_id]
        user = await get_user(user_id)
        
        if not user:
            await callback.message.answer("❌ Ошибка пользователя")
            return
            
        if user['balance'] < item['price']:
            await callback.message.answer("❌ Недостаточно винограда 🍇")
            return
        
        if item['type'] == "upgrade":
            if item_id == "auto_collect" and user['auto_collect']:
                await callback.message.answer("❌ Уже куплено")
                return
            if item_id == "double_grapes" and user['double_grapes']:
                await callback.message.answer("❌ Уже куплено")
                return
            if item_id == "bonus_2h" and user['bonus_2h']:
                await callback.message.answer("❌ Уже куплено")
                return
        
        await update_balance(user_id, -item['price'])
        await buy_item(user_id, item_id)
        await callback.message.answer(f"✅ {item['name']} куплен!")
    except Exception as e:
        logging.error(f"Ошибка callback_buy: {e}")

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    text = "📚 **СПРАВКА** 📚\n\n"
    text += "🌾 **Ферма**:\n"
    text += "/ферма - ферма\n"
    text += "/посадить [грядка] [культура]\n"
    text += "/собрать [грядка]\n\n"
    text += "🏠 **Дом**:\n"
    text += "/дом - дом\n"
    text += "/бустеры - бустеры\n\n"
    text += "🎁 **Подарки**:\n"
    text += "/подарки - магазин подарков\n"
    text += "/инвентарь - мои подарки\n"
    text += "/передать @user предмет\n\n"
    text += "🍇 **Сбор**:\n"
    text += "/баланс - проверить\n\n"
    text += "🏪 **Магазин**:\n"
    text += "/магазин - улучшения\n\n"
    text += "👥 **Другое**:\n"
    text += "/топ - рейтинг\n"
    text += "/помощь - справка"
    await message.answer(text)

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    try:
        top = await get_top_users(10)
        if not top:
            await message.answer("📊 Пока нет игроков")
            return
        
        text = "🏆 ТОП ИГРОКОВ 🏆\n\n"
        for i, row in enumerate(top, 1):
            medal = "🥇" if i == 1 else "🥈" if i == 2 else "🥉" if i == 3 else f"{i}."
            try:
                u = await bot.get_chat(row['user_id'])
                name = u.first_name[:20]
            except:
                name = f"User{row['user_id']}"
            text += f"{medal} {name} - {row['balance']} 🍇\n"
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_top: {e}")

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    try:
        total = await get_total_users()
        grapes = await get_total_grapes()
        text = "📊 СТАТИСТИКА 📊\n\n"
        text += f"👥 Игроков: {total}\n"
        text += f"🍇 Всего: {grapes or 0}"
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_stats: {e}")

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
