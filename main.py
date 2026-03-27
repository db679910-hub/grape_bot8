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

# =============================================================================
# НАСТРОЙКИ БОТА
# =============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# =============================================================================
# ЭКОНОМИКА ИГРЫ
# =============================================================================
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

# =============================================================================
# КУЛЬТУРЫ (ИСПРАВЛЕНО: убраны все пробелы в ключах)
# =============================================================================
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

# =============================================================================
# УЛУЧШЕНИЯ ФЕРМЫ (ВЫСОКИЕ ЦЕНЫ, МАКСИМУМ 15 ГРЯДОК)
# =============================================================================
FARM_UPGRADES = {
    1: {"level": 1, "plots": 3, "upgrade_cost": 0},
    2: {"level": 2, "plots": 5, "upgrade_cost": 5000},
    3: {"level": 3, "plots": 7, "upgrade_cost": 15000},
    4: {"level": 4, "plots": 9, "upgrade_cost": 35000},
    5: {"level": 5, "plots": 11, "upgrade_cost": 75000},
    6: {"level": 6, "plots": 13, "upgrade_cost": 150000},
    7: {"level": 7, "plots": 15, "upgrade_cost": 300000},
}

# =============================================================================
# ИНСТРУМЕНТЫ (ИСПРАВЛЕНО: убраны пробелы)
# =============================================================================
TOOLS = {
    "hoe": {"name": "🔓 Мотыга", "price": 200, "effect": "growth_speed", "bonus": 1.1},
    "watering_can": {"name": "🚿 Лейка", "price": 500, "effect": "growth_speed", "bonus": 1.2},
    "fertilizer": {"name": "💩 Удобрение", "price": 1000, "effect": "growth_speed", "bonus": 1.3},
    "tractor": {"name": "🚜 Трактор", "price": 3000, "effect": "growth_speed", "bonus": 1.5},
    "harvester": {"name": "🌾 Комбайн", "price": 7500, "effect": "growth_speed", "bonus": 1.7},
    "drone": {"name": "🚁 Дрон", "price": 15000, "effect": "growth_speed", "bonus": 2.0},
    "ai_system": {"name": "🤖 ИИ Система", "price": 35000, "effect": "growth_speed", "bonus": 2.5},
}

# =============================================================================
# КАТАЛОГ ПОДАРКОВ (ИСПРАВЛЕНО: убраны пробелы)
# =============================================================================
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

# =============================================================================
# БУСТЕРЫ (ИСПРАВЛЕНО: убраны пробелы)
# =============================================================================
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

# =============================================================================
# ДОМА (ИСПРАВЛЕНО: убраны пробелы)
# =============================================================================
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

# =============================================================================
# НАСТРОЙКИ ЛОГИРОВАНИЯ
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
pool = None

# =============================================================================
# МАГАЗИН ПРЕДМЕТОВ (ИСПРАВЛЕНО: убраны пробелы)
# =============================================================================
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

# =============================================================================
# ВСПОМОГАТЕЛЬНЫЕ ФУНКЦИИ
# =============================================================================
def generate_ref_code():
    """Генерация реферального кода"""
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
# =============================================================================
async def init_db():
    """Создание таблиц в базе данных"""
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
                    farm_plots TEXT DEFAULT '["empty","empty","empty"]',
                    tools TEXT DEFAULT '[]',
                    house_level INTEGER DEFAULT 1,
                    house_xp INTEGER DEFAULT 0,
                    last_passive_claim INTEGER DEFAULT 0,
                    boosters TEXT DEFAULT '[]',
                    inventory TEXT DEFAULT '[]',
                    gifts_sent INTEGER DEFAULT 0,
                    gifts_received INTEGER DEFAULT 0,
                    total_harvest INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    active_boosters TEXT DEFAULT '[]'
                )
            """)
            logging.info("Таблица users проверена!")
            
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS inventory TEXT DEFAULT '[]'")
                logging.info("Колонка inventory добавлена/проверена!")
            except Exception as e:
                logging.info(f"Колонка inventory уже существует: {e}")
            
            try:
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS active_boosters TEXT DEFAULT '[]'")
                logging.info("Колонка active_boosters добавлена/проверена!")
            except Exception as e:
                logging.info(f"Колонка active_boosters уже существует: {e}")
            
            columns_to_add = [
                ("farm_plots", "TEXT DEFAULT '[\"empty\",\"empty\",\"empty\"]'"),
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

# =============================================================================
# ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
# =============================================================================
async def get_user(user_id):
    """Получение данных пользователя из БД"""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                result = dict(row)
                try:
                    result['farm_plots'] = json.loads(row['farm_plots']) if row['farm_plots'] else ["empty"] * 3
                    result['boosters'] = json.loads(row['boosters']) if row['boosters'] else []
                    result['inventory'] = json.loads(row['inventory']) if row['inventory'] else []
                    result['active_boosters'] = json.loads(row['active_boosters']) if row['active_boosters'] else []
                except:
                    result['farm_plots'] = ["empty"] * 3
                    result['boosters'] = []
                    result['inventory'] = []
                    result['active_boosters'] = []
                return result
            return None
    except Exception as e:
        logging.error(f"Ошибка get_user: {e}")
        return None

async def get_user_by_username(username):
    """Поиск пользователя по username"""
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE username = $1", username.lower())
            return dict(row) if row else None
    except:
        return None

async def add_user(user_id, ref_code=None, username=None):
    """Добавление нового пользователя"""
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
                "INSERT INTO users (user_id, ref_code, invited_by, username, farm_plots, tools, boosters, inventory, active_boosters) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) ON CONFLICT (user_id) DO NOTHING",
                user_id, my_ref_code, inviter_id, username.lower() if username else None, 
                '["empty","empty","empty"]', '[]', '[]', '[]', '[]'
            )
            
            if inviter_id:
                await conn.execute("UPDATE users SET balance = balance + $1, total_invited = total_invited + 1 WHERE user_id = $2", REFERRAL_BONUS, inviter_id)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", REFERRAL_BONUS, user_id)
            
            return await get_user(user_id)
    except Exception as e:
        logging.error(f"Ошибка add_user: {e}")
        return None

async def update_balance(user_id, amount):
    """Обновление баланса пользователя"""
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def update_collect_time(user_id, timestamp):
    """Обновление времени последнего сбора"""
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_collect = $1 WHERE user_id = $2", timestamp, user_id)

async def update_bonus_time(user_id, timestamp):
    """Обновление времени последнего бонуса"""
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET last_bonus = $1 WHERE user_id = $2", timestamp, user_id)

async def add_passive_income(user_id, amount):
    """Начисление пассивного дохода"""
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def buy_item(user_id, item_id):
    """Покупка предмета из магазина"""
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

# =============================================================================
# ФУНКЦИИ БУСТЕРОВ
# =============================================================================
async def activate_booster(user_id, booster_id):
    """Активация бустера для пользователя"""
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        booster = BOOSTERS.get(booster_id)
        if not booster:
            return False, "Бустер не найден"
        
        if user['balance'] < booster['price']:
            return False, f"Недостаточно винограда! Нужно {booster['price']} 🍇"
        
        now = int(time.time())
        boosters = user.get('active_boosters', [])
        
        new_booster = {
            "id": booster_id,
            "name": booster['name'],
            "activated_at": now,
            "duration": booster['duration'],
            "expires_at": now + booster['duration'] if booster['duration'] > 0 else 0
        }
        boosters.append(new_booster)
        
        boosters = [b for b in boosters if b['expires_at'] > now or b['duration'] == 0]
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, active_boosters = $2 WHERE user_id = $3",
                booster['price'], json.dumps(boosters), user_id
            )
        
        return True, f"{booster['name']} активирован!"
    except Exception as e:
        logging.error(f"Ошибка activate_booster: {e}")
        return False, "Ошибка активации бустера"

def get_active_booster_effect(user_id, user_data, effect_type):
    """Получение множителя от активного бустера"""
    now = int(time.time())
    boosters = user_data.get('active_boosters', [])
    
    for booster in boosters:
        if booster.get('expires_at', 0) > now:
            if booster.get('id') == 'speed' and effect_type == 'speed':
                return booster.get('bonus', 1.0)
            elif booster.get('id') == 'double' and effect_type == 'double':
                return booster.get('bonus', 1.0)
    
    return 1.0

async def get_active_boosters(user_id):
    """Получение активных бустеров пользователя"""
    user = await get_user(user_id)
    if not user:
        return []
    
    boosters = user.get('boosters', [])
    now = int(time.time())
    
    return [b for b in boosters if isinstance(b, dict) and b.get('expires_at', 0) > now]

async def get_booster_effect(user_id, effect_type):
    """Получение эффекта бустера"""
    boosters = await get_active_boosters(user_id)
    multiplier = 1.0
    
    for booster in boosters:
        if booster.get('effect') == effect_type or booster.get('effect') == 'all':
            multiplier = max(multiplier, booster.get('bonus', 1.0))
    
    return multiplier

# =============================================================================
# ФУНКЦИИ ФЕРМЫ
# =============================================================================
async def plant_crop(user_id, plot_index, crop_id):
    """Посадка культуры на грядку"""
    try:
        user = await get_user(user_id)
        if not user:
            logging.error(f"Пользователь {user_id} не найден")
            return False, "Пользователь не найден"
        
        plots = user.get('farm_plots', ["empty"] * 3)
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
        
        speed_multiplier = get_active_booster_effect(user_id, user, 'speed')
        
        plots[plot_index] = {
            "crop": crop_id,
            "planted_at": int(time.time()),
            "growth_time": int(crop['growth_time'] / speed_multiplier)
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
    """Сбор урожая с грядки"""
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
        planted = plot.get('planted_at', 0)
        growth_time = plot.get('growth_time', crop['growth_time'])
        ready_time = planted + growth_time
        
        if now < ready_time:
            remaining = ready_time - now
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            return False, f"Ещё растёт! {hours}ч {minutes}м"
        
        reward = crop['reward']
        double_multiplier = get_active_booster_effect(user_id, user, 'double')
        reward = int(reward * double_multiplier)
        
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
        
        return True, f"Собрано: {reward} 🍇 (+{crop['xp']} XP)"
    except Exception as e:
        logging.error(f"Ошибка harvest_crop: {e}")
        return False, "Ошибка сбора"

async def upgrade_farm(user_id):
    """Улучшение фермы с высокими ценами"""
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_level = user.get('farm_level', 1)
        
        if current_level >= 7:
            return False, "Максимальный уровень фермы (15 грядок)!"
        
        next_level = current_level + 1
        upgrade_info = FARM_UPGRADES.get(next_level)
        
        if not upgrade_info:
            return False, "Ошибка улучшения"
        
        if user['balance'] < upgrade_info['upgrade_cost']:
            return False, f"Нужно {upgrade_info['upgrade_cost']:,} 🍇"
        
        new_plots = ["empty"] * upgrade_info['plots']
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, farm_level = $2, farm_plots = $3 WHERE user_id = $4",
                upgrade_info['upgrade_cost'], next_level, json.dumps(new_plots), user_id
            )
        
        return True, f"Ферма улучшена до уровня {next_level}! Теперь {upgrade_info['plots']} грядок."
    except Exception as e:
        logging.error(f"Ошибка upgrade_farm: {e}")
        return False, "Ошибка улучшения"

async def upgrade_farm_level(user_id):
    """Старая функция улучшения (для совместимости)"""
    return await upgrade_farm(user_id)

async def upgrade_house_level(user_id, house_id):
    """Улучшение дома"""
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
    """Получение пассивного дохода от дома"""
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
    """Добавление предмета в инвентарь"""
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
    """Отправка подарка между пользователями"""
    async with pool.acquire() as conn:
        commission = int(amount * GIFT_COMMISSION / 100)
        received = amount - commission
        
        await conn.execute("UPDATE users SET balance = balance - $1 WHERE user_id = $2", amount, from_user_id)
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", received, to_user_id)
        await conn.execute("UPDATE users SET gifts_sent = gifts_sent + 1, total_gifted = total_gifted + $1 WHERE user_id = $2", amount, from_user_id)
        await conn.execute("UPDATE users SET gifts_received = gifts_received + 1, total_received = total_received + $1 WHERE user_id = $2", received, to_user_id)
        
        return commission, received

async def get_skin_emoji(skin):
    """Получение эмодзи для скина"""
    return {"grape": "🍇", "wine": "🍷", "diamond": "💎", "gold": "🏆"}.get(skin, "🍇")

async def get_top_users(limit=10):
    """Получение топ пользователей по балансу"""
    async with pool.acquire() as conn:
        return await conn.fetch("SELECT user_id, balance FROM users ORDER BY balance DESC LIMIT $1", limit)

async def get_total_users():
    """Получение общего количества пользователей"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT COUNT(*) FROM users")
        return row['count']

async def get_total_grapes():
    """Получение общего количества винограда"""
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT SUM(balance) FROM users")
        return row['sum'] if row['sum'] else 0

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ БОТА
# =============================================================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =============================================================================
# КОМАНДЫ БОТА (БЕЗ СЛЭША)
# =============================================================================

@dp.message(CommandStart())
async def cmd_start(message: Message):
    """Команда старт"""
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code, username)
        
        if user:
            text = (
                "🍇 Добро пожаловать в Виноградную Ферму! 🍇\n\n"
                f"Привет, {message.from_user.first_name}! 👋\n\n"
                "🌟 Что ты можешь делать:\n"
                "🌱 Выращивать виноград и другие культуры\n"
                "🏠 Строить дома и получать пассивный доход\n"
                "🎁 Покупать подарки и обмениваться с друзьями\n"
                "🏆 Соревноваться с другими игроками\n\n"
                "📚 Быстрый старт:\n"
                "1. ферма - посади культуры\n"
                "2. сбор - собери виноград\n"
                "3. магазин - купи улучшения\n\n"
                "💡 Совет: Начинай с винограда - он растёт быстрее всего!\n\n"
                "🎮 Основные команды:\n"
                "помощь - полная справка по боту"
            )
            await message.answer(text)
        else:
            await message.answer("❌ Произошла ошибка при регистрации. Попробуйте ещё раз!")
    except Exception as e:
        logging.error(f"Ошибка cmd_start: {e}")
        await message.answer("❌ Произошла ошибка. Попробуйте позже.")

@dp.message(F.text == "ферма")
async def cmd_farm(message: Message):
    """Команда ферма - отображение фермы с интерактивными грядками"""
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user:
            user = await add_user(user_id, username=message.from_user.username)
            if not user:
                await message.answer("❌ Ошибка")
                return
        
        plots = user.get('farm_plots', ["empty"] * 3)
        farm_level = user.get('farm_level', 1)
        balance = user.get('balance', 0)
        now = int(time.time())
        
        keyboard = InlineKeyboardBuilder()
        grid_text = ""
        
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                # Пустая грядка
                grid_text += f"{i+1}. 🟫 Пусто\n"
                keyboard.button(
                    text=f"🟫 Грядка {i+1}",
                    callback_data=f"plot_empty_{i}"
                )
            else:
                crop = CROPS.get(plot.get('crop'))
                if crop:
                    planted = plot.get('planted_at', 0)
                    growth_time = plot.get('growth_time', crop['growth_time'])
                    ready_time = planted + growth_time
                    
                    if now >= ready_time:
                        # Готово к сбору
                        grid_text += f"{i+1}. {crop['name']} ✅\n"
                        keyboard.button(
                            text=f"✅ {crop['name']} ({i+1})",
                            callback_data=f"plot_ready_{i}"
                        )
                    else:
                        # Ещё растёт - показываем время
                        remaining = ready_time - now
                        h = remaining // 3600
                        m = (remaining % 3600) // 60
                        grid_text += f"{i+1}. {crop['name']} ⏳{h}ч{m}м\n"
                        keyboard.button(
                            text=f"⏳ Грядка {i+1}",
                            callback_data=f"plot_growing_{i}"
                        )
        
        # Кнопки управления фермой
        keyboard.button(text="🌱 Посадить", callback_data="farm_plant")
        keyboard.button(text="🚜 Улучшить ферму", callback_data="farm_upgrade")
        keyboard.button(text="📊 Инфо", callback_data="farm_stats")
        keyboard.button(text="🔄 Обновить", callback_data="farm_refresh")
        keyboard.adjust(2)
        
        # Статистика грядок
        empty_plots = sum(1 for p in plots if p == "empty" or not p or not isinstance(p, dict))
        ready_plots = sum(1 for i, p in enumerate(plots) if isinstance(p, dict) and CROPS.get(p.get('crop')) and now >= p.get('planted_at', 0) + p.get('growth_time', CROPS.get(p.get('crop'))['growth_time']))
        growing_plots = len(plots) - empty_plots - ready_plots
        
        upgrade_info = FARM_UPGRADES.get(farm_level + 1)
        upgrade_text = f"\n🔜 След. уровень: {upgrade_info['upgrade_cost']:,} 🍇" if upgrade_info else ""
        
        text = (
            f"🌾 ВАША ФЕРМА (ур. {farm_level}){upgrade_text}\n\n"
            f"💰 Баланс: {balance:,} 🍇\n\n"
            f"📍 Грядки ({len(plots)} шт):\n"
            f"🟫 Пустых: {empty_plots}\n"
            f"✅ Готово: {ready_plots}\n"
            f"⏳ Растёт: {growing_plots}\n\n"
            f"💡 Нажмите на грядку!"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_farm: {e}")
        await message.answer("❌ Ошибка")

@dp.message(F.text == "сбор")
async def cmd_collect(message: Message):
    """Команда сбор - сбор винограда"""
    try:
        user_id = message.from_user.id
        user = await get_user(user_id)
        if not user:
            user = await add_user(user_id, username=message.from_user.username)
            if not user:
                await message.answer("❌ Ошибка")
                return
        
        now = int(time.time())
        last_time = user.get('last_collect', 0)
        cooldown = COOLDOWN_SECONDS
        
        if now - last_time < cooldown:
            remaining = cooldown - (now - last_time)
            h = remaining // 3600
            m = (remaining % 3600) // 60
            await message.answer(f"⏳ Подождите: {h}ч {m}м")
            return
        
        reward = GRAPE_REWARD
        await update_balance(user_id, reward)
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET last_collect = $1 WHERE user_id = $2", now, user_id)
        
        new_user = await get_user(user_id)
        await message.answer(f"🍇 Собрано: {reward}\n💰 Баланс: {new_user['balance']}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "бустеры")
async def cmd_boosters(message: Message):
    """Команда бустеры - магазин бустеров"""
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала ферма")
            return
        
        balance = user.get('balance', 0)
        
        keyboard = InlineKeyboardBuilder()
        for booster_id, booster in BOOSTERS.items():
            keyboard.button(
                text=f"{booster['name']} - {booster['price']} 🍇",
                callback_data=f"buy_booster_{booster_id}"
            )
        keyboard.adjust(1)
        
        active_text = "\n\n⚡ Активные бустеры:\n"
        now = int(time.time())
        has_active = False
        
        for b in user.get('active_boosters', []):
            if b.get('expires_at', 0) > now:
                remaining = b['expires_at'] - now
                mins = remaining // 60
                active_text += f"• {b['name']} - {mins} мин\n"
                has_active = True
        
        if not has_active:
            active_text = "\n\n⚡ Активных бустеров нет"
        
        text = (
            f"🚀 МАГАЗИН БУСТЕРОВ\n\n"
            f"💰 Баланс: {balance:,} 🍇{active_text}\n\n"
            f"Выберите бустер:"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "дом")
async def cmd_house(message: Message):
    """Команда дом"""
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала старт")
            return
        
        house_level = user.get('house_level', 1)
        balance = user.get('balance', 0)
        
        house = HOUSES.get('tent')
        for h in HOUSES.values():
            if h['level'] == house_level:
                house = h
                break
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💰 Забрать", callback_data="house_claim")
        keyboard.adjust(1)
        
        text = (
            f"🏠 {house['name']}\n\n"
            f"📊 Уровень: {house_level}\n"
            f"💰 Доход: {house['passive_income']}/час\n"
            f"💵 Баланс: {balance:,} 🍇"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "подарки")
async def cmd_gifts(message: Message):
    """Команда подарки"""
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in GIFT_CATALOG.items():
            keyboard.button(text=f"{item['name']} - {item['price']} 🍇", callback_data=f"gift_{item_id}")
        keyboard.adjust(2)
        
        await message.answer(f"🎁 Магазин\n💰 Баланс: {balance} 🍇", reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "инвентарь")
async def cmd_inventory(message: Message):
    """Команда инвентарь"""
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        inventory = user.get('inventory', [])
        if not inventory:
            await message.answer("📦 Пусто\n\nподарки - купить")
            return
        
        text = "📦 Ваш инвентарь\n\n"
        keyboard = InlineKeyboardBuilder()
        
        item_counts = {}
        for item in inventory:
            if isinstance(item, dict):
                item_id = item.get('item_id')
                qty = item.get('quantity', 1)
                if item_id:
                    item_counts[item_id] = item_counts.get(item_id, 0) + qty
        
        for item_id, count in item_counts.items():
            item_info = GIFT_CATALOG.get(item_id)
            if item_info:
                text += f"{item_info['name']} x{count}\n"
                keyboard.button(text=f"🎁 Передать {item_info['name']}", callback_data=f"transfer_{item_id}")
        
        keyboard.adjust(1)
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "баланс")
async def cmd_balance(message: Message):
    """Команда баланс"""
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала старт")
            return
        
        text = (
            f"💰 Баланс\n\n"
            f"🍇 Виноград: {user.get('balance', 0):,}\n"
            f"🌾 Ферма: ур. {user.get('farm_level', 1)}\n"
            f"🏠 Дом: ур. {user.get('house_level', 1)}"
        )
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message(F.text == "помощь")
async def cmd_help(message: Message):
    """Команда помощь"""
    text = (
        "📚 Справка\n\n"
        "ферма - ваша ферма и грядки\n"
        "посадить [номер] [культура] - посадить\n"
        "собрать [номер] - собрать урожай\n"
        "сбор - собрать виноград\n\n"
        "дом - ваш дом и доход\n"
        "бустеры - магазин ускорений\n\n"
        "подарки - магазин подарков\n"
        "инвентарь - ваши подарки\n"
        "инвентарь @user - чужой инвентарь\n"
        "передать @user предмет - подарить\n\n"
        "баланс - ваш баланс и прогресс\n"
        "магазин - улучшения и скины\n\n"
        "топ - рейтинг игроков\n"
        "статистика - статистика бота"
    )
    await message.answer(text)

@dp.message(F.text == "топ")
async def cmd_top(message: Message):
    """Команда топ"""
    try:
        top = await get_top_users(10)
        if not top:
            await message.answer("📊 Пока нет игроков. Будьте первыми!")
            return
        
        text = "🏆 Топ игроков 🏆\n\n"
        text += "🥇 Лучшие фермеры:\n\n"
        
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
    """Команда статистика"""
    try:
        total = await get_total_users()
        grapes = await get_total_grapes()
        
        text = (
            "📊 Статистика бота\n\n"
            f"👥 Всего игроков: {total:,}\n"
            f"🍇 Винограда собрано: {grapes:,}\n\n"
            f"🌟 Присоединяйтесь!\n"
            f"Станьте частью нашего сообщества!"
        )
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_stats: {e}")
        await message.answer("❌ Ошибка при загрузке статистики.")

# =============================================================================
# CALLBACK ОБРАБОТЧИКИ
# =============================================================================

@dp.callback_query(lambda c: c.data.startswith("plot_"))
async def callback_plot_action(callback: CallbackQuery):
    """Обработчик действий с грядками"""
    try:
        await callback.answer()
        parts = callback.data.split("_")
        action = parts[1]
        plot_index = int(parts[2])
        
        if action == "empty":
            keyboard = InlineKeyboardBuilder()
            for crop_id, crop in CROPS.items():
                keyboard.button(
                    text=f"{crop['name']} - {crop['cost']} 🍇",
                    callback_data=f"plant_{crop_id}_{plot_index}"
                )
            keyboard.adjust(2)
            keyboard.button(text="◀️ Назад", callback_data="farm_back")
            
            await callback.message.answer(
                f"🌱 Посадка на грядку {plot_index + 1}\n\nВыберите культуру:",
                reply_markup=keyboard.as_markup()
            )
        
        elif action == "ready":
            success, msg = await harvest_crop(callback.from_user.id, plot_index)
            await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
            await callback.message.delete()
            await cmd_farm(callback.message)
        
        elif action == "growing":
            await callback.message.answer("⏳ Культура ещё растёт...")
    
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("plant_"))
async def callback_plant(callback: CallbackQuery):
    """Обработчик посадки на грядку"""
    try:
        await callback.answer()
        parts = callback.data.replace("plant_", "").split("_")
        crop_id = parts[0]
        plot_index = int(parts[1])
        
        success, msg = await plant_crop(callback.from_user.id, plot_index, crop_id)
        await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
        
        if success:
            await callback.message.delete()
            await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_plant")
async def callback_farm_plant(callback: CallbackQuery):
    """Выбор культуры для посадки"""
    try:
        await callback.answer()
        keyboard = InlineKeyboardBuilder()
        for crop_id, crop in CROPS.items():
            keyboard.button(text=f"{crop['name']} - {crop['cost']} 🍇", callback_data=f"select_crop_{crop_id}")
        keyboard.adjust(2)
        await callback.message.answer("🌱 Выберите культуру:", reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("select_crop_"))
async def callback_select_crop(callback: CallbackQuery):
    """Выбор грядки для посадки"""
    try:
        await callback.answer()
        crop_id = callback.data.replace("select_crop_", "")
        
        user = await get_user(callback.from_user.id)
        plots = user.get('farm_plots', [])
        
        keyboard = InlineKeyboardBuilder()
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                keyboard.button(text=f"🌾 Грядка {i+1}", callback_data=f"plant_{crop_id}_{i}")
        
        if not keyboard.buttons:
            await callback.message.answer("❌ Нет свободных грядок")
            return
        
        keyboard.adjust(3)
        await callback.message.answer(f"🌱 Выберите грядку:", reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_upgrade")
async def callback_farm_upgrade(callback: CallbackQuery):
    """Обработчик улучшения фермы"""
    try:
        await callback.answer()
        success, msg = await upgrade_farm(callback.from_user.id)
        await callback.message.answer(msg)
        if success:
            await callback.message.delete()
            await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_stats")
async def callback_farm_stats(callback: CallbackQuery):
    """Статистика фермы"""
    try:
        await callback.answer()
        user = await get_user(callback.from_user.id)
        if user:
            farm_level = user.get('farm_level', 1)
            plots = user.get('farm_plots', [])
            
            upgrade_info = FARM_UPGRADES.get(farm_level + 1)
            next_plots = upgrade_info['plots'] if upgrade_info else 15
            
            text = (
                f"📊 СТАТИСТИКА ФЕРМЫ\n\n"
                f"🌾 Уровень: {farm_level}\n"
                f"⭐ XP: {user.get('farm_xp', 0)}\n"
                f"🚜 Грядок: {len(plots)} из {next_plots}\n"
                f"🏆 Урожаев: {user.get('total_harvest', 0)}\n\n"
                f"💡 Улучшите ферму для новых грядок!"
            )
            await callback.message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_back")
async def callback_farm_back(callback: CallbackQuery):
    """Кнопка назад"""
    try:
        await callback.answer()
        await callback.message.delete()
        await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_refresh")
async def callback_farm_refresh(callback: CallbackQuery):
    """Обновление фермы"""
    try:
        await callback.answer("🔄 Обновляю...")
        await callback.message.delete()
        await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_refresh")
async def callback_farm_refresh(callback: CallbackQuery):
    """Обновление фермы"""
    try:
        await callback.answer("🔄 Обновляю...")
        await callback.message.delete()
        await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_booster_"))
async def callback_buy_booster(callback: CallbackQuery):
    """Покупка бустера"""
    try:
        await callback.answer()
        booster_id = callback.data.replace("buy_booster_", "")
        
        success, msg = await activate_booster(callback.from_user.id, booster_id)
        await callback.message.answer(msg)
        
        if success:
            await callback.message.delete()
            await cmd_boosters(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("gift_"))
async def callback_gift_buy(callback: CallbackQuery):
    """Покупка подарка"""
    try:
        await callback.answer()
        item_id = callback.data.replace("gift_", "")
        item = GIFT_CATALOG.get(item_id)
        
        if not item:
            await callback.message.answer("❌ Не найдено")
            return
        
        user = await get_user(callback.from_user.id)
        if user['balance'] < item['price']:
            await callback.message.answer("❌ Недостаточно 🍇")
            return
        
        await update_balance(callback.from_user.id, -item['price'])
        success = await add_to_inventory(callback.from_user.id, item_id)
        
        if success:
            await callback.message.answer(f"✅ {item['name']} куплен!\nинвентарь - посмотреть")
        else:
            await update_balance(callback.from_user.id, item['price'])
            await callback.message.answer("❌ Ошибка")
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("house_"))
async def callback_house(callback: CallbackQuery):
    """Обработчик действий с домом"""
    try:
        await callback.answer()
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
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_"))
async def callback_buy(callback: CallbackQuery):
    """Покупка предмета из магазина"""
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
        logging.error(f"Ошибка: {e}")

# =============================================================================
# ЗАПУСК БОТА
# =============================================================================
async def main():
    """Главная функция запуска"""
    try:
        await init_db()
        logging.info("Бот запущен!")
        await dp.start_polling(bot)
    except Exception as e:
        logging.error(f"Критическая ошибка: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(main())
