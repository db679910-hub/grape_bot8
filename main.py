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
from aiogram.types import Message, CallbackQuery, FSInputFile
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# =============================================================================
# НАСТРОЙКИ БОТА
# =============================================================================
BOT_TOKEN = os.getenv("BOT_TOKEN")
DATABASE_URL = os.getenv("DATABASE_URL")

# =============================================================================
# ЭКОНОМИКА (СБАЛАНСИРОВАННАЯ)
# =============================================================================
GRAPE_REWARD = 10
COOLDOWN_HOURS = 4
COOLDOWN_SECONDS = COOLDOWN_HOURS * 3600
DAILY_BONUS = 100
REFERRAL_BONUS = 200
REFERRAL_PERCENT = 5

# =============================================================================
# КУЛЬТУРЫ (БЕЗ ПРОБЕЛОВ В КЛЮЧАХ)
# =============================================================================
CROPS = {
    "grape": {"name": "🍇 Виноград", "cost": 0, "reward": 10, "growth_time": 7200, "xp": 5},
    "strawberry": {"name": "🍓 Клубника", "cost": 50, "reward": 120, "growth_time": 10800, "xp": 15},
    "corn": {"name": "🌽 Кукуруза", "cost": 150, "reward": 350, "growth_time": 14400, "xp": 30},
    "tomato": {"name": "🍅 Томат", "cost": 400, "reward": 900, "growth_time": 21600, "xp": 60},
    "pumpkin": {"name": "🎃 Тыква", "cost": 1000, "reward": 2500, "growth_time": 28800, "xp": 120},
    "melon": {"name": "🍈 Дыня", "cost": 2500, "reward": 6000, "growth_time": 43200, "xp": 250},
    "pineapple": {"name": "🍍 Ананас", "cost": 6000, "reward": 15000, "growth_time": 57600, "xp": 500},
    "diamond_grape": {"name": "💎 Алмазный виноград", "cost": 15000, "reward": 40000, "growth_time": 86400, "xp": 1000},
}

# =============================================================================
# УЛУЧШЕНИЯ ФЕРМЫ (МАКС 15 ГРЯДОК)
# =============================================================================
FARM_UPGRADES = {
    1: {"level": 1, "plots": 3, "upgrade_cost": 0},
    2: {"level": 2, "plots": 5, "upgrade_cost": 2000},
    3: {"level": 3, "plots": 7, "upgrade_cost": 8000},
    4: {"level": 4, "plots": 9, "upgrade_cost": 20000},
    5: {"level": 5, "plots": 11, "upgrade_cost": 50000},
    6: {"level": 6, "plots": 13, "upgrade_cost": 120000},
    7: {"level": 7, "plots": 15, "upgrade_cost": 300000},
}

# =============================================================================
# ДОМА (СБАЛАНСИРОВАННЫЙ ДОХОД)
# =============================================================================
HOUSES = {
    1: {"name": "⛺ Палатка", "level": 1, "price": 0, "passive_income": 5, "maintenance": 0},
    2: {"name": "🏕️ Лагерь", "level": 2, "price": 5000, "passive_income": 25, "maintenance": 3},
    3: {"name": "🛖 Хижина", "level": 3, "price": 15000, "passive_income": 80, "maintenance": 10},
    4: {"name": "🏡 Коттедж", "level": 4, "price": 40000, "passive_income": 200, "maintenance": 30},
    5: {"name": "🏰 Особняк", "level": 5, "price": 100000, "passive_income": 500, "maintenance": 80},
    6: {"name": "🏯 Замок", "level": 6, "price": 300000, "passive_income": 1500, "maintenance": 250},
    7: {"name": "👑 Дворец", "level": 7, "price": 800000, "passive_income": 4000, "maintenance": 700},
    8: {"name": "☁️ Небесный дворец", "level": 8, "price": 2000000, "passive_income": 10000, "maintenance": 2000},
    9: {"name": "🌟 Дворец богов", "level": 9, "price": 5000000, "passive_income": 25000, "maintenance": 5000},
}

# =============================================================================
# СКИНЫ (ОТОБРАЖЕНИЕ В ПРОФИЛЕ)
# =============================================================================
SKINS = {
    "default": {"name": "🍇 Классический", "price": 0, "emoji": "🍇", "description": "Обычный виноград"},
    "gold": {"name": "🏆 Золотой", "price": 5000, "emoji": "🏆", "description": "Роскошный золотой скин"},
    "diamond": {"name": "💎 Алмазный", "price": 15000, "emoji": "💎", "description": "Редкий алмазный скин"},
    "rainbow": {"name": "🌈 Радужный", "price": 30000, "emoji": "🌈", "description": "Уникальный радужный скин"},
    "dark": {"name": "🌑 Тёмный", "price": 50000, "emoji": "🌑", "description": "Загадочный тёмный скин"},
}

# =============================================================================
# ПОДАРКИ (С РУССКИМ ОПИСАНИЕМ)
# =============================================================================
GIFT_CATALOG = {
    "chocolate": {"name": "🍫 Шоколадка", "price": 50, "rarity": "common", "description": "Вкусная молочная шоколадка для друга"},
    "flower": {"name": "🌹 Роза", "price": 100, "rarity": "common", "description": "Красная роза как знак внимания"},
    "balloon": {"name": "🎈 Шарик", "price": 75, "rarity": "common", "description": "Воздушный шарик для настроения"},
    "cupcake": {"name": "🧁 Капкейк", "price": 150, "rarity": "common", "description": "Сладкий кекс с кремом"},
    "teddy": {"name": "🧸 Мишка", "price": 300, "rarity": "rare", "description": "Плюшевый мишка для уюта"},
    "ring": {"name": "💍 Кольцо", "price": 500, "rarity": "rare", "description": "Серебряное кольцо с камнем"},
    "necklace": {"name": "📿 Ожерелье", "price": 750, "rarity": "rare", "description": "Изумительное ожерелье"},
    "car": {"name": "🏎️ Спорткар", "price": 2000, "rarity": "epic", "description": "Быстрый спортивный автомобиль"},
    "house": {"name": "🏡 Дом", "price": 5000, "rarity": "epic", "description": "Уютный загородный дом"},
    "yacht": {"name": "🛥️ Яхта", "price": 10000, "rarity": "epic", "description": "Роскошная яхта для путешествий"},
    "diamond": {"name": "💎 Алмаз", "price": 15000, "rarity": "legendary", "description": "Огромный редкий алмаз"},
    "crown": {"name": "👑 Корона", "price": 25000, "rarity": "legendary", "description": "Королевская золотая корона"},
    "rocket": {"name": "🚀 Ракета", "price": 50000, "rarity": "legendary", "description": "Космическая ракета для полёта к звёздам"},
}

# =============================================================================
# БУСТЕРЫ (АВТОМАТИЧЕСКАЯ АКТИВАЦИЯ)
# =============================================================================
BOOSTERS = {
    "speed_2h": {"name": "⚡ Ускорение 2ч", "price": 100, "duration": 7200, "effect": "growth_speed", "bonus": 1.5, "description": "Ускоряет рост культур в 1.5 раза"},
    "speed_8h": {"name": "⚡ Ускорение 8ч", "price": 300, "duration": 28800, "effect": "growth_speed", "bonus": 1.5, "description": "Ускоряет рост культур в 1.5 раза"},
    "speed_24h": {"name": "⚡ Ускорение 24ч", "price": 700, "duration": 86400, "effect": "growth_speed", "bonus": 1.5, "description": "Ускоряет рост культур в 1.5 раза"},
    "yield_2h": {"name": "📈 Урожай 2ч", "price": 150, "duration": 7200, "effect": "yield", "bonus": 1.3, "description": "Увеличивает награду за сбор на 30%"},
    "yield_8h": {"name": "📈 Урожай 8ч", "price": 400, "duration": 28800, "effect": "yield", "bonus": 1.3, "description": "Увеличивает награду за сбор на 30%"},
    "yield_24h": {"name": "📈 Урожай 24ч", "price": 900, "duration": 86400, "effect": "yield", "bonus": 1.3, "description": "Увеличивает награду за сбор на 30%"},
    "income_4h": {"name": "💰 Доход 4ч", "price": 250, "duration": 14400, "effect": "passive_income", "bonus": 2.0, "description": "Удваивает пассивный доход от дома"},
    "income_12h": {"name": "💰 Доход 12ч", "price": 600, "duration": 43200, "effect": "passive_income", "bonus": 2.0, "description": "Удваивает пассивный доход от дома"},
    "income_24h": {"name": "💰 Доход 24ч", "price": 1000, "duration": 86400, "effect": "passive_income", "bonus": 2.0, "description": "Удваивает пассивный доход от дома"},
    "super_4h": {"name": "🌟 СУПЕР 4ч", "price": 1500, "duration": 14400, "effect": "all", "bonus": 1.5, "description": "Действует на все улучшения сразу"},
    "super_12h": {"name": "🌟 СУПЕР 12ч", "price": 3500, "duration": 43200, "effect": "all", "bonus": 1.5, "description": "Действует на все улучшения сразу"},
    "super_24h": {"name": "🌟 СУПЕР 24ч", "price": 6000, "duration": 86400, "effect": "all", "bonus": 1.5, "description": "Действует на все улучшения сразу"},
}

# =============================================================================
# НАСТРОЙКИ ЛОГИРОВАНИЯ
# =============================================================================
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
pool = None

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ БАЗЫ ДАННЫХ
# =============================================================================
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
                    username VARCHAR(100),
                    farm_level INTEGER DEFAULT 1,
                    farm_xp INTEGER DEFAULT 0,
                    farm_plots TEXT DEFAULT '["empty","empty","empty"]',
                    house_level INTEGER DEFAULT 1,
                    house_xp INTEGER DEFAULT 0,
                    last_passive_claim INTEGER DEFAULT 0,
                    inventory TEXT DEFAULT '[]',
                    gifts_sent INTEGER DEFAULT 0,
                    gifts_received INTEGER DEFAULT 0,
                    total_harvest INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0,
                    active_boosters TEXT DEFAULT '[]',
                    skin VARCHAR(50) DEFAULT 'default',
                    last_daily_bonus INTEGER DEFAULT 0
                )
            """)
            
            columns_to_add = [
                ("active_boosters", "TEXT DEFAULT '[]'"),
                ("skin", "VARCHAR(50) DEFAULT 'default'"),
                ("last_daily_bonus", "INTEGER DEFAULT 0")
            ]
            
            for col_name, col_type in columns_to_add:
                try:
                    await conn.execute(f"ALTER TABLE users ADD COLUMN IF NOT EXISTS {col_name} {col_type}")
                    logging.info(f"Колонка {col_name} добавлена!")
                except Exception as e:
                    logging.info(f"Колонка {col_name} уже существует")
        
        logging.info("База данных готова!")
    except Exception as e:
        logging.error(f"Ошибка БД: {e}")
        raise

# =============================================================================
# ФУНКЦИИ РАБОТЫ С ПОЛЬЗОВАТЕЛЯМИ
# =============================================================================
async def get_user(user_id):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if row:
                result = dict(row)
                try:
                    result['farm_plots'] = json.loads(row['farm_plots']) if row['farm_plots'] else ["empty"] * 3
                    result['inventory'] = json.loads(row['inventory']) if row['inventory'] else []
                    result['active_boosters'] = json.loads(row['active_boosters']) if row['active_boosters'] else []
                except:
                    result['farm_plots'] = ["empty"] * 3
                    result['inventory'] = []
                    result['active_boosters'] = []
                return result
            return None
    except Exception as e:
        logging.error(f"Ошибка get_user: {e}")
        return None

async def get_user_by_username(username):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM users WHERE username ILIKE $1", username.lower())
            return dict(row) if row else None
    except:
        return None

async def add_user(user_id, username=None):
    try:
        async with pool.acquire() as conn:
            existing = await conn.fetchrow("SELECT * FROM users WHERE user_id = $1", user_id)
            if existing:
                if username:
                    await conn.execute("UPDATE users SET username = $1 WHERE user_id = $2", username.lower(), user_id)
                return dict(existing)
            
            await conn.execute(
                "INSERT INTO users (user_id, username) VALUES ($1, $2) ON CONFLICT (user_id) DO NOTHING",
                user_id, username.lower() if username else None
            )
            
            return await get_user(user_id)
    except Exception as e:
        logging.error(f"Ошибка add_user: {e}")
        return None

async def update_balance(user_id, amount):
    async with pool.acquire() as conn:
        await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", amount, user_id)

async def get_active_booster_effect(user_id, user_data, effect_type):
    now = int(time.time())
    boosters = user_data.get('active_boosters', [])
    multiplier = 1.0
    
    for booster in boosters:
        if booster.get('expires_at', 0) > now:
            if booster.get('effect') == effect_type or booster.get('effect') == 'all':
                multiplier = max(multiplier, booster.get('bonus', 1.0))
    
    return multiplier

async def activate_booster(user_id, booster_id):
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
            "expires_at": now + booster['duration'],
            "effect": booster['effect'],
            "bonus": booster['bonus']
        }
        boosters.append(new_booster)
        
        boosters = [b for b in boosters if b['expires_at'] > now]
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, active_boosters = $2 WHERE user_id = $3",
                booster['price'], json.dumps(boosters), user_id
            )
        
        return True, f"{booster['name']} активирован!"
    except Exception as e:
        logging.error(f"Ошибка activate_booster: {e}")
        return False, "Ошибка активации бустера"

async def plant_crop(user_id, plot_index, crop_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        plots = user.get('farm_plots', ["empty"] * 3)
        
        if plot_index < 0 or plot_index >= len(plots):
            return False, "Неверный номер грядки"
        
        crop = CROPS.get(crop_id)
        if not crop:
            return False, "Культура не найдена"
        
        if user['balance'] < crop['cost']:
            return False, f"Недостаточно винограда! Нужно {crop['cost']} 🍇"
        
        if plots[plot_index] != "empty" and isinstance(plots[plot_index], dict):
            return False, "Грядка занята! Сначала соберите урожай."
        
        speed_multiplier = await get_active_booster_effect(user_id, user, 'growth_speed')
        
        plots[plot_index] = {
            "crop": crop_id,
            "planted_at": int(time.time()),
            "growth_time": int(crop['growth_time'] / speed_multiplier)
        }
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, farm_plots = $2 WHERE user_id = $3",
                crop['cost'], json.dumps(plots), user_id
            )
        
        return True, f"{crop['name']} посажен!"
    except Exception as e:
        logging.error(f"Ошибка plant_crop: {e}")
        return False, "Ошибка посадки"

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
        planted = plot.get('planted_at', 0)
        growth_time = plot.get('growth_time', crop['growth_time'])
        ready_time = planted + growth_time
        
        if now < ready_time:
            remaining = ready_time - now
            hours = remaining // 3600
            minutes = (remaining % 3600) // 60
            return False, f"Ещё растёт! {hours}ч {minutes}м"
        
        reward = crop['reward']
        yield_multiplier = await get_active_booster_effect(user_id, user, 'yield')
        reward = int(reward * yield_multiplier)
        
        if random.random() < 0.1:
            reward = int(reward * 2)
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, total_harvest = total_harvest + 1, total_earned = total_earned + $1, farm_xp = farm_xp + $2 WHERE user_id = $3",
                reward, crop['xp'], user_id
            )
            plots[plot_index] = "empty"
            await conn.execute("UPDATE users SET farm_plots = $1 WHERE user_id = $2", json.dumps(plots), user_id)
        
        return True, f"Собрано: {reward} 🍇 (+{crop['xp']} XP)"
    except Exception as e:
        logging.error(f"Ошибка harvest_crop: {e}")
        return False, "Ошибка сбора"

async def upgrade_farm(user_id):
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

async def upgrade_house(user_id, house_level):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_level = user.get('house_level', 1)
        
        if house_level <= current_level:
            return False, "Нужно улучшить до следующего уровня!"
        
        if house_level != current_level + 1:
            return False, "Улучшайте по порядку!"
        
        house = HOUSES.get(house_level)
        if not house:
            return False, "Дом не найден"
        
        if user['balance'] < house['price']:
            return False, f"Нужно {house['price']:,} 🍇"
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, house_level = $2, house_xp = house_xp + $3 WHERE user_id = $4",
                house['price'], house_level, house_level * 100, user_id
            )
        
        return True, f"Дом улучшен до {house['name']}!"
    except Exception as e:
        logging.error(f"Ошибка upgrade_house: {e}")
        return False, "Ошибка улучшения дома"

async def claim_passive_income(user_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        house_level = user.get('house_level', 1)
        last_claim = user.get('last_passive_claim', 0)
        
        house = HOUSES.get(house_level)
        if not house:
            house = HOUSES[1]
        
        now = int(time.time())
        hours_passed = (now - last_claim) // 3600
        
        if hours_passed < 1:
            minutes_left = 60 - ((now - last_claim) // 60)
            return False, f"Доход через {minutes_left} мин"
        
        income_multiplier = await get_active_booster_effect(user_id, user, 'passive_income')
        base_income = house['passive_income'] * hours_passed
        maintenance = house['maintenance'] * hours_passed
        total_income = int(base_income * income_multiplier) - maintenance
        
        if total_income < 0:
            total_income = 0
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, last_passive_claim = $2 WHERE user_id = $3",
                total_income, now, user_id
            )
        
        return True, f"Получено: {total_income} 🍇 (за {hours_passed}ч)"
    except Exception as e:
        logging.error(f"Ошибка claim_passive_income: {e}")
        return False, "Ошибка получения дохода"

async def add_to_inventory(user_id, item_id):
    try:
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT inventory FROM users WHERE user_id = $1", user_id)
            
            if not row:
                return False
            
            try:
                inventory = json.loads(row['inventory']) if row['inventory'] else []
            except:
                inventory = []
            
            found = False
            for item in inventory:
                if isinstance(item, dict) and item.get('item_id') == item_id:
                    item['quantity'] = item.get('quantity', 1) + 1
                    found = True
                    break
            
            if not found:
                inventory.append({"item_id": item_id, "quantity": 1})
            
            await conn.execute(
                "UPDATE users SET inventory = $1 WHERE user_id = $2",
                json.dumps(inventory, ensure_ascii=False),
                user_id
            )
            
            return True
    except Exception as e:
        logging.error(f"Ошибка add_to_inventory: {e}")
        return False

async def transfer_gift(sender_id, recipient_id, item_id):
    try:
        async with pool.acquire() as conn:
            sender_row = await conn.fetchrow("SELECT inventory FROM users WHERE user_id = $1", sender_id)
            if not sender_row:
                return False, "Отправитель не найден"
            
            sender_inventory = json.loads(sender_row['inventory']) if sender_row['inventory'] else []
            
            item_found = False
            item_index = -1
            
            for i, inv_item in enumerate(sender_inventory):
                if isinstance(inv_item, dict) and inv_item.get('item_id') == item_id:
                    item_found = True
                    item_index = i
                    break
            
            if not item_found:
                return False, "У вас нет этого предмета"
            
            if sender_inventory[item_index].get('quantity', 1) > 1:
                sender_inventory[item_index]['quantity'] -= 1
            else:
                sender_inventory.pop(item_index)
            
            await conn.execute(
                "UPDATE users SET inventory = $1 WHERE user_id = $2",
                json.dumps(sender_inventory, ensure_ascii=False),
                sender_id
            )
            
            recipient_row = await conn.fetchrow("SELECT inventory FROM users WHERE user_id = $1", recipient_id)
            recipient_inventory = json.loads(recipient_row['inventory']) if recipient_row and recipient_row['inventory'] else []
            
            found = False
            for inv_item in recipient_inventory:
                if isinstance(inv_item, dict) and inv_item.get('item_id') == item_id:
                    inv_item['quantity'] = inv_item.get('quantity', 1) + 1
                    found = True
                    break
            
            if not found:
                recipient_inventory.append({"item_id": item_id, "quantity": 1})
            
            await conn.execute(
                "UPDATE users SET inventory = $1 WHERE user_id = $2",
                json.dumps(recipient_inventory, ensure_ascii=False),
                recipient_id
            )
            
            await conn.execute("UPDATE users SET gifts_sent = gifts_sent + 1 WHERE user_id = $1", sender_id)
            await conn.execute("UPDATE users SET gifts_received = gifts_received + 1 WHERE user_id = $1", recipient_id)
            
            return True, "Подарок передан!"
    except Exception as e:
        logging.error(f"Ошибка transfer_gift: {e}")
        return False, "Ошибка передачи"

async def buy_skin(user_id, skin_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        skin = SKINS.get(skin_id)
        if not skin:
            return False, "Скин не найден"
        
        if user['balance'] < skin['price']:
            return False, f"Недостаточно винограда! Нужно {skin['price']} 🍇"
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance - $1, skin = $2 WHERE user_id = $3",
                skin['price'], skin_id, user_id
            )
        
        return True, f"Скин {skin['name']} куплен!"
    except Exception as e:
        logging.error(f"Ошибка buy_skin: {e}")
        return False, "Ошибка покупки скина"

async def claim_daily_bonus(user_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        now = int(time.time())
        last_bonus = user.get('last_daily_bonus', 0)
        
        if now - last_bonus < 86400:
            remaining = 86400 - (now - last_bonus)
            hours = remaining // 3600
            return False, f"Ежедневный бонус через {hours} ч"
        
        async with pool.acquire() as conn:
            await conn.execute(
                "UPDATE users SET balance = balance + $1, last_daily_bonus = $2 WHERE user_id = $3",
                DAILY_BONUS, now, user_id
            )
        
        return True, f"Получено: {DAILY_BONUS} 🍇"
    except Exception as e:
        logging.error(f"Ошибка claim_daily_bonus: {e}")
        return False, "Ошибка получения бонуса"

# =============================================================================
# ИНИЦИАЛИЗАЦИЯ БОТА
# =============================================================================
bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# =============================================================================
# ГЛАВНОЕ МЕНЮ
# =============================================================================
def get_main_keyboard():
    keyboard = ReplyKeyboardBuilder()
    keyboard.button(text="🌾 Ферма")
    keyboard.button(text="🏠 Дом")
    keyboard.button(text="🎁 Подарки")
    keyboard.button(text="👤 Профиль")
    keyboard.button(text="🏪 Магазин")
    keyboard.button(text="📚 Помощь")
    keyboard.adjust(2)
    return keyboard.as_markup(resize_keyboard=True)

# =============================================================================
# КОМАНДА СТАРТ
# =============================================================================
@dp.message(CommandStart())
async def cmd_start(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        
        if user:
            skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
            
            text = (
                f"{skin['emoji']} **Добро пожаловать в Виноградную Ферму!** {skin['emoji']}\n\n"
                f"Привет, {message.from_user.first_name}! 👋\n\n"
                f"🌟 **Здесь ты можешь:**\n"
                f"🌱 Выращивать различные культуры\n"
                f"🏠 Строить дома и получать доход\n"
                f"🎁 Дарить подарки друзьям\n"
                f"🏆 Соревноваться с другими фермерами\n\n"
                f"💡 **Совет:** Начни с винограда — он растёт быстрее всего!\n\n"
                f"🎮 **Используй меню внизу для навигации**"
            )
            
            await message.answer(text, reply_markup=get_main_keyboard())
        else:
            await message.answer("❌ Ошибка регистрации. Попробуйте ещё раз!")
    except Exception as e:
        logging.error(f"Ошибка cmd_start: {e}")
        await message.answer("❌ Произошла ошибка")

# =============================================================================
# МЕНЮ: ФЕРМА
# =============================================================================
@dp.message(F.text == "🌾 Ферма")
async def cmd_farm(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        plots = user.get('farm_plots', ["empty"] * 3)
        farm_level = user.get('farm_level', 1)
        balance = user.get('balance', 0)
        now = int(time.time())
        
        keyboard = InlineKeyboardBuilder()
        
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                keyboard.button(text=f"🟫 Грядка {i+1}", callback_data=f"plot_empty_{i}")
            else:
                crop = CROPS.get(plot.get('crop'))
                if crop:
                    planted = plot.get('planted_at', 0)
                    growth_time = plot.get('growth_time', crop['growth_time'])
                    ready_time = planted + growth_time
                    
                    if now >= ready_time:
                        keyboard.button(text=f"✅ {crop['name']} ({i+1})", callback_data=f"plot_ready_{i}")
                    else:
                        remaining = ready_time - now
                        h = remaining // 3600
                        m = (remaining % 3600) // 60
                        keyboard.button(text=f"⏳ {i+1}", callback_data=f"plot_growing_{i}")
        
        keyboard.button(text="🌱 Посадить", callback_data="farm_plant")
        keyboard.button(text="🚜 Улучшить", callback_data="farm_upgrade")
        keyboard.button(text="📊 Статистика", callback_data="farm_stats")
        keyboard.button(text="🔄 Обновить", callback_data="farm_refresh")
        keyboard.adjust(3)
        
        empty_plots = sum(1 for p in plots if p == "empty" or not p or not isinstance(p, dict))
        ready_plots = sum(1 for i, p in enumerate(plots) if isinstance(p, dict) and CROPS.get(p.get('crop')) and now >= p.get('planted_at', 0) + p.get('growth_time', CROPS.get(p.get('crop'))['growth_time']))
        
        upgrade_info = FARM_UPGRADES.get(farm_level + 1)
        upgrade_text = f"\n🔜 След. уровень: {upgrade_info['upgrade_cost']:,} 🍇" if upgrade_info else ""
        
        skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
        
        text = (
            f"{skin['emoji']} **Ферма** (ур. {farm_level}){upgrade_text}\n\n"
            f"💰 Баланс: {balance:,} 🍇\n\n"
            f"🟫 Пустых: {empty_plots}\n"
            f"✅ Готово: {ready_plots}\n"
            f"⏳ Растёт: {len(plots) - empty_plots - ready_plots}\n\n"
            f"💡 Нажми на грядку для действия!"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_farm: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# МЕНЮ: ДОМ
# =============================================================================
@dp.message(F.text == "🏠 Дом")
async def cmd_house(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        house_level = user.get('house_level', 1)
        house = HOUSES.get(house_level)
        balance = user.get('balance', 0)
        last_claim = user.get('last_passive_claim', 0)
        
        now = int(time.time())
        hours_passed = (now - last_claim) // 3600
        
        income_multiplier = await get_active_booster_effect(message.from_user.id, user, 'passive_income')
        base_income = house['passive_income'] * hours_passed
        maintenance = house['maintenance'] * hours_passed
        pending_income = max(0, int(base_income * income_multiplier) - maintenance)
        
        next_house = HOUSES.get(house_level + 1)
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="💰 Забрать доход", callback_data="house_claim")
        if next_house:
            keyboard.button(text=f"🔨 Улучшить ({next_house['price']:,} 🍇)", callback_data=f"house_upgrade_{next_house['level']}")
        keyboard.button(text="📊 Статистика", callback_data="house_stats")
        keyboard.adjust(2)
        
        skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
        
        text = (
            f"{skin['emoji']} **{house['name']}** (ур. {house_level})\n\n"
            f"💰 Доход: {house['passive_income']:,} 🍇/час\n"
            f"🔧 Содержание: {house['maintenance']:,} 🍇/час\n"
            f"📈 Чистый: {house['passive_income'] - house['maintenance']:,} 🍇/час\n\n"
            f"💵 Баланс: {balance:,} 🍇\n"
            f"🎁 Доступно: {pending_income:,} 🍇\n"
            f"⏱ Прошло: {hours_passed} ч\n"
        )
        
        if next_house:
            text += f"\n🔜 **{next_house['name']}**\n💰 {next_house['price']:,} 🍇"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_house: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# МЕНЮ: ПОДАРКИ
# =============================================================================
@dp.message(F.text == "🎁 Подарки")
async def cmd_gifts(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        balance = user.get('balance', 0)
        
        keyboard = InlineKeyboardBuilder()
        for item_id, item in GIFT_CATALOG.items():
            keyboard.button(text=f"{item['name']} - {item['price']} 🍇", callback_data=f"gift_{item_id}")
        keyboard.adjust(2)
        keyboard.button(text="📦 Инвентарь", callback_data="my_inventory")
        keyboard.button(text="📜 История", callback_data="gift_history")
        keyboard.adjust(2)
        
        skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
        
        text = (
            f"{skin['emoji']} **Магазин подарков**\n\n"
            f"💰 Баланс: {balance:,} 🍇\n\n"
            f"🎨 Выберите подарок для друга:"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_gifts: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# МЕНЮ: ПРОФИЛЬ
# =============================================================================
@dp.message(F.text == "👤 Профиль")
async def cmd_profile(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
        
        text = (
            f"{skin['emoji']} **{skin['name']}**\n\n"
            f"👤 {message.from_user.first_name}\n"
            f"💰 Баланс: {user.get('balance', 0):,} 🍇\n\n"
            f"🌾 Ферма: ур. {user.get('farm_level', 1)}\n"
            f"🏠 Дом: ур. {user.get('house_level', 1)}\n\n"
            f"🚜 Урожаев: {user.get('total_harvest', 0):,}\n"
            f"💵 Заработано: {user.get('total_earned', 0):,} 🍇\n\n"
            f"🎁 Отправлено: {user.get('gifts_sent', 0)}\n"
            f"🎁 Получено: {user.get('gifts_received', 0)}"
        )
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🎨 Скины", callback_data="skins_shop")
        keyboard.button(text="🔄 Сменить скин", callback_data="skins_change")
        keyboard.adjust(2)
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_profile: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# МЕНЮ: МАГАЗИН
# =============================================================================
@dp.message(F.text == "🏪 Магазин")
async def cmd_shop(message: Message):
    try:
        user = await add_user(message.from_user.id, username=message.from_user.username)
        if not user:
            await message.answer("❌ Ошибка")
            return
        
        balance = user.get('balance', 0)
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="⚡ Бустеры", callback_data="shop_boosters")
        keyboard.button(text="🎨 Скины", callback_data="skins_shop")
        keyboard.button(text="🔄 Авто-сбор", callback_data="shop_auto")
        keyboard.button(text="📈 Умножение", callback_data="shop_multiplier")
        keyboard.adjust(2)
        
        skin = SKINS.get(user.get('skin', 'default'), SKINS['default'])
        
        text = (
            f"{skin['emoji']} **Магазин улучшений**\n\n"
            f"💰 Баланс: {balance:,} 🍇\n\n"
            f"🔧 Выберите категорию:"
        )
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка cmd_shop: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# МЕНЮ: ПОМОЩЬ
# =============================================================================
@dp.message(F.text == "📚 Помощь")
async def cmd_help(message: Message):
    try:
        skin = SKINS.get('default', SKINS['default'])
        
        text = (
            f"{skin['emoji']} **Помощь и Обучение**\n\n"
            f"🌾 **ФЕРМА:**\n"
            f"• Нажми на пустую грядку для посадки\n"
            f"• Нажми на готовую грядку для сбора\n"
            f"• Улучшай ферму для новых грядок (макс. 15)\n\n"
            f"🏠 **ДОМ:**\n"
            f"• Забирай доход каждый день\n"
            f"• Улучшай дом для большего дохода\n"
            f"• Учитывай содержание дома\n\n"
            f"🎁 **ПОДАРКИ:**\n"
            f"• Покупай подарки в магазине\n"
            f"• Отправляй друзьям через инвентарь\n"
            f"• Получай подарки от других\n\n"
            f"⚡ **БУСТЕРЫ:**\n"
            f"• Активируются автоматически при покупке\n"
            f"• Ускоряют рост, увеличивают урожай\n"
            f"• Удваивают пассивный доход\n\n"
            f"🎨 **СКИНЫ:**\n"
            f"• Отображаются в профиле\n"
            f"• Меняй в разделе Профиль\n"
            f"• Покупай в магазине\n\n"
            f"💡 **СОВЕТЫ:**\n"
            f"• Заходи каждый день за бонусом\n"
            f"• Используй бустеры для максимальной прибыли\n"
            f"• Дари подарки активным игрокам\n\n"
            f"🎮 **Удачи на ферме!**"
        )
        
        await message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка cmd_help: {e}")
        await message.answer("❌ Ошибка")

# =============================================================================
# CALLBACK ОБРАБОТЧИКИ
# =============================================================================

@dp.callback_query(lambda c: c.data.startswith("plot_"))
async def callback_plot_action(callback: CallbackQuery):
    try:
        await callback.answer()
        parts = callback.data.split("_")
        action = parts[1]
        plot_index = int(parts[2])
        
        if action == "empty":
            keyboard = InlineKeyboardBuilder()
            for crop_id, crop in CROPS.items():
                keyboard.button(text=f"{crop['name']} - {crop['cost']} 🍇", callback_data=f"plant_{crop_id}_{plot_index}")
            keyboard.adjust(2)
            keyboard.button(text="◀️ Назад", callback_data="farm_back")
            
            await callback.message.answer(
                f"🌱 **Посадка на грядку {plot_index + 1}**\n\nВыберите культуру:",
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

@dp.callback_query(lambda c: c.data == "farm_back")
async def callback_farm_back(callback: CallbackQuery):
    try:
        await callback.answer()
        await callback.message.delete()
        await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_refresh")
async def callback_farm_refresh(callback: CallbackQuery):
    try:
        await callback.answer("🔄 Обновляю...")
        await callback.message.delete()
        await cmd_farm(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "farm_upgrade")
async def callback_farm_upgrade(callback: CallbackQuery):
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
    try:
        await callback.answer()
        user = await get_user(callback.from_user.id)
        if user:
            text = (
                f"📊 **Статистика фермы**\n\n"
                f"🌾 Уровень: {user.get('farm_level', 1)}\n"
                f"⭐ XP: {user.get('farm_xp', 0)}\n"
                f"🚜 Грядок: {len(user.get('farm_plots', []))}\n"
                f"🏆 Урожаев: {user.get('total_harvest', 0):,}"
            )
            await callback.message.answer(text)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("house_"))
async def callback_house(callback: CallbackQuery):
    try:
        await callback.answer()
        action = callback.data.replace("house_", "")
        
        if action == "claim":
            success, msg = await claim_passive_income(callback.from_user.id)
            await callback.message.answer(f"{'✅' if success else '⏳'} {msg}")
        elif action.startswith("upgrade_"):
            level = int(action.replace("upgrade_", ""))
            success, msg = await upgrade_house(callback.from_user.id, level)
            await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("gift_"))
async def callback_gift_buy(callback: CallbackQuery):
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
            await callback.message.answer(
                f"✅ {item['name']} куплен!\n\n"
                f"💬 Описание: {item['description']}\n\n"
                f"инвентарь - посмотреть"
            )
        else:
            await update_balance(callback.from_user.id, item['price'])
            await callback.message.answer("❌ Ошибка")
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("buy_booster_"))
async def callback_buy_booster(callback: CallbackQuery):
    try:
        await callback.answer()
        booster_id = callback.data.replace("buy_booster_", "")
        
        success, msg = await activate_booster(callback.from_user.id, booster_id)
        await callback.message.answer(msg)
        
        if success:
            await callback.message.delete()
            await cmd_shop(callback.message)
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("skins_"))
async def callback_skins(callback: CallbackQuery):
    try:
        await callback.answer()
        action = callback.data.replace("skins_", "")
        
        if action == "shop":
            keyboard = InlineKeyboardBuilder()
            for skin_id, skin in SKINS.items():
                keyboard.button(text=f"{skin['emoji']} {skin['name']} - {skin['price']} 🍇", callback_data=f"buy_skin_{skin_id}")
            keyboard.adjust(2)
            
            await callback.message.answer(
                f"🎨 **Магазин скинов**\n\n"
                f"Выберите скин:",
                reply_markup=keyboard.as_markup()
            )
        
        elif action.startswith("buy_"):
            skin_id = action.replace("buy_", "")
            success, msg = await buy_skin(callback.from_user.id, skin_id)
            await callback.message.answer(msg)
    
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data == "my_inventory")
async def callback_inventory(callback: CallbackQuery):
    try:
        await callback.answer()
        user = await get_user(callback.from_user.id)
        inventory = user.get('inventory', [])
        
        if not inventory:
            await callback.message.answer("📦 Инвентарь пуст")
            return
        
        text = "📦 **Ваш инвентарь**\n\n"
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
        text += "\n💡 Нажми для передачи"
        
        await callback.message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.callback_query(lambda c: c.data.startswith("transfer_"))
async def callback_transfer(callback: CallbackQuery):
    try:
        await callback.answer()
        item_id = callback.data.replace("transfer_", "")
        item = GIFT_CATALOG.get(item_id)
        
        await callback.message.answer(
            f"🎁 **Передача: {item['name']}**\n\n"
            f"Отправьте username получателя:\n"
            f"(без @, например: username123)"
        )
        
        await callback.message.answer(
            f"💡 **Инструкция:**\n"
            f"1. Скопируй username друга\n"
            f"2. Отправь его следующим сообщением\n"
            f"3. Подтверди передачу"
        )
    except Exception as e:
        logging.error(f"Ошибка: {e}")

@dp.message()
async def handle_transfer_username(message: Message):
    try:
        user_id = message.from_user.id
        username = message.text.strip().replace('@', '')
        
        if len(username) < 3:
            await message.answer("❌ Неверный username")
            return
        
        recipient = await get_user_by_username(username)
        
        if not recipient:
            await message.answer(f"❌ @{username} не найден!")
            return
        
        if recipient['user_id'] == user_id:
            await message.answer("❌ Нельзя передать самому себе!")
            return
        
        await message.answer(
            f"✅ **Подтверждение**\n\n"
            f"👤 Получатель: @{username}\n\n"
            f"Отправьте название подарка для передачи"
        )
    except Exception as e:
        logging.error(f"Ошибка: {e}")

# =============================================================================
# ЗАПУСК БОТА
# =============================================================================
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
