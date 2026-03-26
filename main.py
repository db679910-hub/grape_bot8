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

HOUSES = {
    "tent": {"name": "⛺ Палатка", "level": 1, "price": 0, "passive_income": 1, "desc": "Простое жильё"},
    "hut": {"name": "🛖 Хижина", "level": 2, "price": 1000, "passive_income": 5, "desc": "Уютная хижина"},
    "cottage": {"name": "🏡 Коттедж", "level": 3, "price": 5000, "passive_income": 20, "desc": "Загородный дом"},
    "mansion": {"name": "🏰 Особняк", "level": 4, "price": 15000, "passive_income": 50, "desc": "Роскошный особняк"},
    "castle": {"name": "🏯 Замок", "level": 5, "price": 50000, "passive_income": 150, "desc": "Королевский замок"},
    "palace": {"name": "👑 Дворец", "level": 6, "price": 150000, "passive_income": 500, "desc": "Императорский дворец"},
}

BOOSTERS = {
    "speed_1h": {"name": "⚡ Ускорение 1ч", "price": 50, "duration": 3600, "effect": "growth_speed", "bonus": 1.5},
    "speed_4h": {"name": "⚡ Ускорение 4ч", "price": 150, "duration": 14400, "effect": "growth_speed", "bonus": 1.5},
    "speed_24h": {"name": "⚡ Ускорение 24ч", "price": 500, "duration": 86400, "effect": "growth_speed", "bonus": 1.5},
    "yield_1h": {"name": "📈 Урожай 1ч", "price": 75, "duration": 3600, "effect": "yield", "bonus": 1.3},
    "yield_4h": {"name": "📈 Урожай 4ч", "price": 200, "duration": 14400, "effect": "yield", "bonus": 1.3},
    "yield_24h": {"name": "📈 Урожай 24ч", "price": 600, "duration": 86400, "effect": "yield", "bonus": 1.3},
    "income_1h": {"name": "💰 Доход 1ч", "price": 120, "duration": 3600, "effect": "passive_income", "bonus": 2.0},
    "income_4h": {"name": "💰 Доход 4ч", "price": 350, "duration": 14400, "effect": "passive_income", "bonus": 2.0},
    "income_24h": {"name": "💰 Доход 24ч", "price": 1000, "duration": 86400, "effect": "passive_income", "bonus": 2.0},
    "super_booster": {"name": "🌟 СУПЕР БУСТЕР", "price": 2000, "duration": 43200, "effect": "all", "bonus": 1.5},
}

CROPS = {
    "grape": {"name": "🍇 Виноград", "cost": 0, "reward": 15, "growth_time": 14400, "xp": 10},
    "strawberry": {"name": "🍓 Клубника", "cost": 50, "reward": 80, "growth_time": 7200, "xp": 25},
    "corn": {"name": "🌽 Кукуруза", "cost": 100, "reward": 180, "growth_time": 10800, "xp": 40},
    "tomato": {"name": "🍅 Томат", "cost": 200, "reward": 400, "growth_time": 18000, "xp": 60},
    "pumpkin": {"name": "🎃 Тыква", "cost": 500, "reward": 1200, "growth_time": 28800, "xp": 100},
    "melon": {"name": "🍈 Дыня", "cost": 1000, "reward": 2500, "growth_time": 43200, "xp": 200},
    "diamond_grape": {"name": "💎 Алмазный виноград", "cost": 5000, "reward": 15000, "growth_time": 86400, "xp": 500},
}

FARM_PLOTS = {
    1: {"level": 1, "plots": 3, "upgrade_cost": 0},
    2: {"level": 2, "plots": 5, "upgrade_cost": 500},
    3: {"level": 3, "plots": 7, "upgrade_cost": 1500},
    4: {"level": 4, "plots": 10, "upgrade_cost": 3000},
    5: {"level": 5, "plots": 15, "upgrade_cost": 6000},
}

TOOLS = {
    "hoe": {"name": "🔓 Мотыга", "price": 200, "effect": "growth_speed", "bonus": 1.1},
    "watering_can": {"name": "🚿 Лейка", "price": 500, "effect": "growth_speed", "bonus": 1.2},
    "fertilizer": {"name": "💩 Удобрение", "price": 1000, "effect": "growth_speed", "bonus": 1.3},
    "tractor": {"name": "🚜 Трактор", "price": 3000, "effect": "growth_speed", "bonus": 1.5},
}

GIFT_MIN = 10
GIFT_MAX = 10000
GIFT_COMMISSION = 5

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
                    username VARCHAR(100),
                    farm_level INTEGER DEFAULT 1,
                    farm_xp INTEGER DEFAULT 0,
                    farm_plots TEXT DEFAULT '["empty", "empty", "empty"]',
                    tools TEXT DEFAULT '[]',
                    last_daily INTEGER DEFAULT 0,
                    house_level INTEGER DEFAULT 1,
                    house_xp INTEGER DEFAULT 0,
                    last_passive_claim INTEGER DEFAULT 0,
                    boosters TEXT DEFAULT '[]',
                    total_harvest INTEGER DEFAULT 0,
                    total_earned INTEGER DEFAULT 0
                )
            """)
            logging.info("✅ Таблица users готова!")
        
        logging.info("✅ База данных готова!")
    except Exception as e:
        logging.error(f"❌ Ошибка инициализации БД: {e}")
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
                return result
            return None
    except Exception as e:
        logging.error(f"❌ Ошибка get_user: {e}")
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
                "INSERT INTO users (user_id, ref_code, invited_by, username, farm_plots, tools, boosters) VALUES ($1, $2, $3, $4, $5, $6, $7) ON CONFLICT (user_id) DO NOTHING",
                user_id, my_ref_code, inviter_id, username.lower() if username else None, 
                '["empty", "empty", "empty"]', '[]', '[]'
            )
            
            if inviter_id:
                await conn.execute("UPDATE users SET balance = balance + $1, total_invited = total_invited + 1 WHERE user_id = $2", REFERRAL_BONUS, inviter_id)
                await conn.execute("UPDATE users SET balance = balance + $1 WHERE user_id = $2", REFERRAL_BONUS, user_id)
            
            return await get_user(user_id)
    except Exception as e:
        logging.error(f"❌ Ошибка add_user: {e}")
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
        elif item_id in ["skin_wine", "skin_diamond"]:
            skin = "wine" if item_id == "skin_wine" else "diamond"
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
            return False, "Пользователь не найден"
        
        plots = user.get('farm_plots', ["empty", "empty", "empty"])
        
        if plot_index < 0 or plot_index >= len(plots):
            return False, "Неверный номер грядки"
        
        crop = CROPS.get(crop_id)
        if not crop:
            return False, "Культура не найдена"
        
        if user['balance'] < crop['cost']:
            return False, f"Недостаточно винограда! Нужно {crop['cost']} 🍇"
        
        plots[plot_index] = {
            "crop": crop_id,
            "planted_at": int(time.time()),
            "ready": False
        }
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance - $1, farm_plots = $2 WHERE user_id = $3", 
                              crop['cost'], json.dumps(plots), user_id)
        
        return True, f"{crop['name']} посажен!"
    except Exception as e:
        logging.error(f"❌ Ошибка plant_crop: {e}")
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
        
        return True, f"Собрано: {reward} 🍇 (+{crop['xp']} XP)"
    except Exception as e:
        logging.error(f"❌ Ошибка harvest_crop: {e}")
        return False, "Ошибка сбора"

async def upgrade_farm_level(user_id):
    try:
        user = await get_user(user_id)
        if not user:
            return False, "Пользователь не найден"
        
        current_level = user.get('farm_level', 1)
        
        if current_level >= 5:
            return False, "Максимальный уровень фермы!"
        
        next_level = current_level + 1
        upgrade_info = FARM_PLOTS.get(next_level)
        
        if not upgrade_info:
            return False, "Ошибка улучшения"
        
        if user['balance'] < upgrade_info['upgrade_cost']:
            return False, f"Нужно {upgrade_info['upgrade_cost']} 🍇"
        
        new_plots = ["empty"] * upgrade_info['plots']
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance - $1, farm_level = $2, farm_plots = $3 WHERE user_id = $4",
                              upgrade_info['upgrade_cost'], next_level, json.dumps(new_plots), user_id)
        
        return True, f"Ферма улучшена до уровня {next_level}! Теперь {upgrade_info['plots']} грядок."
    except Exception as e:
        logging.error(f"❌ Ошибка upgrade_farm_level: {e}")
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
            return False, f"Нужно {house['price']} 🍇"
        
        async with pool.acquire() as conn:
            await conn.execute("UPDATE users SET balance = balance - $1, house_level = $2, house_xp = house_xp + $3 WHERE user_id = $4",
                              house['price'], house['level'], house['level'] * 100, user_id)
        
        return True, f"Дом улучшен до {house['name']}!"
    except Exception as e:
        logging.error(f"❌ Ошибка upgrade_house_level: {e}")
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
        
        return True, f"Получено: {total_income} 🍇 (за {hours_passed}ч)"
    except Exception as e:
        logging.error(f"❌ Ошибка claim_passive_income: {e}")
        return False, "Ошибка получения дохода"

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
    try:
        user_id = message.from_user.id
        username = message.from_user.username
        args = message.text.split()
        ref_code = args[1] if len(args) > 1 else None
        
        user = await add_user(user_id, ref_code, username)
        
        if user:
            await message.answer(f"🍇 Привет, {message.from_user.first_name}!\n\n📋 Команды:\n/ферма — моя ферма\n/дом — мой дом\n/бустеры — магазин\n/баланс — проверить\n/помощь — справка")
        else:
            await message.answer("❌ Ошибка регистрации")
    except Exception as e:
        logging.error(f"❌ Ошибка в /start: {e}")
        await message.answer("❌ Произошла ошибка.")

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
        
        plot_display = []
        now = int(time.time())
        speed_bonus = await get_booster_effect(user_id, 'growth_speed')
        
        for i, plot in enumerate(plots):
            if plot == "empty" or not plot or not isinstance(plot, dict):
                plot_display.append(f"{i+1}. 🟫 Пусто")
            else:
                crop = CROPS.get(plot.get('crop'))
                if crop:
                    planted = plot.get('planted_at', 0)
                    growth_time = int(crop['growth_time'] / speed_bonus)
                    ready_time = planted + growth_time
                    
                    if now >= ready_time:
                        status = "✅ Готово!"
                    else:
                        remaining = ready_time - now
                        hours = remaining // 3600
                        minutes = (remaining % 3600) // 60
                        status = f"⏳ {hours}ч {minutes}м"
                    
                    crop_emoji = crop['name'].split()[0]
                    plot_display.append(f"{i+1}. {crop_emoji} {crop['name']} — {status}")
                else:
                    plot_display.append(f"{i+1}. 🟫 Пусто")
        
        keyboard = InlineKeyboardBuilder()
        keyboard.button(text="🌱 Посадить", callback_data="farm_plant")
        keyboard.button(text="🚜 Улучшить", callback_data="farm_upgrade")
        keyboard.button(text="📊 Статистика", callback_data="farm_stats")
        keyboard.adjust(2)
        
        text = f"🌾 **Ваша ферма**\n\n"
        text += f"📊 Уровень: {farm_level}\n"
        text += f"✨ Опыт: {farm_xp}\n"
        text += f"🍇 Баланс: {balance}\n\n"
        text += f"**Грядки:**\n" + "\n".join(plot_display)
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"❌ Ошибка в /ферма: {e}")
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
        
        text = f"🏠 **Ваш дом**\n\n"
        text += f"{house['name']}\n"
        text += f"{house['desc']}\n\n"
        text += f"📊 Уровень: {house_level}\n"
        text += f"✨ Опыт: {house_xp}\n"
        text += f"💰 Доход: {house['passive_income']} 🍇/час\n"
        text += f"🍇 Баланс: {balance}\n\n"
        
        if pending_income > 0:
            text += f"🎁 Доступно: {pending_income} 🍇\n\n"
        
        if next_house:
            text += f"**Следующий:**\n{next_house['name']} — {next_house['price']} 🍇"
        
        await message.answer(text, reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"❌ Ошибка в /дом: {e}")
        await message.answer("❌ Ошибка дома.")

@dp.message(Command("бустеры"))
async def cmd_boosters(message: Message):
    try:
        user = await get_user(message.from_user.id)
        balance = user.get('balance', 0) if user else 0
        
        keyboard = InlineKeyboardBuilder()
        for booster_id, booster in BOOSTERS.items():
            keyboard.button(text=f"{booster['name']} — {booster['price']} 🍇", callback_data=f"buy_booster_{booster_id}")
        keyboard.adjust(2)
        
        await message.answer(f"🚀 **Бустеры**\n\nБаланс: {balance} 🍇", reply_markup=keyboard.as_markup())
    except Exception as e:
        logging.error(f"❌ Ошибка в /бустеры: {e}")

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
            await callback.answer("❌ Недостаточно 🍇", show_alert=True)
            return
        
        await update_balance(user_id, -booster['price'])
        success, msg = await add_booster(user_id, booster_id)
        
        await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_buy_booster: {e}")

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
            text = f"📊 **Статистика**\n\n"
            text += f"Дом: ур. {user.get('house_level', 1)}\n"
            text += f"Опыт: {user.get('house_xp', 0)}\n"
            text += f"Ферма: ур. {user.get('farm_level', 1)}\n"
            text += f"Урожаев: {user.get('total_harvest', 0)}"
            await callback.message.answer(text)
        
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_house: {e}")

@dp.callback_query(lambda c: c.data.startswith("farm_"))
async def callback_farm(callback: CallbackQuery):
    try:
        user_id = callback.from_user.id
        action = callback.data.replace("farm_", "")
        
        if action == "plant":
            keyboard = InlineKeyboardBuilder()
            for crop_id, crop in CROPS.items():
                keyboard.button(text=f"{crop['name']} — {crop['cost']} 🍇", callback_data=f"plant_select_{crop_id}")
            keyboard.adjust(2)
            await callback.message.answer("🌱 Выберите культуру:", reply_markup=keyboard.as_markup())
        elif action == "upgrade":
            success, msg = await upgrade_farm_level(user_id)
            await callback.message.answer(f"{'✅' if success else '❌'} {msg}")
        elif action == "stats":
            user = await get_user(user_id)
            text = f"📊 **Ферма**\n\n"
            text += f"Уровень: {user.get('farm_level', 1)}\n"
            text += f"Опыт: {user.get('farm_xp', 0)}\n"
            text += f"Грядок: {len(user.get('farm_plots', []))}"
            await callback.message.answer(text)
        elif action.startswith("plant_select_"):
            crop_id = action.replace("plant_select_", "")
            await callback.message.answer(f"🌱 Выбрано: {CROPS[crop_id]['name']}\n\nИспользуйте /посадить [номер] {crop_id}")
        
        await callback.answer()
    except Exception as e:
        logging.error(f"❌ Ошибка в callback_farm: {e}")

@dp.message(Command("посадить"))
async def cmd_plant(message: Message):
    try:
        args = message.text.split()
        if len(args) < 3:
            await message.answer("🌱 /посадить [грядка] [культура]\nПример: /посадить 1 grape")
            return
        
        plot_index = int(args[1]) - 1
        crop_id = args[2]
        
        if crop_id not in CROPS:
            await message.answer("❌ Культура не найдена!")
            return
        
        success, msg = await plant_crop(message.from_user.id, plot_index, crop_id)
        await message.answer(f"{'✅' if success else '❌'} {msg}")
    except Exception as e:
        logging.error(f"❌ Ошибка в /посадить: {e}")
        await message.answer("❌ Ошибка.")

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
        logging.error(f"❌ Ошибка в /собрать: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("баланс"))
async def cmd_balance(message: Message):
    try:
        user = await get_user(message.from_user.id)
        if not user:
            await message.answer("❌ Сначала /start")
            return
        
        text = f"💰 **Баланс**\n\n"
        text += f"🍇 Виноград: {user.get('balance', 0)}\n"
        text += f"🌾 Ферма: ур. {user.get('farm_level', 1)}\n"
        text += f"🏠 Дом: ур. {user.get('house_level', 1)}"
        await message.answer(text)
    except Exception as e:
        logging.error(f"❌ Ошибка в /баланс: {e}")
        await message.answer("❌ Ошибка.")

@dp.message(Command("помощь"))
async def cmd_help(message: Message):
    await message.answer("📚 **Справка**\n\n🌾 /ферма — ферма\n🏠 /дом — дом\n🚀 /бустеры — бустеры\n🌱 /посадить [грядка] [культура]\n🚜 /собрать [грядка]\n💰 /баланс — баланс\n❓ /помощь — справка")

@dp.message(Command("топ"))
async def cmd_top(message: Message):
    try:
        top = await get_top_users(10)
        if not top:
            await message.answer("📊 Пока нет игроков")
            return
        
        text = "🏆 **Топ**\n\n"
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

@dp.message(Command("статистика"))
async def cmd_stats(message: Message):
    try:
        total = await get_total_users()
        grapes = await get_total_grapes()
        await message.answer(f"📊 **Статистика**\n\n👥 Игроков: {total}\n🍇 Всего: {grapes or 0}")
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
