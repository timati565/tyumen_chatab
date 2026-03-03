import os
from dotenv import load_dotenv


BOT_TOKEN = "7697607142:AAEyvq3AkIBkLnw6WueTVficJGTiu6B5Qek"
ADMIN_IDS = [7479394466, 5063450800, 1959719142]  


DB_NAME = "data/tyumenchat.db"
DEBUG = False


TYUMEN_DISTRICTS = [
    "🏛️ Центральный",
    "🏭 Калининский", 
    "🏘️ Ленинский",
    "🌳 Восточный",
    "🏞️ Мыс",
    "🏡 Тарманы",
    "🏕️ Комарово",
    "🌲 Гилевская роща",
    "🏘️ МЖК",
    "🏛️ Зарека",
    "🏭 Нефтяников",
    "🌿 Дружба"
]


if not BOT_TOKEN:
    raise ValueError("BOT_TOKEN не установлен!")

print(f"✅ Конфигурация загружена (прямая вставка)")
print(f"   Бот токен: {BOT_TOKEN[:10]}...")
print(f"   Админы: {ADMIN_IDS}")
