import os
from dotenv import load_dotenv

load_dotenv()

BOT_TOKEN = os.getenv('BOT_TOKEN')
ADMIN_IDS_STR = os.getenv('ADMIN_IDS', '')
ADMIN_IDS = [int(id.strip()) for id in ADMIN_IDS_STR.split(',') if id.strip()]
DB_NAME = os.getenv('DB_NAME', 'tyumenchat.db')
DEBUG = os.getenv('DEBUG', 'False').lower() == 'true'

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

if not ADMIN_IDS:
    raise ValueError("ADMIN_IDS не установлены!")