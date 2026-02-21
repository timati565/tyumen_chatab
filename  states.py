from aiogram.fsm.state import State, StatesGroup

class States(StatesGroup):
    waiting = State()
    chatting = State()
    changing_nick = State()
    changing_district = State()
    admin_broadcast = State()
    admin_get_user = State()
    admin_search_district = State()
    admin_search_messages = State()
    admin_view_chat = State()