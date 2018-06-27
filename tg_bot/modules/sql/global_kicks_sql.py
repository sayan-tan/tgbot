import threading

from sqlalchemy import Column, UnicodeText, Integer, String, Boolean

from tg_bot.modules.sql import BASE, SESSION


class GloballyKickedUsers(BASE):
    __tablename__ = "gkicks"
    user_id = Column(Integer, primary_key=True)
    name = Column(UnicodeText, nullable=False)
    reason = Column(UnicodeText)

    def __init__(self, user_id, name, reason=None):
        self.user_id = user_id
        self.name = name
        self.reason = reason

    def __repr__(self):
        return "<GKicked User {} ({})>".format(self.name, self.user_id)

    def to_dict(self):
        return {"user_id": self.user_id,
                "name": self.name,
                "reason": self.reason}


class GkickSettings(BASE):
    __tablename__ = "gkick_settings"
    chat_id = Column(String(14), primary_key=True)
    setting = Column(Boolean, default=True, nullable=False)

    def __init__(self, chat_id, enabled):
        self.chat_id = str(chat_id)
        self.setting = enabled

    def __repr__(self):
        return "<Gkick setting {} ({})>".format(self.chat_id, self.setting)


GloballyKickedUsers.__table__.create(checkfirst=True)
GkickSettings.__table__.create(checkfirst=True)

GKICKED_USERS_LOCK = threading.RLock()
GKICK_SETTING_LOCK = threading.RLock()
GKICKED_LIST = set()
GKICKSTAT_LIST = set()


def gkick_user(user_id, name, reason=None):
    with GKICKED_USERS_LOCK:
        user = SESSION.query(GloballyKickedUsers).get(user_id)
        if not user:
            user = GloballyKickedUsers(user_id, name, reason)
        else:
            user.name = name
            user.reason = reason

        SESSION.merge(user)
        SESSION.commit()
        __load_gkicked_userid_list()


def update_gkick_reason(user_id, name, reason=None):
    with GKICKED_USERS_LOCK:
        user = SESSION.query(GloballyKickedUsers).get(user_id)
        if not user:
            return False
        user.name = name
        user.reason = reason

        SESSION.merge(user)
        SESSION.commit()
        return True


def ungkick_user(user_id):
    with GKICKED_USERS_LOCK:
        user = SESSION.query(GloballyKickedUsers).get(user_id)
        if user:
            SESSION.delete(user)

        SESSION.commit()
        __load_gkicked_userid_list()


def is_user_gkicked(user_id):
    return user_id in GKICKED_LIST


def get_gkicked_user(user_id):
    try:
        return SESSION.query(GloballyKickedUsers).get(user_id)
    finally:
        SESSION.close()


def get_gkick_list():
    try:
        return [x.to_dict() for x in SESSION.query(GloballyKickedUsers).all()]
    finally:
        SESSION.close()


def enable_gkicks(chat_id):
    with GKICK_SETTING_LOCK:
        chat = SESSION.query(GkickSettings).get(str(chat_id))
        if not chat:
            chat = GkickSettings(chat_id, True)

        chat.setting = True
        SESSION.add(chat)
        SESSION.commit()
        if str(chat_id) in GKICKSTAT_LIST:
            GKICKSTAT_LIST.remove(str(chat_id))


def disable_gkicks(chat_id):
    with GKICK_SETTING_LOCK:
        chat = SESSION.query(GkickSettings).get(str(chat_id))
        if not chat:
            chat = GkickSettings(chat_id, False)

        chat.setting = False
        SESSION.add(chat)
        SESSION.commit()
        GKICKSTAT_LIST.add(str(chat_id))


def does_chat_gkick(chat_id):
    return str(chat_id) not in GKICKSTAT_LIST


def num_gkicked_users():
    return len(GKICKED_LIST)


def __load_gkicked_userid_list():
    global GKICKED_LIST
    try:
        GKICKED_LIST = {x.user_id for x in SESSION.query(GloballyKickedUsers).all()}
    finally:
        SESSION.close()


def __load_gkick_stat_list():
    global GKICKSTAT_LIST
    try:
        GKICKSTAT_LIST = {x.chat_id for x in SESSION.query(GkickSettings).all() if not x.setting}
    finally:
        SESSION.close()


def migrate_chat(old_chat_id, new_chat_id):
    with GKICK_SETTING_LOCK:
        chat = SESSION.query(GkickSettings).get(str(old_chat_id))
        if chat:
            chat.chat_id = new_chat_id
            SESSION.add(chat)

        SESSION.commit()


# Create in memory userid to avoid disk access
__load_gkicked_userid_list()
__load_gkick_stat_list()
