import sqlite3
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import Application, CommandHandler, MessageHandler, filters, CallbackQueryHandler, ContextTypes
import uuid
import logging
import os
import time
from datetime import datetime
from messages import get_text  # Импортируем функцию для получения текста

# Настройка логгера
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[
        logging.FileHandler('bot.log'),
        logging.StreamHandler()
    ]
)

logger = logging.getLogger(__name__)

# Конфигурация бота
BOT_TOKEN = "TOKEN"  # Замените на ваш токен
ADMIN_IDS = {1727085454, 8110533761}  # множество int
VALUTE = "TON"  # По умолчанию валюта - TON

# Хранение данных в памяти (кэш)
user_data = {}   # {user_id: {'wallet': str, 'balance': float, 'successful_deals': int, 'lang': 'ru'}}
deals = {}       # {deal_id: {'amount': float, 'description': str, 'seller_id': int, 'buyer_id': int, ...}}
admin_commands = {}  # {user_id: 'command'}

DB_NAME = 'bot_data.db'

def is_admin(user_id: int) -> bool:
    return int(user_id) in ADMIN_IDS

def now_ts() -> int:
    return int(time.time())

def dt(ts: int) -> str:
    return datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')

def init_db():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('''
        CREATE TABLE IF NOT EXISTS users (
            user_id INTEGER PRIMARY KEY,
            wallet TEXT,
            balance REAL,
            successful_deals INTEGER,
            lang TEXT
        )
    ''')

    cursor.execute("PRAGMA table_info(users)")
    columns = cursor.fetchall()
    column_names = [column[1] for column in columns]
    if 'lang' not in column_names:
        cursor.execute('ALTER TABLE users ADD COLUMN lang TEXT DEFAULT "ru"')

    # deals — расширенная схема со статусами/кодом/метаданными
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS deals (
            deal_id TEXT PRIMARY KEY,
            amount REAL,
            description TEXT,
            seller_id INTEGER,
            buyer_id INTEGER,
            status TEXT,               -- created / confirmed / paid / canceled
            code TEXT,                 -- приватный код операции (для покупателя)
            currency TEXT,             -- валюта операции
            created_at INTEGER,
            confirmed_at INTEGER,
            paid_at INTEGER,
            canceled_at INTEGER,
            seller_username TEXT,
            buyer_username TEXT
        )
    ''')

    # Миграции для новых столбцов, если старая таблица уже была
    def ensure_column(table, col, ddl):
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [c[1] for c in cursor.fetchall()]
        if col not in cols:
            cursor.execute(f"ALTER TABLE {table} ADD COLUMN {ddl}")

    ensure_column('deals', 'status', 'status TEXT')
    ensure_column('deals', 'code', 'code TEXT')
    ensure_column('deals', 'currency', 'currency TEXT')
    ensure_column('deals', 'created_at', 'created_at INTEGER')
    ensure_column('deals', 'confirmed_at', 'confirmed_at INTEGER')
    ensure_column('deals', 'paid_at', 'paid_at INTEGER')
    ensure_column('deals', 'canceled_at', 'canceled_at INTEGER')
    ensure_column('deals', 'seller_username', 'seller_username TEXT')
    ensure_column('deals', 'buyer_username', 'buyer_username TEXT')

    # Черный список
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS blacklist (
            user_id INTEGER PRIMARY KEY,
            reason TEXT,
            added_at INTEGER
        )
    ''')

    conn.commit()
    conn.close()

def load_data():
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()

    cursor.execute('SELECT user_id, wallet, balance, successful_deals, COALESCE(lang, "ru") FROM users')
    for user_id, wallet, balance, successful_deals, lang in cursor.fetchall():
        user_data[user_id] = {
            'wallet': wallet or '',
            'balance': balance or 0.0,
            'successful_deals': successful_deals or 0,
            'lang': lang or 'ru'
        }

    cursor.execute('SELECT deal_id, amount, description, seller_id, buyer_id, COALESCE(status,""), COALESCE(code,""), COALESCE(currency,""), COALESCE(created_at,0), COALESCE(confirmed_at,0), COALESCE(paid_at,0), COALESCE(canceled_at,0), COALESCE(seller_username,""), COALESCE(buyer_username,"") FROM deals')
    for row in cursor.fetchall():
        deal_id, amount, description, seller_id, buyer_id, status, code, currency, created_at, confirmed_at, paid_at, canceled_at, seller_username, buyer_username = row
        deals[deal_id] = {
            'amount': amount or 0.0,
            'description': description or '',
            'seller_id': seller_id,
            'buyer_id': buyer_id,
            'status': status or '',
            'code': code or '',
            'currency': currency or VALUTE,
            'created_at': created_at,
            'confirmed_at': confirmed_at,
            'paid_at': paid_at,
            'canceled_at': canceled_at,
            'seller_username': seller_username or '',
            'buyer_username': buyer_username or ''
        }

    conn.close()

def save_user_data(user_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    user = user_data.get(user_id, {})
    cursor.execute('''
        INSERT OR REPLACE INTO users (user_id, wallet, balance, successful_deals, lang)
        VALUES (?, ?, ?, ?, ?)
    ''', (user_id, user.get('wallet', ''), user.get('balance', 0.0), user.get('successful_deals', 0), user.get('lang', 'ru')))
    conn.commit()
    conn.close()

def save_deal(deal_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    d = deals.get(deal_id, {})
    cursor.execute('''
        INSERT OR REPLACE INTO deals (
            deal_id, amount, description, seller_id, buyer_id, status, code, currency,
            created_at, confirmed_at, paid_at, canceled_at, seller_username, buyer_username
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    ''', (
        deal_id, d.get('amount', 0.0), d.get('description', ''), d.get('seller_id'),
        d.get('buyer_id'), d.get('status', ''), d.get('code', ''), d.get('currency', VALUTE),
        d.get('created_at', 0), d.get('confirmed_at', 0), d.get('paid_at', 0), d.get('canceled_at', 0),
        d.get('seller_username', ''), d.get('buyer_username', '')
    ))
    conn.commit()
    conn.close()

def delete_deal(deal_id):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM deals WHERE deal_id = ?', (deal_id,))
    conn.commit()
    conn.close()

def ensure_user_exists(user_id):
    if user_id not in user_data:
        user_data[user_id] = {'wallet': '', 'balance': 0.0, 'successful_deals': 0, 'lang': 'ru'}
        save_user_data(user_id)

def user_in_blacklist(user_id: int) -> bool:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('SELECT 1 FROM blacklist WHERE user_id = ?', (user_id,))
    row = cursor.fetchone()
    conn.close()
    return row is not None

def blacklist_add(user_id: int, reason: str):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('INSERT OR REPLACE INTO blacklist (user_id, reason, added_at) VALUES (?, ?, ?)', (user_id, reason, now_ts()))
    conn.commit()
    conn.close()

def blacklist_remove(user_id: int):
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('DELETE FROM blacklist WHERE user_id = ?', (user_id,))
    conn.commit()
    conn.close()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        if update.message:
            user_id = update.message.from_user.id
            chat_id = update.message.chat_id
            args = context.args
        elif update.callback_query:
            user_id = update.callback_query.from_user.id
            chat_id = update.callback_query.message.chat_id
            args = []
        else:
            return

        if user_in_blacklist(user_id):
            await context.bot.send_message(chat_id, "Доступ запрещен. Обратитесь в поддержку.")
            return

        lang = user_data.get(user_id, {}).get('lang', 'ru')

        # Если /start <deal_id> — присоединение к сделке
        if args and args[0] in deals:
            deal_id = args[0]
            deal = deals[deal_id]
            seller_id = deal['seller_id']
            seller_username = deal.get('seller_username') or ((await context.bot.get_chat(seller_id)).username if seller_id else "unknown")

            deals[deal_id]['buyer_id'] = user_id
            deals[deal_id]['buyer_username'] = (await context.bot.get_chat(user_id)).username or ""
            if not deals[deal_id].get('status'):
                deals[deal_id]['status'] = 'created'
            save_deal(deal_id)

            await context.bot.send_message(
                chat_id,
                get_text(lang, "deal_info_message",
                         deal_id=deal_id,
                         seller_username=seller_username,
                         successful_deals=user_data.get(seller_id, {}).get('successful_deals', 0),
                         description=deal['description'],
                         wallet=user_data.get(seller_id, {}).get('wallet', 'Не указан'),
                         amount=deal['amount'],
                         valute=deals[deal_id].get('currency') or VALUTE),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "pay_from_balance_button"), callback_data=f'pay_from_balance_{deal_id}')],
                    [InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]
                ])
            )

            buyer_username = (await context.bot.get_chat(user_id)).username or "unknown"
            if seller_id:
                await context.bot.send_message(
                    seller_id,
                    get_text(lang, "seller_notification_message",
                             buyer_username=buyer_username,
                             deal_id=deal_id,
                             successful_deals=user_data.get(seller_id, {}).get('successful_deals', 0))
                )
            return

        if is_admin(user_id):
            keyboard = [
                [InlineKeyboardButton("📄 Реквизиты/заявки", callback_data='admin_requisites')],
                [InlineKeyboardButton("📜 История оплат", callback_data='admin_history')],
                [InlineKeyboardButton("📊 Статистика", callback_data='admin_stats')],
                [InlineKeyboardButton(get_text(lang, "admin_change_balance_button"), callback_data='admin_change_balance')],
                [InlineKeyboardButton(get_text(lang, "admin_change_successful_deals_button"), callback_data='admin_change_successful_deals')],
                [InlineKeyboardButton(get_text(lang, "admin_change_valute_button"), callback_data='admin_change_valute')],
                [InlineKeyboardButton("🚫 Бан/разбан", callback_data='admin_users')]
            ]
            await context.bot.send_message(chat_id, get_text(lang, "admin_panel_message"), reply_markup=InlineKeyboardMarkup(keyboard))
        else:
            keyboard = [
                [InlineKeyboardButton(get_text(lang, "add_wallet_button"), callback_data='wallet')],
                [InlineKeyboardButton(get_text(lang, "create_deal_button"), callback_data='create_deal')],
                [InlineKeyboardButton("💳 Купить (/buy)", callback_data='show_buy_help')],
                [InlineKeyboardButton(get_text(lang, "referral_button"), callback_data='referral')],
                [InlineKeyboardButton(get_text(lang, "change_lang_button"), callback_data='change_lang')],
                [InlineKeyboardButton(get_text(lang, "support_button"), url='https://t.me/otcgifttg/113382/113404')],
            ]
            await context.bot.send_photo(
                chat_id,
                photo="https://postimg.cc/8sHq27HV",
                caption=get_text(lang, "start_message"),
                reply_markup=InlineKeyboardMarkup(keyboard)
            )
    except Exception as e:
        logger.error(f"Ошибка в функции start: {e}")
        try:
            await context.bot.send_message(chat_id, "Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass

async def button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        query = update.callback_query
        await query.answer()
        data = query.data
        user_id = query.from_user.id
        chat_id = query.message.chat_id
        lang = user_data.get(user_id, {}).get('lang', 'ru')

        if user_in_blacklist(user_id) and not is_admin(user_id):
            await query.edit_message_text("Доступ запрещен.")
            return

        if data.startswith('lang_'):
            new_lang = data.split('_')[-1]
            ensure_user_exists(user_id)
            user_data[user_id]['lang'] = new_lang
            save_user_data(user_id)
            await query.edit_message_text(get_text(new_lang, "lang_set_message"))
            await start(update, context)
            return

        elif data == 'wallet':
            wallet = user_data.get(user_id, {}).get('wallet') or "Не указан"
            await context.bot.send_message(
                chat_id,
                get_text(lang, "wallet_message", wallet=wallet),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )
            context.user_data['awaiting_wallet'] = True

        elif data == 'create_deal':
            await context.bot.send_photo(
                chat_id,
                photo="https://postimg.cc/8sHq27HV",
                caption=get_text(lang, "create_deal_message", valute=VALUTE),
                parse_mode="MarkdownV2",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )
            context.user_data['awaiting_amount'] = True

        elif data == 'show_buy_help':
            await context.bot.send_message(
                chat_id,
                "Как оплатить:\n/buy <seller_id> <amount> <описание>\nПример: /buy 123456789 50 Подарок: книга",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )

        elif data == 'referral':
            referral_link = f"https://t.me/GltfEIfbot?start={user_id}"
            await context.bot.send_message(
                chat_id,
                get_text(lang, "referral_message", referral_link=referral_link, valute=VALUTE),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )

        elif data == 'change_lang':
            await context.bot.send_message(
                chat_id,
                get_text(lang, "change_lang_message"),
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton(get_text(lang, "english_lang_button"), callback_data='lang_en')],
                    [InlineKeyboardButton(get_text(lang, "russian_lang_button"), callback_data='lang_ru')]
                ])
            )

        elif data == 'menu':
            await start(update, context)

        # Админка
        elif data == 'admin_requisites' and is_admin(user_id):
            text = admin_list_requisites()
            await context.bot.send_message(chat_id, text or "Нет заявок.", disable_web_page_preview=True)

        elif data == 'admin_history' and is_admin(user_id):
            text = admin_history_text()
            await context.bot.send_message(chat_id, text or "История пуста.", disable_web_page_preview=True)

        elif data == 'admin_stats' and is_admin(user_id):
            text = admin_stats_text()
            await context.bot.send_message(chat_id, text, disable_web_page_preview=True)

        elif data == 'admin_users' and is_admin(user_id):
            await context.bot.send_message(
                chat_id,
                "Управление пользователями:\n/ban <user_id> <reason>\n/unban <user_id>",
                disable_web_page_preview=True
            )

        elif data == 'admin_change_balance' and is_admin(user_id):
            await query.edit_message_text(get_text(lang, "admin_change_balance_message"))
            admin_commands[user_id] = 'change_balance'

        elif data == 'admin_change_successful_deals' and is_admin(user_id):
            await query.edit_message_text(get_text(lang, "admin_change_successful_deals_message"))
            admin_commands[user_id] = 'change_successful_deals'

        elif data == 'admin_change_valute' and is_admin(user_id):
            await query.edit_message_text(get_text(lang, "admin_change_valute_message"))
            admin_commands[user_id] = 'change_valute'

        elif data.startswith('pay_from_balance_'):
            deal_id = data.split('_')[-1]
            deal = deals.get(deal_id)
            if not deal:
                await context.bot.send_message(chat_id, "Сделка не найдена.")
                return

            buyer_id = user_id
            seller_id = deal['seller_id']
            amount = float(deal['amount'])
            ensure_user_exists(buyer_id)
            ensure_user_exists(seller_id)

            if user_data[buyer_id]['balance'] >= amount:
                user_data[buyer_id]['balance'] -= amount
                save_user_data(buyer_id)

                user_data[seller_id]['balance'] += amount
                save_user_data(seller_id)

                # метаданные сделки
                deals[deal_id]['status'] = 'paid'
                deals[deal_id]['paid_at'] = now_ts()
                if not deals[deal_id].get('currency'):
                    deals[deal_id]['currency'] = VALUTE
                save_deal(deal_id)

                await context.bot.send_message(
                    chat_id,
                    get_text(lang, "payment_confirmed_message", deal_id=deal_id, amount=amount, valute=deals[deal_id]['currency'], description=deal['description']),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
                )
                await start(update, context)

                buyer_username = (await context.bot.get_chat(buyer_id)).username or "unknown"
                await context.bot.send_message(
                    seller_id,
                    get_text(lang, "payment_confirmed_seller_message",
                             deal_id=deal_id,
                             description=deal['description'],
                             buyer_username=buyer_username)
                )

                user_data[seller_id]['successful_deals'] += 1
                save_user_data(seller_id)

            else:
                await context.bot.send_message(
                    chat_id,
                    get_text(lang, "insufficient_balance_message"),
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
                )

    except Exception as e:
        logger.error(f"Ошибка в функции button: {e}")
        try:
            await context.bot.send_message(chat_id, "Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass

async def handle_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        global VALUTE
        user_id = update.message.from_user.id
        text = (update.message.text or "").strip()
        lang = user_data.get(user_id, {}).get('lang', 'ru')

        if user_in_blacklist(user_id) and not is_admin(user_id):
            await update.message.reply_text("Доступ запрещен.")
            return

        # Админские пошаговые команды
        if is_admin(user_id) and admin_commands.get(user_id) == 'change_balance':
            try:
                target_user_id_str, new_balance_str = text.split(maxsplit=2)[:2]
                target_user_id = int(target_user_id_str)
                new_balance = float(new_balance_str)
                ensure_user_exists(target_user_id)
                user_data[target_user_id]['balance'] = new_balance
                save_user_data(target_user_id)
                await update.message.reply_text(f"Баланс пользователя {target_user_id} изменен на {new_balance} {VALUTE}.")
            except Exception:
                await update.message.reply_text("Неверный формат. Введите: user_id баланс")
            admin_commands[user_id] = None
            return

        if is_admin(user_id) and admin_commands.get(user_id) == 'change_successful_deals':
            try:
                target_user_id_str, cnt_str = text.split(maxsplit=2)[:2]
                target_user_id = int(target_user_id_str)
                new_successful_deals = int(cnt_str)
                ensure_user_exists(target_user_id)
                user_data[target_user_id]['successful_deals'] = new_successful_deals
                save_user_data(target_user_id)
                await update.message.reply_text(f"Успешные сделки пользователя {target_user_id}: {new_successful_deals}.")
            except Exception:
                await update.message.reply_text("Неверный формат. Введите: user_id количество")
            admin_commands[user_id] = None
            return

        if is_admin(user_id) and admin_commands.get(user_id) == 'change_valute':
            VALUTE = text.upper()
            await update.message.reply_text(f"Валюта изменена на {VALUTE}.")
            admin_commands[user_id] = None
            return

        # Пошаговые формы
        if context.user_data.get('awaiting_amount', False):
            try:
                context.user_data['amount'] = float(text)
                context.user_data['awaiting_amount'] = False
                context.user_data['awaiting_description'] = True
                await update.message.reply_text(
                    get_text(lang, "awaiting_description_message"),
                    parse_mode="MarkdownV2",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
                )
            except ValueError:
                await update.message.reply_text("Неверный формат. Введите число.")
            return

        if context.user_data.get('awaiting_description', False):
            deal_id = str(uuid.uuid4())
            ensure_user_exists(user_id)
            seller_username = (update.message.from_user.username or "")
            deals[deal_id] = {
                'amount': float(context.user_data['amount']),
                'description': text,
                'seller_id': user_id,
                'buyer_id': None,
                'status': 'created',
                'code': '',  # код здесь не нужен — он будет в /buy
                'currency': VALUTE,
                'created_at': now_ts(),
                'confirmed_at': 0,
                'paid_at': 0,
                'canceled_at': 0,
                'seller_username': seller_username,
                'buyer_username': ''
            }
            save_deal(deal_id)
            context.user_data.clear()

            await update.message.reply_text(
                get_text(lang, "deal_created_message",
                         amount=deals[deal_id]['amount'],
                         valute=VALUTE,
                         description=deals[deal_id]['description'],
                         deal_link=f"https://t.me/GltfEIfbot?start={deal_id}"),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )

            for admin_id in ADMIN_IDS:
                try:
                    await context.bot.send_message(
                        admin_id,
                        f"Новая сделка создана:\n"
                        f"ID: {deal_id}\n"
                        f"Сумма: {deals[deal_id]['amount']} {VALUTE}\n"
                        f"Продавец: {deals[deal_id]['seller_id']} (@{seller_username})"
                    )
                except:
                    pass
            return

        if context.user_data.get('awaiting_wallet', False):
            ensure_user_exists(user_id)
            user_data[user_id]['wallet'] = text
            save_user_data(user_id)
            context.user_data.pop('awaiting_wallet', None)
            await update.message.reply_text(
                get_text(lang, "wallet_updated_message", wallet=text),
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton(get_text(lang, "menu_button"), callback_data='menu')]])
            )
            return

    except Exception as e:
        logger.error(f"Ошибка в функции handle_message: {e}")
        try:
            await update.message.reply_text("Произошла ошибка. Пожалуйста, попробуйте позже.")
        except:
            pass

# /buy <seller_id> <amount> <description...>
async def buy(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        if user_in_blacklist(user_id) and not is_admin(user_id):
            await update.message.reply_text("Доступ запрещен.")
            return

        args = context.args or []
        if len(args) < 2:
            await update.message.reply_text("Использование: /buy <seller_id> <amount> <описание>")
            return

        try:
            seller_id = int(args[0])
            amount = float(args[1])
        except ValueError:
            await update.message.reply_text("seller_id должен быть числом, amount — числом.")
            return

        description = " ".join(args[2:]).strip() or "Без описания"

        ensure_user_exists(user_id)
        ensure_user_exists(seller_id)

        deal_id = str(uuid.uuid4())
        private_code = str(uuid.uuid4())[:8].upper()  # короткий код для покупателя

        seller_username = (await context.bot.get_chat(seller_id)).username if seller_id else ""
        buyer_username = (update.message.from_user.username or "")

        deals[deal_id] = {
            'amount': amount,
            'description': description,
            'seller_id': seller_id,
            'buyer_id': user_id,
            'status': 'created',          # создано
            'code': private_code,        # приватный код
            'currency': VALUTE,
            'created_at': now_ts(),
            'confirmed_at': 0,
            'paid_at': 0,
            'canceled_at': 0,
            'seller_username': seller_username or "",
            'buyer_username': buyer_username or ""
        }
        save_deal(deal_id)

        # Покупателю — только его приватный код
        await update.message.reply_text(
            f"Заявка на оплату создана #{deal_id}\n"
            f"Кому (продавцу): @{seller_username or 'unknown'} (ID {seller_id})\n"
            f"Сумма: {amount} {VALUTE}\n"
            f"Описание: {description}\n"
            f"Ваш код к оплате: {private_code}\n\n"
            f"Перед оплатой проверьте реквизиты у продавца в ЛС. Код никому не показывайте."
        )

        # Продавцу — уведомление (без приватного кода)
        try:
            await context.bot.send_message(
                seller_id,
                f"Покупатель @{buyer_username or 'unknown'} создал заявку #{deal_id}\n"
                f"Сумма: {amount} {VALUTE}\n"
                f"Описание: {description}\n"
                f"Статус: создано"
            )
        except:
            pass

        # Админам — все реквизиты (включая код)
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(
                    admin_id,
                    f"Новая заявка на оплату:\n"
                    f"ID: {deal_id}\n"
                    f"Кто: @{buyer_username or 'unknown'} (ID {user_id})\n"
                    f"Кому: @{seller_username or 'unknown'} (ID {seller_id})\n"
                    f"Сумма: {amount} {VALUTE}\n"
                    f"Описание: {description}\n"
                    f"Код: {private_code}\n"
                    f"Статус: создано\n"
                    f"Создано: {dt(deals[deal_id]['created_at'])}"
                )
            except:
                pass

    except Exception as e:
        logger.error(f"Ошибка в /buy: {e}")
        await update.message.reply_text("Не удалось создать заявку. Попробуйте позже.")

# /confirm <deal_id> — подтверждение продавцом (тестовый флоу)
async def confirm(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text("Использование: /confirm <deal_id>")
            return
        deal_id = args[0]
        deal = deals.get(deal_id)
        if not deal:
            await update.message.reply_text("Сделка не найдена.")
            return
        if deal['seller_id'] != user_id and not is_admin(user_id):
            await update.message.reply_text("Только продавец или админ может подтверждать.")
            return
        if deal.get('status') in ('paid', 'canceled'):
            await update.message.reply_text(f"Сделка уже в статусе {deal['status']}.")
            return

        deals[deal_id]['status'] = 'confirmed'
        deals[deal_id]['confirmed_at'] = now_ts()
        save_deal(deal_id)

        await update.message.reply_text(f"Сделка #{deal_id} подтверждена продавцом.")
        buyer_id = deal.get('buyer_id')
        if buyer_id:
            try:
                await context.bot.send_message(buyer_id, f"Ваша сделка #{deal_id} подтверждена продавцом. Можете оплачивать.")
            except:
                pass
    except Exception as e:
        logger.error(f"Ошибка в /confirm: {e}")
        await update.message.reply_text("Ошибка подтверждения.")

# /paid <deal_id> <code> — пометить оплачено (псевдо-проверка по коду)
async def paid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        args = context.args or []
        if len(args) != 2:
            await update.message.reply_text("Использование: /paid <deal_id> <code>")
            return
        deal_id, code = args
        deal = deals.get(deal_id)
        if not deal:
            await update.message.reply_text("Сделка не найдена.")
            return
        if deal.get('buyer_id') != user_id and not is_admin(user_id):
            await update.message.reply_text("Только покупатель или админ может завершить оплату.")
            return
        if deal.get('status') in ('paid', 'canceled'):
            await update.message.reply_text(f"Сделка уже в статусе {deal['status']}.")
            return
        if code.strip().upper() != (deal.get('code') or '').upper():
            await update.message.reply_text("Неверный код.")
            return

        deals[deal_id]['status'] = 'paid'
        deals[deal_id]['paid_at'] = now_ts()
        save_deal(deal_id)

        # Успешные сделки продавцу
        seller_id = deal.get('seller_id')
        if seller_id:
            ensure_user_exists(seller_id)
            user_data[seller_id]['successful_deals'] += 1
            save_user_data(seller_id)

        await update.message.reply_text(f"Оплата по сделке #{deal_id} подтверждена.")
        # уведомления
        if seller_id:
            try:
                await context.bot.send_message(seller_id, f"Сделка #{deal_id} оплачена.")
            except:
                pass
        for admin_id in ADMIN_IDS:
            try:
                await context.bot.send_message(admin_id, f"Сделка #{deal_id} оплачена. Сумма: {deal['amount']} {deal.get('currency') or VALUTE}")
            except:
                pass

    except Exception as e:
        logger.error(f"Ошибка в /paid: {e}")
        await update.message.reply_text("Ошибка подтверждения оплаты.")

# /cancel <deal_id> — отмена покупателем/продавцом/админом
async def cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text("Использование: /cancel <deal_id>")
            return
        deal_id = args[0]
        deal = deals.get(deal_id)
        if not deal:
            await update.message.reply_text("Сделка не найдена.")
            return
        if user_id not in {deal.get('buyer_id'), deal.get('seller_id')} and not is_admin(user_id):
            await update.message.reply_text("Нет прав отменять эту сделку.")
            return
        if deal.get('status') in ('paid', 'canceled'):
            await update.message.reply_text(f"Сделка уже в статусе {deal['status']}.")
            return

        deals[deal_id]['status'] = 'canceled'
        deals[deal_id]['canceled_at'] = now_ts()
        save_deal(deal_id)
        await update.message.reply_text(f"Сделка #{deal_id} отменена.")
    except Exception as e:
        logger.error(f"Ошибка в /cancel: {e}")
        await update.message.reply_text("Ошибка отмены.")

# /history [from=YYYY-MM-DD] [to=YYYY-MM-DD] [user_id]
async def history_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        if not is_admin(user_id):
            await update.message.reply_text("Доступно только админам.")
            return

        args = context.args or []
        frm, to, uid = None, None, None
        if len(args) >= 1:
            frm = args[0]
        if len(args) >= 2:
            to = args[1]
        if len(args) >= 3:
            try:
                uid = int(args[2])
            except:
                uid = None

        text = admin_history_text(frm, to, uid)
        await update.message.reply_text(text or "История пуста.", disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Ошибка в /history: {e}")
        await update.message.reply_text("Ошибка запроса истории.")

# /stats [from=YYYY-MM-DD] [to=YYYY-MM-DD] [user_id]
async def stats_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        if not is_admin(user_id):
            await update.message.reply_text("Доступно только админам.")
            return
        args = context.args or []
        frm, to, uid = None, None, None
        if len(args) >= 1:
            frm = args[0]
        if len(args) >= 2:
            to = args[1]
        if len(args) >= 3:
            try:
                uid = int(args[2])
            except:
                uid = None
        await update.message.reply_text(admin_stats_text(frm, to, uid), disable_web_page_preview=True)
    except Exception as e:
        logger.error(f"Ошибка в /stats: {e}")
        await update.message.reply_text("Ошибка запроса статистики.")

# /ban <user_id> <reason...>
async def ban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        if not is_admin(user_id):
            await update.message.reply_text("Доступно только админам.")
            return
        args = context.args or []
        if len(args) < 1:
            await update.message.reply_text("Использование: /ban <user_id> <reason>")
            return
        target_id = int(args[0])
        reason = " ".join(args[1:]).strip() or "no reason"
        blacklist_add(target_id, reason)
        await update.message.reply_text(f"Пользователь {target_id} добавлен в ЧС. Причина: {reason}")
    except Exception as e:
        logger.error(f"Ошибка в /ban: {e}")
        await update.message.reply_text("Ошибка /ban.")

# /unban <user_id>
async def unban_cmd(update: Update, context: ContextTypes.DEFAULT_TYPE):
    try:
        user_id = update.message.from_user.id
        if not is_admin(user_id):
            await update.message.reply_text("Доступно только админам.")
            return
        args = context.args or []
        if len(args) != 1:
            await update.message.reply_text("Использование: /unban <user_id>")
            return
        target_id = int(args[0])
        blacklist_remove(target_id)
        await update.message.reply_text(f"Пользователь {target_id} удален из ЧС.")
    except Exception as e:
        logger.error(f"Ошибка в /unban: {e}")
        await update.message.reply_text("Ошибка /unban.")

# Вспомогательные отчётные функции для админки
def admin_list_requisites(limit: int = 20) -> str:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    cursor.execute('''
        SELECT deal_id, amount, description, seller_id, buyer_id, status, code, currency, created_at, seller_username, buyer_username
        FROM deals
        ORDER BY created_at DESC
        LIMIT ?
    ''', (limit,))
    rows = cursor.fetchall()
    conn.close()
    if not rows:
        return ""
    lines = []
    for deal_id, amount, desc, seller_id, buyer_id, status, code, curr, created_at, s_un, b_un in rows:
        lines.append(
            f"#{deal_id} | {amount} {curr or VALUTE} | {status or ''}\n"
            f"Кто: @{(b_un or 'unknown')} (ID {buyer_id})\n"
            f"Кому: @{(s_un or 'unknown')} (ID {seller_id})\n"
            f"Описание: {desc}\n"
            f"Код: {code or '-'}\n"
            f"Создано: {dt(created_at or 0)}\n"
        )
    return "Последние заявки:\n\n" + "\n".join(lines)

def admin_history_text(frm: str = None, to: str = None, user_id: int = None) -> str:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    where = ["status = 'paid'"]
    params = []
    if frm:
        where.append("date(paid_at, 'unixepoch') >= date(?)")
        params.append(frm)
    if to:
        where.append("date(paid_at, 'unixepoch') <= date(?)")
        params.append(to)
    if user_id:
        where.append("(buyer_id = ? OR seller_id = ?)")
        params.extend([user_id, user_id])
    where_sql = " AND ".join(where)
    cursor.execute(f'''
        SELECT deal_id, amount, currency, buyer_id, seller_id, description, paid_at, buyer_username, seller_username
        FROM deals
        WHERE {where_sql}
        ORDER BY paid_at DESC
        LIMIT 100
    ''', params)
    rows = cursor.fetchall()
    conn.close()
    if not rows:
        return ""
    lines = []
    total = 0.0
    for deal_id, amount, curr, buyer_id, seller_id, desc, paid_at, b_un, s_un in rows:
        total += float(amount or 0)
        lines.append(
            f"#{deal_id} | {amount} {curr or VALUTE} | {dt(paid_at or 0)}\n"
            f"Покупатель: @{b_un or 'unknown'} (ID {buyer_id})  →  Продавец: @{s_un or 'unknown'} (ID {seller_id})\n"
            f"{desc}\n"
        )
    lines.append(f"\nИтого завершённых сделок: {len(rows)} на сумму: {round(total, 4)} {VALUTE}")
    return "\n".join(lines)

def admin_stats_text(frm: str = None, to: str = None, user_id: int = None) -> str:
    conn = sqlite3.connect(DB_NAME)
    cursor = conn.cursor()
    where = ["status = 'paid'"]
    params = []
    if frm:
        where.append("date(paid_at, 'unixepoch') >= date(?)")
        params.append(frm)
    if to:
        where.append("date(paid_at, 'unixepoch') <= date(?)")
        params.append(to)
    if user_id:
        where.append("(buyer_id = ? OR seller_id = ?)")
        params.extend([user_id, user_id])
    where_sql = " AND ".join(where)
    cursor.execute(f'''
        SELECT COUNT(1), COALESCE(SUM(amount),0)
        FROM deals WHERE {where_sql}
    ''', params)
    cnt, total = cursor.fetchone()
    conn.close()
    return f"Статистика:\nСделок: {cnt}\nОборот: {round(total or 0, 4)} {VALUTE}"

def main() -> None:
    init_db()
    load_data()

    application = Application.builder().token(BOT_TOKEN).build()

    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("buy", buy))
    application.add_handler(CommandHandler("confirm", confirm))
    application.add_handler(CommandHandler("paid", paid))
    application.add_handler(CommandHandler("cancel", cancel))
    application.add_handler(CommandHandler("history", history_cmd))
    application.add_handler(CommandHandler("stats", stats_cmd))
    application.add_handler(CommandHandler("ban", ban_cmd))
    application.add_handler(CommandHandler("unban", unban_cmd))

    application.add_handler(CallbackQueryHandler(button))
    application.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_message))

    application.run_polling()

if __name__ == "__main__":
    main()
