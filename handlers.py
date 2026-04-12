"""
handlers.py — Спрощена версія: тільки /start, адмін панель та системні сповіщення.
Весь функціонал (канали, черга, налаштування, оплата) перенесено в Mini App.
"""

import asyncio
import json
import logging
from typing import Optional

from aiogram import Bot, Router, F
from aiogram.enums import ParseMode
from aiogram.exceptions import TelegramBadRequest
from aiogram.filters import CommandStart, Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.types import (
    Message, CallbackQuery,
    InlineKeyboardMarkup, InlineKeyboardButton,
    InputMediaPhoto, InputMediaVideo,
)

from services import (
    _safe_caption, _safe_text,
    get_session_status, cleanup_published_media, mark_confirm_sent,
    load_media_for_post,
    ADMIN_IDS, BOT_USERNAME, BOT_OWNER_USERNAME,
    REFERRAL_FIRST_PERCENT, REFERRAL_REPEAT_PERCENT, BASE_PRICE, MANAGER_USERNAME,
    get_or_create_user, get_user, get_all_users,
    get_user_channels, get_channel,
    get_channel_settings, get_all_channels,
    get_pending_posts, update_post_status,
    sanitize_html_for_telegram, build_footer,
    create_invoice, check_invoice_status, handle_payment_webhook,
    get_or_create_referral_code, get_referral_code_owner,
    register_referral, get_referral_stats,
    get_admin_stats,
    get_user_balance, adjust_user_balance,
    get_pending_withdrawals, process_withdrawal,
    log_event,
    parse_channel_sources,
    save_last_published,
    get_source, set_pattern_verified, send_pattern_verification_request,
    DB_PATH, _fetchone,
)

log = logging.getLogger(__name__)
router = Router()


# ─── TRANSLATIONS (тільки потрібні ключі) ────────────────────────────────────

_T = {
    "uk": {
        "start": (
            "👋 Привіт, <b>{name}</b>!\n\n"
            "🤖 <b>АвтопостингТГК</b> — бот для автоматичного постингу в Telegram-канали.\n\n"
            "📱 Весь функціонал доступний через Mini App:"
        ),
        "adm_panel": "🔧 <b>Адмін панель</b>",
        "adm_stats": "📊 Статистика",
        "adm_users": "👥 Користувачі",
        "adm_finance": "💰 Фінанси",
        "adm_logs": "📋 Логи",
        "adm_broadcast": "📢 Розсилка",
        "adm_ch_list": "📢 Всі канали",
        "adm_activity": "📈 Активність",
        "adm_sessions": "🔌 Сесії",
        "adm_back": "🔙 Назад",
        "adm_bal_notify": "💰 <b>Ваш баланс змінено адміністратором</b>\n\nЗміна: {sign}${delta:.2f}\nНовий баланс: ${new:.2f}",
        "ap_published_ok": "✅ <b>Пост опубліковано!</b>",
        "ap_next_post_at": "⏰ Наступний пост: {time}",
        "pub_blocked_status": "⛔ Підписка неактивна ({status}). Поповніть баланс у Mini App.",
        "pub_post_not_found": "⚠️ Пост не знайдено.",
        "pub_error": "❌ Помилка публікації: {err}",
        "ap_skip_inactive": "⛔ Підписка неактивна",
        "ap_skipped": "⏭ Пост пропущено.",
        "media_photo": "📸 Фото",
        "media_video": "🎬 Відео",
        "media_album": "📷 Альбом",
        "ap_confirm_pub": "📋 <b>Підтвердіть публікацію</b>\n\n",
        "ap_confirm_yes": "✅ Опублікувати",
        "btn_skip": "⏭ Пропустити",
        "ref_new_joined_user": "🎉 За вашим реферальним посиланням зареєструвався <b>{name}</b>!",
        "ref_new_joined_admin": "🔗 Новий реферал!\n👤 {referred_name} (tg:{referred_tg})\n← від {referrer_name} (tg:{referrer_tg})",
        "ref_bonus_notify": "💸 <b>Реферальний бонус!</b>\n\nКористувач {user_name} оплатив підписку.\nВаш бонус: +${bonus:.2f}",
        "payment_received": "✅ <b>Оплата отримана!</b>\n\nКанал: {ch_title}\nСума: ${amount:.2f}\nАктивно: {days} днів",
        "payment_admin_notif": "💳 Оплата!\nКористувач: {user_tg} ({user_name})\nКанал: {ch_link}\nСума: ${amount:.2f}\nДнів: {days}",
    },
    "ru": {
        "start": (
            "👋 Привет, <b>{name}</b>!\n\n"
            "🤖 <b>АвтопостингТКГ</b> — бот для автоматического постинга в Telegram-каналы.\n\n"
            "📱 Весь функционал доступен через Mini App:"
        ),
        "adm_panel": "🔧 <b>Админ панель</b>",
        "adm_stats": "📊 Статистика",
        "adm_users": "👥 Пользователи",
        "adm_finance": "💰 Финансы",
        "adm_logs": "📋 Логи",
        "adm_broadcast": "📢 Рассылка",
        "adm_ch_list": "📢 Все каналы",
        "adm_activity": "📈 Активность",
        "adm_sessions": "🔌 Сессии",
        "adm_back": "🔙 Назад",
        "adm_bal_notify": "💰 <b>Ваш баланс изменён администратором</b>\n\nИзменение: {sign}${delta:.2f}\nНовый баланс: ${new:.2f}",
        "ap_published_ok": "✅ <b>Пост опубликован!</b>",
        "ap_next_post_at": "⏰ Следующий пост: {time}",
        "pub_blocked_status": "⛔ Подписка неактивна ({status}). Пополните баланс в Mini App.",
        "pub_post_not_found": "⚠️ Пост не найден.",
        "pub_error": "❌ Ошибка публикации: {err}",
        "ap_skip_inactive": "⛔ Подписка неактивна",
        "ap_skipped": "⏭ Пост пропущен.",
        "media_photo": "📸 Фото",
        "media_video": "🎬 Видео",
        "media_album": "📷 Альбом",
        "ap_confirm_pub": "📋 <b>Подтвердите публикацию</b>\n\n",
        "ap_confirm_yes": "✅ Опубликовать",
        "btn_skip": "⏭ Пропустить",
        "ref_new_joined_user": "🎉 По вашей реферальной ссылке зарегистрировался <b>{name}</b>!",
        "ref_new_joined_admin": "🔗 Новый реферал!\n👤 {referred_name} (tg:{referred_tg})\n← от {referrer_name} (tg:{referrer_tg})",
        "ref_bonus_notify": "💸 <b>Реферальный бонус!</b>\n\nПользователь {user_name} оплатил подписку.\nВаш бонус: +${bonus:.2f}",
        "payment_received": "✅ <b>Оплата получена!</b>\n\nКанал: {ch_title}\nСумма: ${amount:.2f}\nАктивно: {days} дней",
        "payment_admin_notif": "💳 Оплата!\nПользователь: {user_tg} ({user_name})\nКанал: {ch_link}\nСумма: ${amount:.2f}\nДней: {days}",
    },
}


def uts(lang: str, key: str, **kwargs) -> str:
    d = _T.get(lang, _T["uk"])
    s = d.get(key, _T["uk"].get(key, key))
    return s.format(**kwargs) if kwargs else s


async def _ul(user_id: int) -> str:
    try:
        u = await get_user(user_id)
        return (u.get("language") or "ru") if u else "ru"
    except Exception:
        return "ru"


# ─── FSM States (мінімум — тільки для адміна) ────────────────────────────────

class AdminStates(StatesGroup):
    broadcast   = State()
    adj_amount  = State()
    set_sub_days = State()


class PatternStates(StatesGroup):
    """FSM for pattern verification correction flow."""
    waiting_correction = State()


# ─── KEYBOARDS ────────────────────────────────────────────────────────────────

def open_app_kb(bot_username: str, lang: str = "uk") -> InlineKeyboardMarkup:
    label = "📱 Відкрити Mini App" if lang != "ru" else "📱 Открыть Mini App"
    return InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(text=label,
                             web_app={"url": f"https://{bot_username.lower().replace('_bot','')}.f.jrnm.app/miniapp"})
    ]])


def admin_kb(lang: str = "uk") -> InlineKeyboardMarkup:
    t = lambda k: uts(lang, k)
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=t("adm_users"),    callback_data="adm_users"),
         InlineKeyboardButton(text=t("adm_stats"),    callback_data="adm_stats")],
        [InlineKeyboardButton(text=t("adm_finance"),  callback_data="adm_finance"),
         InlineKeyboardButton(text=t("adm_logs"),     callback_data="adm_logs")],
        [InlineKeyboardButton(text=t("adm_broadcast"), callback_data="adm_broadcast")],
        [InlineKeyboardButton(text=t("adm_ch_list"),  callback_data="adm_ch_list")],
        [InlineKeyboardButton(text=t("adm_activity"), callback_data="adm_activity")],
        [InlineKeyboardButton(text=t("adm_sessions"), callback_data="adm_sessions")],
    ])


def admin_channel_status_kb(ch_id: int, lang: str = "ru") -> InlineKeyboardMarkup:
    if lang == "uk":
        statuses = [
            ("🟢 Активна підписка", "active"),
            ("🟡 Пробний період",   "trial"),
            ("🔴 Прострочена",      "restricted"),
            ("⚫ Заблокований",     "blocked"),
        ]
    else:
        statuses = [
            ("🟢 Активная подписка", "active"),
            ("🟡 Пробный период",    "trial"),
            ("🔴 Просроченная",      "restricted"),
            ("⚫ Заблокирован",      "blocked"),
        ]
    rows = [[InlineKeyboardButton(text=label, callback_data=f"adm_set_status:{ch_id}:{s}")]
            for label, s in statuses]
    rows.append([InlineKeyboardButton(text=uts(lang, "adm_back"), callback_data="adm_ch_list")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


# ─── HELPERS ──────────────────────────────────────────────────────────────────

async def safe_edit(cq: CallbackQuery, text: str, reply_markup=None, parse_mode=ParseMode.HTML):
    try:
        await cq.message.edit_text(text, reply_markup=reply_markup, parse_mode=parse_mode)
    except TelegramBadRequest:
        pass


async def _send_payment_notifications(bot: Bot, buyer_tg_id: int, ch_id: int, pay_result: dict):
    """Надіслати сповіщення про оплату користувачу, реферу та адмінам."""
    if not pay_result:
        return
    amount  = pay_result.get("amount", 0)
    days    = pay_result.get("days", 30)
    ref_info = pay_result.get("ref_info")

    ch = await get_channel(ch_id)
    ch_title = ch["title"] if ch else f"Канал {ch_id}"
    ch_username = ch.get("username", "") if ch else ""
    ch_link = f"@{ch_username}" if ch_username else ch_title

    lang = await _ul(buyer_tg_id)

    # Notify buyer
    try:
        await bot.send_message(
            buyer_tg_id,
            uts(lang, "payment_received").format(ch_title=ch_title, amount=amount, days=days),
            parse_mode=ParseMode.HTML
        )
    except Exception:
        pass

    # Notify admins
    buyer = await get_user(buyer_tg_id)
    buyer_name = (buyer.get("username") or buyer.get("first_name") or str(buyer_tg_id)) if buyer else str(buyer_tg_id)
    for adm_id in ADMIN_IDS:
        try:
            await bot.send_message(
                adm_id,
                uts("uk", "payment_admin_notif").format(
                    user_tg=buyer_tg_id, user_name=buyer_name,
                    ch_link=ch_link, amount=amount, days=days
                ),
                parse_mode=ParseMode.HTML
            )
        except Exception:
            pass

    # Referral bonus notification
    if ref_info:
        ref_tg = ref_info.get("referrer_tg_id")
        bonus  = ref_info.get("bonus", 0)
        referred_name = ref_info.get("referred_name", "")
        if ref_tg and bonus > 0:
            ref_lang = await _ul(ref_tg)
            try:
                await bot.send_message(
                    ref_tg,
                    uts(ref_lang, "ref_bonus_notify").format(user_name=referred_name, bonus=bonus),
                    parse_mode=ParseMode.HTML
                )
            except Exception:
                pass
            for adm_id in ADMIN_IDS:
                try:
                    await bot.send_message(
                        adm_id,
                        f"💸 Реферальний бонус!\n"
                        f"Реферер: tg:{ref_tg} ({ref_info.get('referrer_name','')})\n"
                        f"Рефер: {referred_name}\nБонус: +${bonus:.2f}",
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass


def calc_next_autopost_time(settings: dict, lang: str = "uk") -> str:
    """Повертає рядок з часом наступного авто-посту."""
    try:
        from datetime import datetime, timezone, timedelta
        import os
        tz_offset = int(os.getenv("TIMEZONE_OFFSET", "0"))
        now = datetime.now(timezone(timedelta(hours=tz_offset)))
        ppd = int(settings.get("posts_per_day") or 1)
        interval_min = max(1, 1440 // ppd)
        next_dt = now + timedelta(minutes=interval_min)
        return next_dt.strftime("%H:%M")
    except Exception:
        return "—"


# ─── /start ───────────────────────────────────────────────────────────────────

@router.message(CommandStart())
async def cmd_start(msg: Message, state: FSMContext):
    await state.clear()
    tg = msg.from_user
    user = await get_or_create_user(tg.id, tg.username, tg.first_name, tg.last_name)

    # Referral handling
    args = msg.text.split()
    if len(args) > 1 and args[1].startswith("ref_"):
        code = args[1][4:]
        owner = await get_referral_code_owner(code)
        if owner and owner["telegram_id"] != tg.id:
            ref_result = await register_referral(owner["telegram_id"], tg.id)
            if ref_result:
                ref_lang = await _ul(ref_result["referrer_tg_id"])
                try:
                    await msg.bot.send_message(
                        ref_result["referrer_tg_id"],
                        uts(ref_lang, "ref_new_joined_user").format(name=ref_result["referred_name"]),
                        parse_mode=ParseMode.HTML
                    )
                except Exception:
                    pass
                for adm_id in ADMIN_IDS:
                    try:
                        await msg.bot.send_message(
                            adm_id,
                            uts("uk", "ref_new_joined_admin").format(
                                referred_tg=ref_result["referred_tg_id"],
                                referred_name=ref_result["referred_name"],
                                referrer_tg=ref_result["referrer_tg_id"],
                                referrer_name=ref_result["referrer_name"],
                            ),
                            parse_mode=ParseMode.HTML
                        )
                    except Exception:
                        pass

    lang = (user.get("language") or "uk")
    name = tg.first_name or tg.username or "друже"
    miniapp_url = "https://autopost.f.jrnm.app/miniapp"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text="📱 Відкрити Mini App" if lang != "ru" else "📱 Открыть Mini App",
            web_app={"url": miniapp_url}
        )
    ]])
    await msg.answer(uts(lang, "start").format(name=name), reply_markup=kb, parse_mode=ParseMode.HTML)


@router.message(Command("clearkeyboard"))
async def cmd_clearkeyboard(msg: Message):
    """Send a message that removes the reply keyboard."""
    from aiogram.types import ReplyKeyboardRemove
    await msg.answer("✅ Клавіатуру прибрано.", reply_markup=ReplyKeyboardRemove())


@router.message(Command("admin"))
async def cmd_admin(msg: Message):
    if msg.from_user.id not in ADMIN_IDS:
        return
    lang = await _ul(msg.from_user.id)
    await msg.answer(uts(lang, "adm_panel"), reply_markup=admin_kb(lang), parse_mode=ParseMode.HTML)


# ─── ADMIN PANEL ──────────────────────────────────────────────────────────────

@router.callback_query(F.data == "adm_panel")
async def adm_panel_cb(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("⛔", show_alert=True); return
    lang = await _ul(cq.from_user.id)
    await safe_edit(cq, uts(lang, "adm_panel"), admin_kb(lang))
    await cq.answer()

@router.callback_query(F.data == "adm_back")
async def adm_back(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    lang = await _ul(cq.from_user.id)
    await safe_edit(cq, uts(lang, "adm_panel"), admin_kb(lang))
    await cq.answer()

@router.callback_query(F.data == "adm_stats")
async def adm_stats(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS:
        await cq.answer("⛔", show_alert=True); return
    stats = await get_admin_stats()
    by_status = stats.get("channels_by_status", {})
    status_str = " | ".join(f"{k}:{v}" for k, v in by_status.items())
    text = (
        f"📊 <b>Статистика</b>\n\n"
        f"👥 Користувачів: <b>{stats.get('total_users',0)}</b>\n"
        f"🆕 Сьогодні: <b>{stats.get('new_today',0)}</b>\n"
        f"📢 Каналів: <b>{stats.get('total_channels',0)}</b>\n"
        f"📊 По статусу: {status_str}\n"
        f"📤 Опубліковано: <b>{stats.get('total_published',0)}</b>\n"
        f"💰 Дохід: <b>${stats.get('total_revenue',0):.2f}</b>\n"
        f"👥 Реферали: <b>${stats.get('total_referral_paid',0):.2f}</b>\n"
        f"🔌 AI: {stats.get('ai_provider','?')}/{stats.get('ai_model','?')}\n"
        f"💳 Payments: {stats.get('crypto_mode','?')}"
    )
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.callback_query(F.data == "adm_users")
async def adm_users(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    from services import search_users as _su
    users = await get_all_users(limit=20)
    lines = []
    for u in users:
        bal = await get_user_balance(u["telegram_id"])
        name = u.get("username") or u.get("first_name") or str(u["telegram_id"])
        lines.append(f"• <a href='tg://user?id={u['telegram_id']}'>@{name}</a> [<code>{u['telegram_id']}</code>] — ${bal:.2f}")
    text = "👥 <b>Останні користувачі</b>\n\n" + "\n".join(lines) if lines else "Немає користувачів"
    text += "\n\n💡 Пошук: /admsearch &lt;ID або @username&gt;"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.message(Command("admsearch"))
async def cmd_admsearch(msg: Message):
    if msg.from_user.id not in ADMIN_IDS: return
    from services import search_users as _su
    args = msg.text.split(maxsplit=1)
    if len(args) < 2:
        await msg.answer("Використання: /admsearch ID або @username"); return
    query = args[1].strip()
    users = await _su(query)
    if not users:
        await msg.answer(f"❌ Нічого не знайдено за запитом: {query}"); return
    lines = []
    for u in users:
        bal = await get_user_balance(u["telegram_id"])
        name = u.get("username") or u.get("first_name") or str(u["telegram_id"])
        channels = await get_user_channels(u["telegram_id"])
        ch_info = ", ".join(f"@{c.get('username',c['id'])} [{c.get('subscription_status','?')}]" for c in channels) or "—"
        lines.append(
            f"👤 <b>@{name}</b> [<code>{u['telegram_id']}</code>]\n"
            f"   💰 Баланс: ${bal:.2f}\n"
            f"   📢 Канали: {ch_info}\n"
            f"   📅 Зареєстрований: {(u.get('created_at') or '')[:10]}"
        )
    await msg.answer("🔍 <b>Результати пошуку</b>\n\n" + "\n\n".join(lines), parse_mode=ParseMode.HTML)


@router.message(Command("bonus"))
async def cmd_bonus(msg: Message):
    """Admin command: /bonus @username 10 or /bonus 123456789 5.50"""
    if msg.from_user.id not in ADMIN_IDS: return
    from services import DB_PATH, _fetchone
    import aiosqlite
    args = msg.text.split()
    if len(args) < 3:
        await msg.answer(
            "💰 <b>Нарахування бонусу</b>\n\n"
            "Формат: <code>/bonus @username сума</code>\n"
            "Або: <code>/bonus telegram_id сума</code>\n\n"
            "Приклади:\n"
            "<code>/bonus @ivan 10</code> — нарахувати $10\n"
            "<code>/bonus 123456789 5.50</code> — нарахувати $5.50\n"
            "<code>/bonus @ivan -3</code> — зняти $3",
            parse_mode=ParseMode.HTML
        )
        return
    target = args[1].strip().lstrip("@")
    try:
        amount = float(args[2])
    except ValueError:
        await msg.answer("❌ Невірна сума. Приклад: <code>/bonus @ivan 10</code>", parse_mode=ParseMode.HTML)
        return
    note = " ".join(args[3:]) if len(args) > 3 else "Бонус (admin)"
    # Find user by ID or username
    user = None
    if target.isdigit():
        user = await get_user(int(target))
    if not user:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            user = await _fetchone(db, "SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (target,))
    if not user:
        await msg.answer(f"❌ Користувача <code>{target}</code> не знайдено", parse_mode=ParseMode.HTML)
        return
    tg_id = user["telegram_id"]
    uname = user.get("username") or user.get("first_name") or str(tg_id)
    # Record transaction
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO transactions(user_id,amount,type,description) VALUES(?,?,?,?)",
            (user["id"], amount, "admin_adjust", note))
        await db.commit()
    new_bal = await adjust_user_balance(tg_id, amount)
    sign = "+" if amount > 0 else ""
    await msg.answer(
        f"✅ <b>Бонус нараховано</b>\n\n"
        f"👤 @{uname} [<code>{tg_id}</code>]\n"
        f"💰 {sign}${amount:.2f}\n"
        f"📝 {note}\n"
        f"💵 Новий баланс: <b>${new_bal:.2f}</b>",
        parse_mode=ParseMode.HTML
    )


@router.callback_query(F.data == "adm_finance")
async def adm_finance(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    stats = await get_admin_stats()
    text = (f"💰 <b>Фінанси</b>\n\nЗагальний дохід: <b>${stats.get('total_revenue',0):.2f}</b>\n"
            f"Реферальні виплати: <b>${stats.get('total_referral_paid',0):.2f}</b>\n")
    try:
        withdrawals = await get_pending_withdrawals()
        if withdrawals: text += f"\n⏳ Pending withdrawals: {len(withdrawals)}"
    except Exception: pass
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.callback_query(F.data == "adm_logs")
async def adm_logs(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    try:
        with open("/app/bot.log", "r") as f:
            last = f.readlines()[-30:]
        text = "📋 <b>Останні логи</b>\n\n<pre>" + "".join(last[-20:])[-3000:] + "</pre>"
    except Exception as e:
        text = f"📋 Логи недоступні: {e}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.callback_query(F.data == "adm_broadcast")
async def adm_broadcast_start(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id not in ADMIN_IDS: return
    await safe_edit(cq, "📢 Надішліть текст розсилки (HTML підтримується):")
    await state.set_state(AdminStates.broadcast); await cq.answer()

@router.message(AdminStates.broadcast)
async def adm_broadcast_send(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    await state.clear()
    users = await get_all_users()
    sent = 0
    for u in users:
        try:
            await msg.bot.send_message(u["telegram_id"], msg.text or msg.caption or "", parse_mode=ParseMode.HTML)
            sent += 1
            await asyncio.sleep(0.05)
        except Exception: pass
    await msg.answer(f"✅ Розсилка: {sent}/{len(users)}")

@router.callback_query(F.data == "adm_ch_list")
async def adm_ch_list(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    channels = await get_all_channels()
    lines = []
    for ch in channels[:25]:
        status = ch.get("subscription_status", "?")
        title = ch.get("title") or ch.get("username") or f"#{ch['id']}"
        username = ch.get("username", "")
        icon = {"active":"🟢","trial":"🟡","restricted":"🔴","blocked":"⚫"}.get(status,"⚪")
        try:
            s = json.loads(ch.get("settings") or "{}")
        except Exception:
            s = {}
        link = s.get("channel_link") or (f"https://t.me/{username}" if username else "")
        ap = "▶" if s.get("autopost_enabled") else "⏸"
        ppd = s.get("autopost_ppd", "6")
        lang = (s.get("channel_lang") or "off").upper()
        title_display = f"<a href='{link}'>{title}</a>" if link else f"<b>{title}</b>"
        un_display = f" @{username}" if username else ""
        lines.append(f"{icon} {title_display}{un_display}\n   {ap} {ppd}/д · {lang} · [{status}] · id:{ch['id']}")
    text = "📢 <b>Всі канали</b>\n\n" + "\n".join(lines) if lines else "Каналів немає"
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text[:4000], kb); await cq.answer()

@router.callback_query(F.data.startswith("adm_set_status:"))
async def adm_set_status(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    from services import set_channel_status
    _, ch_id_s, new_status = cq.data.split(":")
    await set_channel_status(int(ch_id_s), new_status)
    await cq.answer(f"✅ Статус → {new_status}", show_alert=True)
    await adm_ch_list(cq)

@router.callback_query(F.data == "adm_activity")
async def adm_activity(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    stats = await get_admin_stats()
    text = (f"📈 <b>Активність</b>\n\nОпубліковано всього: <b>{stats.get('total_published',0)}</b>\n"
            f"AI токенів: <b>{stats.get('total_ai_tokens',0)}</b>\n")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.callback_query(F.data == "adm_sessions")
async def adm_sessions(cq: CallbackQuery):
    if cq.from_user.id not in ADMIN_IDS: return
    from services import get_session_status as _gss
    import glob as _gl
    sess_files = sorted(f for f in _gl.glob("/app/*.session"))
    statuses = _gss()
    lines = []
    for sf in sess_files:
        import os as _osa
        num = _osa.path.splitext(_osa.path.basename(sf))[0]
        s = statuses.get(num, {})
        ok = s.get("ok", True)
        err = s.get("error") or ""
        last_ok = s.get("last_ok") or "—"
        fails = s.get("fail_count", 0)
        icon = "🟢" if ok else "🔴"
        lines.append(f"{icon} Сесія <b>#{num}</b>")
        if not ok:
            lines[-1] += f"\n  ❌ {err[:80]}" if err else "\n  ❌ недоступна"
        lines[-1] += f"\n  ✅ Остання OK: {last_ok} | Помилок: {fails}"
    text = "🔌 <b>Telethon сесії</b>\n\n" + ("\n\n".join(lines) if lines else "Немає .session файлів в /app")
    kb = InlineKeyboardMarkup(inline_keyboard=[[InlineKeyboardButton(text="🔙 Назад", callback_data="adm_back")]])
    await safe_edit(cq, text, kb); await cq.answer()

@router.callback_query(F.data.startswith("adm_adj:"))
async def adm_adj(cq: CallbackQuery, state: FSMContext):
    if cq.from_user.id not in ADMIN_IDS: return
    tg_id = int(cq.data.split(":")[1])
    await state.update_data(adj_tg_id=tg_id)
    await state.set_state(AdminStates.adj_amount)
    user = await get_user(tg_id); bal = await get_user_balance(tg_id)
    name = (user.get("username") or user.get("first_name") or str(tg_id)) if user else str(tg_id)
    await safe_edit(cq, f"💰 Зміна балансу @{name}\nПоточний: ${bal:.2f}\n\nВведіть суму (+/-):"); await cq.answer()

@router.message(AdminStates.adj_amount)
async def adm_adj_amount(msg: Message, state: FSMContext):
    if msg.from_user.id not in ADMIN_IDS: return
    data = await state.get_data(); tg_id = data.get("adj_tg_id"); await state.clear()
    try:
        delta = float(msg.text.replace(",", "."))
    except Exception:
        await msg.answer("❌ Невірний формат."); return
    new_balance = await adjust_user_balance(tg_id, delta)
    sign = "+" if delta >= 0 else ""
    await msg.answer(f"✅ Баланс змінено!\n{sign}${delta:.2f} → ${new_balance:.2f}")
    lang = await _ul(tg_id)
    try:
        await msg.bot.send_message(tg_id, uts(lang, "adm_bal_notify").format(sign=sign, delta=abs(delta), new=new_balance), parse_mode=ParseMode.HTML)
    except Exception: pass


# ─── AUTOPOST CONFIRM FLOW ────────────────────────────────────────────────────

@router.callback_query(F.data.startswith("ap_do:"))
async def ap_do(cq: CallbackQuery, bot: Bot):
    _, post_id_s, ch_id_s = cq.data.split(":")
    post_id, ch_id = int(post_id_s), int(ch_id_s)
    from services import update_post_status as _ups
    await _ups(post_id, "pending")
    try:
        await cq.message.delete()
    except Exception:
        pass
    await _do_autopost_publish(cq, bot, post_id, ch_id)


async def _do_autopost_publish(cq: CallbackQuery, bot: Bot, post_id: int, ch_id: int):
    ch = await get_channel(ch_id)
    if not ch:
        return
    if ch.get("subscription_status") not in ("active", "trial"):
        lang = await _ul(cq.from_user.id)
        await bot.send_message(
            cq.from_user.id,
            uts(lang, "pub_blocked_status").format(status=ch.get("subscription_status","")),
            parse_mode=ParseMode.HTML
        )
        return

    from services import get_channel_settings as _gcs
    settings = await _gcs(ch_id)
    posts = await get_pending_posts(ch_id)
    post = next((p for p in posts if p["id"] == post_id), None)
    if not post:
        await bot.send_message(cq.from_user.id, uts(await _ul(cq.from_user.id), "pub_post_not_found"), parse_mode=ParseMode.HTML)
        return

    if post.get("media_type") and not post.get("media_file_id"):
        post = await load_media_for_post(post)

    text          = sanitize_html_for_telegram(post.get("cleaned_text") or "")
    media_type    = post.get("media_type")
    media_file_id = post.get("media_file_id")
    chat_id       = ch["chat_id"]
    footer        = build_footer(settings, has_media=bool(media_type))
    if footer:
        text += footer
    if settings.get("postbtn_enabled") and settings.get("postbtn_label") and settings.get("postbtn_url"):
        text += "\n\n<b><a href=\"" + settings["postbtn_url"] + "\">" + settings["postbtn_label"] + "</a></b>"

    sent_msg_id = None
    try:
        if media_type == "photo" and media_file_id:
            sent = await bot.send_photo(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
            sent_msg_id = sent.message_id
        elif media_type == "video" and media_file_id:
            sent = await bot.send_video(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
            sent_msg_id = sent.message_id
        elif media_type == "animation" and media_file_id:
            sent = await bot.send_animation(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
            sent_msg_id = sent.message_id
        elif media_type == "album" and post.get("media_files_json"):
            files = json.loads(post["media_files_json"])
            mg = []
            for i, f in enumerate(files):
                fid = f.get("file_id"); ftyp = f.get("type", "photo")
                cap = _safe_caption(text) if i == 0 else None
                pm  = ParseMode.HTML if i == 0 else None
                mg.append(InputMediaVideo(media=fid, caption=cap, parse_mode=pm) if ftyp == "video"
                          else InputMediaPhoto(media=fid, caption=cap, parse_mode=pm))
            if mg:
                msgs = await bot.send_media_group(chat_id, mg)
                sent_msg_id = msgs[0].message_id
        else:
            sent = await bot.send_message(chat_id, _safe_text(text), parse_mode=ParseMode.HTML,
                                          disable_web_page_preview=True)
            sent_msg_id = sent.message_id

        from services import update_post_status as _ups, save_last_published as _slp, cleanup_published_media as _cpm
        await _ups(post_id, "published")
        if sent_msg_id:
            await _slp(ch_id, sent_msg_id)
        await _cpm(post_id)

        lang = await _ul(cq.from_user.id)
        next_time = calc_next_autopost_time(settings, lang=lang)
        ch_title = ch.get("title") or ch.get("username") or f"Канал {ch_id}"
        await bot.send_message(
            cq.from_user.id,
            f"{uts(lang, 'ap_published_ok')}\n\n📢 {ch_title}\n{uts(lang, 'ap_next_post_at', time=next_time)}",
            parse_mode=ParseMode.HTML
        )
    except Exception as e:
        log.error(f"ap_do publish error ch={ch_id} post={post_id}: {e}")
        await bot.send_message(cq.from_user.id,
                               uts(await _ul(cq.from_user.id), "pub_error").format(err=e),
                               parse_mode=ParseMode.HTML)


@router.callback_query(F.data.startswith("ap_skip:"))
async def ap_skip(cq: CallbackQuery, bot: Bot):
    _, post_id_s, ch_id_s = cq.data.split(":")
    post_id, ch_id = int(post_id_s), int(ch_id_s)
    ch = await get_channel(ch_id)
    if ch and ch.get("subscription_status") not in ("active", "trial"):
        await cq.answer(uts(await _ul(cq.from_user.id), "ap_skip_inactive"), show_alert=True)
        try:
            await cq.message.delete()
        except Exception:
            pass
        return
    from services import update_post_status as _ups, reset_confirm_skipped as _rcs
    await _ups(post_id, "confirm_skipped")
    try:
        await _rcs(ch_id)
    except Exception:
        pass
    try:
        await cq.message.delete()
    except Exception:
        pass
    lang = await _ul(cq.from_user.id)
    await bot.send_message(cq.from_user.id, uts(lang, "ap_skipped"), parse_mode=ParseMode.HTML)
    await cq.answer()


# ─── PATTERN VERIFICATION FLOW ────────────────────────────────────────────────

async def _user_owns_source(tg_id: int, src_id: int) -> bool:
    """Verify that the given Telegram user owns the channel that owns this source.
    Admins bypass this check."""
    if tg_id in ADMIN_IDS:
        return True
    try:
        src = await get_source(src_id)
        if not src:
            return False
        channels = await get_user_channels(tg_id)
        return any(c["id"] == src["channel_id"] for c in channels)
    except Exception:
        return False


@router.callback_query(F.data.startswith("patv_ok:"))
async def patv_ok(cq: CallbackQuery, state: FSMContext):
    """User confirmed cleaning pattern looks correct → mark source as verified."""
    try:
        src_id = int(cq.data.split(":")[1])
        if not await _user_owns_source(cq.from_user.id, src_id):
            await cq.answer("⛔ Нет прав", show_alert=True); return
        await set_pattern_verified(src_id, True)
        await state.clear()
        src = await get_source(src_id)
        name = (src.get("username") or src.get("title") or f"#{src_id}") if src else f"#{src_id}"
        try:
            await cq.message.edit_text(
                f"✅ <b>Паттерн подтвержден</b>\n📡 Источник: <b>@{name}</b>\n\n"
                "Теперь рядом с этим источником будет зеленая галочка ✔︎\n"
                "Посты будут парситься с применением этого паттерна.",
                parse_mode=ParseMode.HTML
            )
        except TelegramBadRequest:
            pass
        await cq.answer("✅ Подтверждено!")
    except Exception as _e:
        log.warning(f"patv_ok error: {_e}")
        try:
            await cq.answer(f"❌ Ошибка: {_e}", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("patv_cancel:"))
async def patv_cancel(cq: CallbackQuery, state: FSMContext):
    """User cancelled the verification dialog."""
    try:
        src_id = int(cq.data.split(":")[1])
        if not await _user_owns_source(cq.from_user.id, src_id):
            await cq.answer("⛔ Нет прав", show_alert=True); return
        await state.clear()
        try:
            await cq.message.edit_text("❌ Проверка отменена.")
        except TelegramBadRequest:
            pass
        await cq.answer("Отменено")
    except Exception as _e:
        log.warning(f"patv_cancel error: {_e}")
        try:
            await cq.answer(f"❌ Ошибка: {_e}", show_alert=True)
        except Exception:
            pass


@router.callback_query(F.data.startswith("patv_fix:"))
async def patv_fix(cq: CallbackQuery, state: FSMContext):
    """User wants to provide a correction pattern."""
    try:
        src_id = int(cq.data.split(":")[1])
        if not await _user_owns_source(cq.from_user.id, src_id):
            await cq.answer("⛔ Нет прав", show_alert=True); return
        await state.update_data(patv_src_id=src_id)
        await state.set_state(PatternStates.waiting_correction)
        try:
            await cq.message.edit_text(
                "✏️ Отправьте текст/фрагмент, который нужно вырезать из постов этого источника.\n\n"
                "Следующее текстовое сообщение будет добавлено как паттерн.\n\n"
                "<i>Пример: если подпись выглядит так:</i>\n"
                "<code>📢 Наш канал | @channel</code>\n"
                "<i>— отправьте этот текст целиком.</i>",
                parse_mode=ParseMode.HTML
            )
        except TelegramBadRequest:
            pass
        await cq.answer()
    except Exception as _e:
        log.warning(f"patv_fix error: {_e}")
        try:
            await cq.answer(f"❌ Ошибка: {_e}", show_alert=True)
        except Exception:
            pass


@router.message(PatternStates.waiting_correction)
async def patv_correction_input(msg: Message, state: FSMContext):
    """Receive correction pattern text, add to source_patterns, re-send verification."""
    data = await state.get_data()
    src_id = data.get("patv_src_id")
    await state.clear()
    if not src_id:
        return
    if not await _user_owns_source(msg.from_user.id, src_id):
        await msg.answer("⛔ Нет прав на этот источник.")
        return
    pattern = (msg.text or "").strip()
    if not pattern:
        await msg.answer("❌ Пустой паттерн — отменено.")
        return
    # Save manual pattern
    import aiosqlite as _aio
    try:
        async with _aio.connect(DB_PATH, timeout=10) as db:
            await db.execute(
                "INSERT INTO source_patterns(source_id, pattern, auto) VALUES(?,?,0)",
                (src_id, pattern))
            # Any new pattern invalidates prior verification
            await db.execute(
                "UPDATE sources SET pattern_verified=0 WHERE id=?", (src_id,))
            await db.commit()
    except Exception as _e:
        await msg.answer(f"❌ Не удалось сохранить паттерн: {_e}")
        return
    await msg.answer(
        f"✅ Паттерн добавлен:\n<code>{pattern[:200]}</code>\n\n"
        "Сейчас пришлю новые примеры очистки для повторной проверки...",
        parse_mode=ParseMode.HTML
    )
    # Re-apply patterns to existing pending posts and re-send verification examples
    try:
        from services import _get_source_signature, reprocess_pending_with_signature
        src = await get_source(src_id)
        if src:
            sig = await _get_source_signature(src_id)
            await reprocess_pending_with_signature(src["channel_id"], src_id, sig or "")
    except Exception as _e:
        log.debug(f"patv_correction reprocess: {_e}")
    try:
        await send_pattern_verification_request(src_id)
    except Exception as _e:
        log.warning(f"patv_correction resend: {_e}")


# ─── /login <user_id> — admin miniapp impersonation ──────────────────────────

@router.message(Command("login"))
async def cmd_login(msg: Message):
    """Admin command: /login <user_id>  — open the Mini App as the given user.
    Generates a link with ?dev_id=<uid> that the miniapp uses as X-Dev-Id
    header for all API calls (admin-only impersonation)."""
    if msg.from_user.id not in ADMIN_IDS:
        return
    args = (msg.text or "").split(maxsplit=1)
    if len(args) < 2:
        await msg.answer(
            "🛂 <b>Вхід від імені користувача</b>\n\n"
            "Формат: <code>/login &lt;user_id або @username&gt;</code>\n\n"
            "Приклади:\n"
            "<code>/login 123456789</code>\n"
            "<code>/login @ivan</code>",
            parse_mode=ParseMode.HTML
        )
        return
    target = args[1].strip().lstrip("@")
    target_tg_id = None
    target_name = ""
    # Resolve target
    import aiosqlite as _aio
    try:
        if target.isdigit():
            u = await get_user(int(target))
            if u:
                target_tg_id = u["telegram_id"]
                target_name = u.get("username") or u.get("first_name") or str(target_tg_id)
        if target_tg_id is None:
            async with _aio.connect(DB_PATH, timeout=10) as db:
                row = await _fetchone(db,
                    "SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (target,))
            if row:
                target_tg_id = row["telegram_id"]
                target_name = row.get("username") or row.get("first_name") or str(target_tg_id)
    except Exception as _e:
        await msg.answer(f"❌ Помилка пошуку: {_e}")
        return
    if not target_tg_id:
        await msg.answer(f"❌ Користувача <code>{target}</code> не знайдено.",
                         parse_mode=ParseMode.HTML)
        return
    miniapp_url = f"https://autopost.f.jrnm.app/miniapp?dev_id={target_tg_id}"
    kb = InlineKeyboardMarkup(inline_keyboard=[[
        InlineKeyboardButton(
            text=f"🚪 Відкрити як @{target_name}",
            web_app={"url": miniapp_url}
        )
    ]])
    await msg.answer(
        f"🛂 <b>Вхід як користувач</b>\n\n"
        f"👤 @{target_name} [<code>{target_tg_id}</code>]\n\n"
        f"Натисніть кнопку нижче — Mini App відкриється від імені цього користувача. "
        f"Усі дії в додатку будуть виконуватись від його імені.",
        reply_markup=kb,
        parse_mode=ParseMode.HTML
    )
