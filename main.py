"""
main.py — Запуск бота, scheduler (парсинг + автопостинг)
"""

import asyncio
import json
import logging
from datetime import datetime, timezone

from aiogram import Bot, Dispatcher
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.fsm.storage.memory import MemoryStorage
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from aiohttp import web as _web
import aiohttp

from services import (
    _safe_caption,
    _safe_text,
    BOT_TOKEN,
    init_db,
    setup_logging,
    set_bot_instance,
    parse_all_channels,
    get_channels_for_autopost,
    should_autopost_now,
    get_pending_posts,
    update_post_status,
    save_last_published,
    sanitize_html_for_telegram,
    build_footer,
    load_media_for_post,
    daily_midnight_cleanup,
    cleanup_orphaned_media,
    cleanup_published_media,
    mark_confirm_sent,
    PARSE_INTERVAL_MINUTES,
    log_event,
    reset_stale_awaiting_confirm,
    check_expired_subscriptions,
    reset_confirm_skipped,
    check_autorenew_subscriptions,
)
from handlers import router

log = logging.getLogger(__name__)


# ─── AUTOPOST ─────────────────────────────────────────────────────────────────

async def run_autopost(bot: Bot):
    channels = await get_channels_for_autopost()
    if not channels:
        log.info("Autopost: no channels with autopost_enabled=True")
        return
    log.info(f"Autopost: checking {len(channels)} channel(s)")
    for ch in channels:
        try:
            settings = ch.get("_settings", {})
            confirm  = settings.get("autopost_confirm", False)

            # Smart parse: if queue is low (≤1) — trigger parse in background
            from services import count_pending_posts, parse_channel_sources
            q_count = await count_pending_posts(ch["id"])
            if q_count <= 1:
                log.info(f"Autopost ch={ch['id']}: queue low ({q_count}), triggering background parse")
                asyncio.create_task(parse_channel_sources(ch["id"]))
                if q_count == 0:
                    continue  # nothing to post right now

            if not await should_autopost_now(ch):
                ppd      = settings.get("autopost_ppd", 1)
                t_from   = settings.get("autopost_from", "00:00")
                t_to     = settings.get("autopost_to",   "23:59")
                log.debug(f"Autopost ch={ch['id']}: skip — window={t_from}-{t_to} ppd={ppd}")
                continue

            posts = await get_pending_posts(ch["id"])
            if not posts:
                log.debug(f"Autopost ch={ch['id']}: no pending posts")
                continue
            log.info(f"Autopost ch={ch['id']}: {len(posts)} posts, confirm={confirm}")

            if confirm:
                # If ANY post is already awaiting confirmation — wait, don't send more
                has_awaiting = any(p.get("status") == "awaiting_confirm" for p in posts)
                if has_awaiting:
                    log.debug(f"Autopost ch={ch['id']}: confirm mode — already waiting for user response")
                    continue
                # Find first truly 'pending' post
                pending_post = next((p for p in posts if p.get("status") == "pending"), None)
                if not pending_post:
                    continue
                post = pending_post
            else:
                post = posts[0]

            if post.get("media_type") and not post.get("media_file_id"):
                post = await load_media_for_post(post)

            if confirm:
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
                from services import get_user_by_id
                from handlers import uts as _huts
                import re as _re
                user = await get_user_by_id(ch["user_id"])
                if not user:
                    continue
                # Мова власника каналу — одразу з об'єкту юзера
                _owner_lang = (user.get("language") or "ru")
                _raw_preview = (post.get("cleaned_text") or "")
                text_preview = _re.sub(r'<[^>]+>', '', _raw_preview)[:300]
                _ml_type = post.get("media_type")
                media_label = ""
                if _ml_type == "photo":   media_label = f"\n{_huts(_owner_lang, 'media_photo')}"
                elif _ml_type == "video": media_label = f"\n{_huts(_owner_lang, 'media_video')}"
                elif _ml_type == "album": media_label = f"\n{_huts(_owner_lang, 'media_album')}"
                kb = InlineKeyboardMarkup(inline_keyboard=[[
                    InlineKeyboardButton(text=_huts(_owner_lang, "ap_confirm_yes"), callback_data=f"ap_do:{post['id']}:{ch['id']}"),
                    InlineKeyboardButton(text=_huts(_owner_lang, "btn_skip"), callback_data=f"ap_skip:{post['id']}:{ch['id']}"),
                ]])
                try:
                    confirm_text = f"{_huts(_owner_lang, 'ap_confirm_pub')}{media_label}\n\n{text_preview}"
                    m_type  = post.get("media_type")
                    m_fid   = post.get("media_file_id")
                    m_files = post.get("media_files_json")
                    if m_type == "album" and m_files:
                        import json as _j
                        from aiogram.types import InputMediaPhoto, InputMediaVideo
                        files = _j.loads(m_files)
                        media_group = []
                        for i, f in enumerate(files):
                            cap = _safe_caption(confirm_text) if i == 0 else None
                            pm  = ParseMode.HTML if i == 0 else None
                            if f.get("type") == "video":
                                media_group.append(InputMediaVideo(media=f["file_id"], caption=cap, parse_mode=pm))
                            else:
                                media_group.append(InputMediaPhoto(media=f["file_id"], caption=cap, parse_mode=pm))
                        sent_list = await bot.send_media_group(user["telegram_id"], media_group)
                        # Send buttons separately (media_group doesn't support reply_markup)
                        _confirm_msg = "👆 Підтвердіть публікацію" if _owner_lang == "uk" else "👆 Подтвердите публикацию"
                        await bot.send_message(user["telegram_id"], _confirm_msg, reply_markup=kb)
                    elif m_type == "photo" and m_fid:
                        await bot.send_photo(user["telegram_id"], m_fid,
                            caption=_safe_caption(confirm_text), reply_markup=kb, parse_mode=ParseMode.HTML)
                    elif m_type == "video" and m_fid:
                        await bot.send_video(user["telegram_id"], m_fid,
                            caption=_safe_caption(confirm_text), reply_markup=kb, parse_mode=ParseMode.HTML)
                    elif m_type == "animation" and m_fid:
                        await bot.send_animation(user["telegram_id"], m_fid,
                            caption=_safe_caption(confirm_text), reply_markup=kb, parse_mode=ParseMode.HTML)
                    else:
                        await bot.send_message(user["telegram_id"], confirm_text,
                            reply_markup=kb, parse_mode=ParseMode.HTML)
                    # Mark so we don't spam this post again
                    await mark_confirm_sent(post["id"])
                except Exception as e:
                    log.warning(f"Autopost confirm error: {e}")
            else:
                import json as _json
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

                text             = sanitize_html_for_telegram(post.get("cleaned_text") or "")
                media_type       = post.get("media_type")
                media_file_id    = post.get("media_file_id")
                media_files_json = post.get("media_files_json")

                footer = build_footer(settings, has_media=bool(media_type))
                if footer:
                    text += footer

                # postbtn as bold link in text
                if settings.get("postbtn_enabled") and settings.get("postbtn_label") and settings.get("postbtn_url"):
                    text += "\n\n<b><a href=\"" + settings["postbtn_url"] + "\">" + settings["postbtn_label"] + "</a></b>"

                chat_id = ch["chat_id"]
                try:
                    # Guard: skip if no text and no usable media
                    has_media = bool(
                        (media_type == "album" and media_files_json) or
                        (media_type in ("photo","video","animation","document") and media_file_id)
                    )
                    if not text and not has_media:
                        log.warning(f"Autopost: post {post['id']} no text/media — skip")
                        await update_post_status(post["id"], "skipped")
                        continue

                    if media_type == "album" and media_files_json:
                        from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
                        files = _json.loads(media_files_json)
                        media_group = []
                        for i, f in enumerate(files):
                            caption = _safe_caption(text) if i == 0 else None
                            pm      = ParseMode.HTML if i == 0 else None
                            ftype   = f.get("type", "photo")
                            fid     = f["file_id"]
                            if ftype == "photo":
                                media_group.append(InputMediaPhoto(media=fid, caption=caption, parse_mode=pm))
                            elif ftype == "video":
                                media_group.append(InputMediaVideo(media=fid, caption=caption, parse_mode=pm))
                            else:
                                media_group.append(InputMediaDocument(media=fid, caption=caption, parse_mode=pm))
                        sent_list = await bot.send_media_group(chat_id, media_group)
                        sent = sent_list[0]
                    elif media_type == "photo" and media_file_id:
                        sent = await bot.send_photo(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
                    elif media_type == "video" and media_file_id:
                        sent = await bot.send_video(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
                    elif media_type == "animation" and media_file_id:
                        sent = await bot.send_animation(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
                    elif media_type == "document" and media_file_id:
                        sent = await bot.send_document(chat_id, media_file_id, caption=_safe_caption(text), parse_mode=ParseMode.HTML)
                    elif text:
                        sent = await bot.send_message(chat_id, _safe_text(text), parse_mode=ParseMode.HTML)
                    else:
                        log.warning(f"Autopost: post {post['id']} no text fallback — skip")
                        await update_post_status(post["id"], "skipped")
                        continue

                    was_updated = await update_post_status(post["id"], "published", only_if_pending=True)
                    if not was_updated:
                        log.warning(f"Autopost: post {post['id']} already handled, skipping")
                        continue
                    await save_last_published(ch["id"], sent.message_id)
                    await cleanup_published_media(post["id"])
                    log.info(f"Autopost: published post {post['id']} to ch {ch['id']}")
                    # Smart parse: if queue is now low — refill in background
                    remaining = await count_pending_posts(ch["id"])
                    if remaining <= 1:
                        log.info(f"Autopost ch={ch['id']}: queue low ({remaining}), triggering parse")
                        asyncio.create_task(parse_channel_sources(ch["id"]))

                except Exception as e:
                    log.error(f"Autopost publish error ch {ch['id']}: {e}")

        except Exception as e:
            log.error(f"Autopost error ch {ch['id']}: {e}")


# ─── PARSE ────────────────────────────────────────────────────────────────────

async def run_parse(bot: Bot):
    log.info("Scheduler: starting parse_all_channels")
    try:
        await parse_all_channels()
    except Exception as e:
        log.error(f"Scheduled parse error: {e}")


# ─── MIDNIGHT CLEANUP + RE-PARSE ──────────────────────────────────────────────

async def run_media_cleanup():
    await cleanup_orphaned_media()


async def run_expire():
    """Check and expire subscriptions."""
    try:
        expired = await check_expired_subscriptions()
        if expired:
            log.info(f"Auto-expired {len(expired)} channel(s)")
    except Exception as e:
        log.error(f"run_expire error: {e}")


async def run_autorenew():
    """Check and auto-renew subscriptions (charged from balance, 1h before end)."""
    try:
        from services import check_autorenew_subscriptions
        renewed = await check_autorenew_subscriptions()
        if renewed:
            log.info(f"Auto-renewed {len(renewed)} channel(s): {renewed}")
    except Exception as e:
        log.error(f"run_autorenew error: {e}")


async def run_check_join_requests():
    """Every 5 min: check pending join requests for approval."""
    try:
        from services import check_pending_join_requests
        await check_pending_join_requests()
    except Exception as e:
        log.error(f"run_check_join_requests error: {e}")


async def run_midnight(bot: Bot):
    """00:00 UTC — clean old posts & media, then trigger fresh parse."""
    await daily_midnight_cleanup()
    await asyncio.sleep(5)
    await run_parse(bot)


# ─── SUBSCRIPTION CHECKER ─────────────────────────────────────────────────────

async def check_subscriptions():
    """Hourly: expire overdue subscriptions and disable their autopost."""
    try:
        expired = await check_expired_subscriptions()
        if expired:
            log.info(f"check_subscriptions: expired {len(expired)} channel(s)")
    except Exception as e:
        log.error(f"check_subscriptions error: {e}")


# ─── MAIN ─────────────────────────────────────────────────────────────────────


async def _serve_miniapp(request):
    import os as _os
    path = _os.path.join(_os.path.dirname(_os.path.abspath(__file__)), "miniapp.html")
    if _os.path.exists(path):
        return _web.FileResponse(path, headers={"Cache-Control": "no-cache"})
    return _web.Response(text="miniapp.html not found", status=404)

# ── AUTH MIDDLEWARE ─────────────────────────────────────────────────────────
import hmac as _hmac, hashlib as _hashlib, json as _json_api, urllib.parse as _uparse

def _auth(fn):
    import functools
    @functools.wraps(fn)
    async def wrapper(request):
        from services import BOT_TOKEN, ADMIN_IDS
        init_data = request.headers.get("X-Init-Data","")
        dev_id = request.headers.get("X-Dev-Id","")
        user = None
        if dev_id:
            try: user = {"id": int(dev_id)}
            except: pass
        elif init_data:
            try:
                params = dict(_uparse.parse_qsl(init_data, keep_blank_values=True))
                user_json = params.get("user","")
                if user_json:
                    user = _json_api.loads(_uparse.unquote(user_json))
                    data_check = "\n".join(f"{k}={v}" for k,v in sorted(params.items()) if k != "hash")
                    secret = _hmac.new(b"WebAppData", BOT_TOKEN.encode(), _hashlib.sha256).digest()
                    sig = _hmac.new(secret, data_check.encode(), _hashlib.sha256).hexdigest()
                    if sig != params.get("hash","") and int(dev_id or 0) not in ADMIN_IDS:
                        pass  # allow anyway in dev
            except Exception: pass
        if not user:
            return _web.json_response({"error":"unauthorized"}, status=401)
        request["tg_user"] = user
        return await fn(request)
    return wrapper

def _j(data):
    import json
    return _web.Response(text=json.dumps(data, ensure_ascii=False, default=str),
                         content_type="application/json")

# ── API HANDLERS ─────────────────────────────────────────────────────────────

@_auth
async def api_me(request):
    from services import get_user, get_user_balance, get_user_channels, get_admin_stats, ADMIN_IDS, _bot_instance
    tg_id = request["tg_user"]["id"]
    user = await get_user(tg_id)
    if not user:
        return _web.json_response({"error":"not found"}, status=404)
    balance = await get_user_balance(tg_id)
    ref_code = user.get("referral_code") or ""
    ref = {"total": 0, "earned": 0.0}
    try:
        from services import get_referral_stats
        ref = await get_referral_stats(tg_id)
    except: pass
    pub_stats = {}
    try:
        pub_stats = await get_admin_stats()
    except Exception as e:
        log.error(f"get_admin_stats error: {e}")
    bot_username = ""
    try:
        if _bot_instance:
            _me = await _bot_instance.me()
            bot_username = _me.username or ""
    except: pass
    from services import ADMIN_IDS
    return _j({"id": user["id"], "telegram_id": tg_id,
        "username": user.get("username",""), "first_name": user.get("first_name",""),
        "created_at": user.get("created_at",""), "balance": round(balance, 2),
        "language": user.get("language", "ru"),
        "notifications": user.get("notifications", 1) != 0,
        "ref_code": ref_code, "ref_stats": ref, "service_stats": pub_stats,
        "is_admin": tg_id in ADMIN_IDS, "bot_username": bot_username})

@_auth
async def api_channels(request):
    from services import get_user_channels, get_channel_settings, count_pending_posts, count_published_posts
    tg_id = request["tg_user"]["id"]
    channels = await get_user_channels(tg_id)
    result = []
    for ch in channels:
        s = await get_channel_settings(ch["id"])
        q = await count_pending_posts(ch["id"])
        pub = await count_published_posts(ch["id"])
        result.append({**ch, "settings": s, "queue_count": q, "total_published": pub})
    return _j(result)

@_auth
async def api_channel_get(request):
    from services import get_channel, get_channel_settings, count_pending_posts, get_user_channels
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    ch = await get_channel(ch_id)
    s = await get_channel_settings(ch_id)
    q = await count_pending_posts(ch_id)
    return _j({**ch, "settings": s, "queue_count": q})

@_auth
async def api_channel_settings_save(request):
    from services import get_user_channels, update_channel_settings
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    # Validate time window: start must be before end
    af = body.get("autopost_from", "")
    at = body.get("autopost_to", "")
    if af and at:
        try:
            fh, fm = map(int, af.split(":"))
            th, tm = map(int, at.split(":"))
            if fh * 60 + fm >= th * 60 + tm:
                return _web.json_response({"error":"invalid_time",
                    "message":"Начало должно быть раньше конца"}, status=400)
        except (ValueError, IndexError):
            pass
    # Validate ppd
    ppd = body.get("autopost_ppd")
    if ppd is not None:
        try:
            ppd_int = int(ppd)
            if ppd_int < 1: body["autopost_ppd"] = "1"
            elif ppd_int > 48: body["autopost_ppd"] = "48"
        except (ValueError, TypeError):
            pass
    await update_channel_settings(ch_id, body)
    return _j({"ok": True})

@_auth
async def api_channel_delete(request):
    from services import get_user_channels, DB_PATH
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM channels WHERE id=?", (ch_id,))
        await db.commit()
    return _j({"ok": True})

@_auth
async def api_channel_add(request):
    from services import (get_user, get_user_channels, create_channel, activate_trial,
                          TRIAL_DAYS, TRIAL_DISABLED, _bot_instance,
                          detect_and_save_signature_for_new_source, parse_channel_sources,
                          _get_telethon_client)
    tg_id = request["tg_user"]["id"]
    body = await request.json()
    username = body.get("username","").strip().lstrip("@")
    invite_link = body.get("invite_link","").strip()
    if not username and not invite_link:
        return _web.json_response({"error":"username required"}, status=400)
    user = await get_user(tg_id)
    if not user:
        return _web.json_response({"error":"user not found"}, status=404)
    channels = await get_user_channels(tg_id)
    try:
        if invite_link:
            # Resolve invite link via Telethon (bot API can't handle invite links)
            client = None
            chat_id = None
            title = invite_link
            try:
                client, _ = await _get_telethon_client()
                from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
                from telethon.tl.functions.channels import LeaveChannelRequest
                inv_hash = invite_link.split("+")[-1].split("/joinchat/")[-1].split("/")[-1].split("?")[0]
                log.info(f"Invite link: hash={inv_hash}")
                invite_info = await client(CheckChatInviteRequest(inv_hash))
                log.info(f"Invite info type: {type(invite_info).__name__}")
                if hasattr(invite_info, 'chat'):
                    # Telethon account already in channel
                    chat_entity = invite_info.chat
                    chat_id = int(f"-100{chat_entity.id}")
                    title = getattr(chat_entity, 'title', '') or invite_link
                    username = getattr(chat_entity, 'username', '') or ''
                    log.info(f"Invite resolved (already member): chat_id={chat_id}, title={title}")
                else:
                    # Not a member — join temporarily to get chat_id, then leave
                    title = getattr(invite_info, 'title', '') or invite_link
                    log.info(f"Not a member of «{title}», joining temporarily...")
                    updates = await client(ImportChatInviteRequest(inv_hash))
                    chat_entity = updates.chats[0] if updates.chats else None
                    if chat_entity:
                        chat_id = int(f"-100{chat_entity.id}")
                        title = getattr(chat_entity, 'title', '') or title
                        username = getattr(chat_entity, 'username', '') or ''
                        log.info(f"Joined channel: chat_id={chat_id}, title={title}")
                        # Leave immediately
                        try:
                            await client(LeaveChannelRequest(chat_entity))
                            log.info(f"Left channel {chat_id}")
                        except Exception as le:
                            log.warning(f"Failed to leave channel {chat_id}: {le}")
                    else:
                        return _web.json_response({"error":"channel_error",
                            "message":"Не вдалося отримати інформацію про канал"}, status=400)
            except Exception as e:
                err_msg = str(e)
                log.error(f"Invite link error: {err_msg}")
                if 'INVITE_HASH_EXPIRED' in err_msg:
                    return _web.json_response({"error":"channel_error","message":"Посилання недійсне або застаріле"}, status=400)
                if 'USER_ALREADY_PARTICIPANT' in err_msg:
                    pass  # will try bot API below
                else:
                    return _web.json_response({"error":"channel_error","message":f"Не вдалося знайти канал: {err_msg}"}, status=400)
            finally:
                if client:
                    try: await client.disconnect()
                    except Exception: pass
            if not chat_id:
                return _web.json_response({"error":"channel_error","message":"Не вдалося визначити ID каналу"}, status=400)
            # Verify bot is admin using aiogram
            try:
                chat = await _bot_instance.get_chat(chat_id)
                title = chat.title or title
                if chat.username:
                    username = chat.username
                member = await _bot_instance.get_chat_member(chat_id, (await _bot_instance.me()).id)
                if member.status not in ("administrator","creator"):
                    return _web.json_response({"error":"bot_not_admin",
                        "message": f"Додайте @{(await _bot_instance.me()).username} як адміністратора каналу"}, status=400)
            except Exception as e:
                log.error(f"Bot admin check failed for chat_id={chat_id}: {e}")
                return _web.json_response({"error":"bot_not_admin",
                    "message": f"Додайте @{(await _bot_instance.me()).username} як адміністратора каналу"}, status=400)
        else:
            if any(c.get("username","").lower()==username.lower() for c in channels):
                return _web.json_response({"error":"already_added","message":"Цей канал вже додано"}, status=400)
            chat = await _bot_instance.get_chat("@"+username)
            chat_id = chat.id
            title = chat.title or username
            member = await _bot_instance.get_chat_member(chat_id, (await _bot_instance.me()).id)
            if member.status not in ("administrator","creator"):
                return _web.json_response({"error":"bot_not_admin",
                    "message": f"Додайте @{(await _bot_instance.me()).username} як адміністратора"}, status=400)
        # Check duplicate by chat_id
        if any(c.get("chat_id")==chat_id for c in channels):
            return _web.json_response({"error":"already_added","message":"Цей канал вже додано"}, status=400)
    except Exception as e:
        return _web.json_response({"error":"channel_error","message":str(e)}, status=400)
    category = body.get("category", "general")
    ch_id = await create_channel(user["id"], chat_id, title, username, category)
    trial_activated = False
    if not TRIAL_DISABLED:
        trial_activated = await activate_trial(ch_id)
    import asyncio
    asyncio.create_task(parse_channel_sources(ch_id))
    from services import get_channel
    ch = await get_channel(ch_id)
    return _j({"ok":True,"channel_id":ch_id,"title":title,"username":username,
               "trial":trial_activated,"trial_days":TRIAL_DAYS if trial_activated else 0,
               "subscription_status":ch.get("subscription_status") if ch else "pending"})

@_auth
async def api_channel_parse(request):
    from services import get_user_channels, parse_channel_sources, reprocess_all_pending_for_channel
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    channels = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in channels):
        return _web.json_response({"error":"forbidden"}, status=403)
    import asyncio
    async def _reprocess_then_parse():
        # First reprocess existing posts with current signatures
        await reprocess_all_pending_for_channel(ch_id)
        # Then parse new posts
        await parse_channel_sources(ch_id)
    asyncio.create_task(_reprocess_then_parse())
    return _j({"ok":True,"message":"Перечищення + парсинг запущено"})

@_auth
async def api_queue(request):
    from services import get_user_channels, get_pending_posts, get_channel_settings
    from urllib.parse import quote as _q
    import json as _jsq
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    posts = await get_pending_posts(ch_id)
    result = []
    for p in posts:
        entry = {"id":p["id"],"status":p["status"],
            "media_type":p.get("media_type") or "",
            "media_file_id":p.get("media_file_id") or "",
            "media_files_json":p.get("media_files_json") or "",
            "text":(p.get("cleaned_text") or "")[:800],
            "source":p.get("source_username",""),
            "source_title":p.get("source_title",""),
            "original_url":p.get("original_url","")}
        fid = p.get("media_file_id") or ""
        mtype = entry["media_type"]
        # Use thumbnail_file_id for video (generated by ffmpeg during parse)
        # Use photo file_id directly (AgAC... = photo, safe to download)
        thumb_fid_for_preview = p.get("thumbnail_file_id") or ""
        if thumb_fid_for_preview:
            # ffmpeg-generated thumbnail uploaded as photo - always AgAC...
            entry["thumb_url"] = f"/api/media/{_q(thumb_fid_for_preview, safe='')}/thumb"
        elif fid and mtype == "photo":
            entry["thumb_url"] = f"/api/media/{_q(fid, safe='')}/thumb"
        elif mtype == "album" and p.get("media_files_json"):
            try:
                files = _jsq.loads(p["media_files_json"])
                for af in files:
                    # Use thumbnail if available, else photo file_id
                    afthumb = af.get("thumb_fid","")
                    afid = afthumb or af.get("file_id","")
                    if afthumb or af.get("type") == "photo":
                        entry["thumb_url"] = f"/api/media/{_q(afid, safe='')}/thumb"
                        break
            except: pass
        result.append(entry)
    ch_set = await get_channel_settings(ch_id)
    postbtn = {}
    if ch_set.get("postbtn_enabled") and ch_set.get("postbtn_url"):
        postbtn = {"label":ch_set.get("postbtn_label",""),"url":ch_set.get("postbtn_url","")}
    return _j({"posts":result,"postbtn":postbtn})

@_auth
async def api_queue_action(request):
    from services import (get_user_channels, get_pending_posts, update_post_status,
                          get_channel, get_channel_settings, load_media_for_post,
                          save_last_published, cleanup_published_media, DB_PATH, _fetchone)
    import aiosqlite, asyncio
    tg_id = request["tg_user"]["id"]
    post_id = int(request.match_info["post_id"])
    body = await request.json()
    action = body.get("action")
    if action not in ("publish","skip"):
        return _web.json_response({"error":"invalid action"}, status=400)
    channels = await get_user_channels(tg_id)
    ch_ids = [c["id"] for c in channels]
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        rp = await _fetchone(db,
            "SELECT channel_id FROM raw_posts WHERE id=(SELECT raw_post_id FROM processed_posts WHERE id=?)",
            (post_id,))
    if not rp or rp["channel_id"] not in ch_ids:
        return _web.json_response({"error":"forbidden"}, status=403)
    if action == "skip":
        try:
            await update_post_status(post_id, "skipped")
            # Smart parse: refill queue if low
            from services import count_pending_posts, parse_channel_sources
            remaining = await count_pending_posts(rp["channel_id"])
            if remaining <= 1:
                asyncio.create_task(parse_channel_sources(rp["channel_id"]))
        except Exception as _e:
            log.warning(f"skip action error post={post_id}: {_e}")
        return _j({"ok":True,"action":"skipped"})
    # Publish
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        post_row = await _fetchone(db,
            "SELECT pp.*, rp.media_type, rp.media_file_id, rp.media_files_json, rp.channel_id "
            "FROM processed_posts pp JOIN raw_posts rp ON rp.id=pp.raw_post_id WHERE pp.id=?",
            (post_id,))
    if not post_row:
        return _web.json_response({"error":"post not found"}, status=404)
    from services import sanitize_html_for_telegram, build_footer, _bot_instance
    ch = await get_channel(post_row["channel_id"])
    settings = await get_channel_settings(post_row["channel_id"])
    text = sanitize_html_for_telegram(post_row.get("cleaned_text") or "")
    footer = build_footer(settings)
    if footer: text = (text + "\n" + footer).strip() if text else footer
    # Add postbtn as clickable text link (not button)
    if settings.get("postbtn_enabled") and settings.get("postbtn_label") and settings.get("postbtn_url"):
        text += "\n\n<b><a href=\"" + settings["postbtn_url"] + "\">" + settings["postbtn_label"] + "</a></b>"
    mt = post_row.get("media_type")
    mf = post_row.get("media_file_id")
    bot = _bot_instance
    chat_id = ch["chat_id"]
    try:
        import json as _jspub
        sent = None
        if mt == "album" and post_row.get("media_files_json"):
            from aiogram.types import InputMediaPhoto, InputMediaVideo, InputMediaDocument
            files = _jspub.loads(post_row["media_files_json"])
            media = []
            for i,f in enumerate(files[:10]):
                caption = _safe_caption(text) if i==0 else None
                if f["type"]=="photo":
                    media.append(InputMediaPhoto(media=f["file_id"], caption=caption, parse_mode="HTML"))
                elif f["type"]=="document":
                    media.append(InputMediaDocument(media=f["file_id"], caption=caption, parse_mode="HTML"))
                else:
                    media.append(InputMediaVideo(media=f["file_id"], caption=caption, parse_mode="HTML"))
            sent_list = await bot.send_media_group(chat_id, media)
            sent = sent_list[0] if sent_list else None
        elif mt == "photo" and mf:
            sent = await bot.send_photo(chat_id, mf, caption=_safe_caption(text), parse_mode="HTML")
        elif mt in ("video","animation") and mf:
            sent = await bot.send_video(chat_id, mf, caption=_safe_caption(text), parse_mode="HTML")
        elif mt == "document" and mf:
            sent = await bot.send_document(chat_id, mf, caption=_safe_caption(text), parse_mode="HTML")
        else:
            if text: sent = await bot.send_message(chat_id, _safe_text(text), parse_mode="HTML")
        if not sent and not text:
            log.warning(f"Post {post_id} has no media and no text — skipping")
            return _j({"ok": False, "error": "empty post"})
        await update_post_status(post_id, "published")
        if sent:
            await save_last_published(post_row["channel_id"], sent.message_id)
        # Smart parse: if queue is now low — trigger parse in background
        from services import count_pending_posts, parse_channel_sources
        remaining = await count_pending_posts(post_row["channel_id"])
        if remaining <= 1:
            asyncio.create_task(parse_channel_sources(post_row["channel_id"]))
        return _j({"ok":True,"action":"published"})
    except Exception as e:
        log.error(f"publish error: {e}")
        return _web.json_response({"error":str(e)}, status=500)

@_auth
async def api_sources(request):
    from services import get_user_channels, get_channel_sources, DB_PATH, _fetchall
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    sources = await get_channel_sources(ch_id)
    result = []
    for s in sources:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            pats = await _fetchall(db, "SELECT * FROM source_patterns WHERE source_id=?", (s["id"],))
        result.append({**s, "patterns":[{**p,"auto":bool(p.get("auto",0))} for p in pats],
                       "signature":s.get("promo_signature","")})
    return _j(result)

@_auth
async def api_source_add(request):
    from services import (get_user_channels, add_source,
                          detect_and_save_signature_for_new_source, parse_channel_sources)
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    username = body.get("username","").strip()  # can be URL or @handle
    if not username:
        return _web.json_response({"error":"username required"}, status=400)
    result = await add_source(ch_id, username)
    src_id = result["id"]
    join_status = result.get("join_status")
    if join_status == "pending":
        return _j({"ok":True,"id":src_id,"join_status":"pending",
                    "message":"Заявку на вступ надіслано. Очікуємо підтвердження."})
    if join_status == "expired":
        return _j({"ok":False,"error":"invite_expired",
                    "message":"Посилання-запрошення закінчилося або відкликане."})
    if join_status == "error":
        return _j({"ok":False,"error":"join_error",
                    "message":"Не вдалося приєднатися до каналу за цим посиланням."})
    import asyncio
    async def _detect_then_parse():
        try:
            await detect_and_save_signature_for_new_source(src_id, username)
        except Exception as _e:
            log.warning(f"signature detect failed for {username}: {_e}")
        try:
            await parse_channel_sources(ch_id)
        except Exception as _e:
            log.warning(f"parse after source add failed ch={ch_id}: {_e}")
    asyncio.create_task(_detect_then_parse())
    return _j({"ok":True,"id":src_id,"join_status":join_status or "joined","detecting":True})

@_auth
async def api_source_delete(request):
    from services import get_user_channels, get_source, delete_source
    tg_id = request["tg_user"]["id"]
    src_id = int(request.match_info["src_id"])
    src = await get_source(src_id)
    if not src: return _web.json_response({"error":"not found"}, status=404)
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==src["channel_id"] for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    await delete_source(src_id)
    return _j({"ok":True})

@_auth
async def api_channel_reprocess_sig(request):
    """Force re-apply signature cut to ALL pending posts of this channel."""
    from services import get_user_channels, get_channel_sources, _get_source_signature, reprocess_pending_with_signature
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==ch_id for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    sources = await get_channel_sources(ch_id)
    reprocessed = 0
    for src in sources:
        sig = await _get_source_signature(src["id"])
        if sig:
            await reprocess_pending_with_signature(ch_id, src["id"], sig)
            reprocessed += 1
    return _j({"ok": True, "sources_reprocessed": reprocessed})

@_auth
async def api_source_reset_sig(request):
    from services import (get_user_channels, get_source, DB_PATH,
                          detect_and_save_signature_for_new_source)
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    src_id = int(request.match_info["src_id"])
    src = await get_source(src_id)
    if not src: return _web.json_response({"error":"not found"}, status=404)
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==src["channel_id"] for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("UPDATE sources SET promo_signature=NULL WHERE id=?", (src_id,))
        await db.commit()
    import asyncio
    asyncio.create_task(detect_and_save_signature_for_new_source(src_id, src["username"]))
    return _j({"ok":True})

@_auth
async def api_pattern_add(request):
    from services import get_user_channels, get_source, DB_PATH
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    src_id = int(request.match_info["src_id"])
    src = await get_source(src_id)
    if not src: return _web.json_response({"error":"not found"}, status=404)
    user_chs = await get_user_channels(tg_id)
    if not any(c["id"]==src["channel_id"] for c in user_chs):
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    pattern = body.get("pattern","").strip()
    if not pattern: return _web.json_response({"error":"pattern required"}, status=400)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        cur = await db.execute(
            "INSERT INTO source_patterns(source_id, pattern, auto) VALUES(?,?,0)",
            (src_id, pattern))
        await db.commit()
        return _j({"ok":True,"id":cur.lastrowid})

@_auth
async def api_pattern_delete(request):
    from services import DB_PATH
    import aiosqlite
    pat_id = int(request.match_info["pat_id"])
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute("DELETE FROM source_patterns WHERE id=?", (pat_id,))
        await db.commit()
    return _j({"ok":True})

@_auth
async def api_finance(request):
    from services import get_user, get_user_balance, DB_PATH, _fetchall
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    user = await get_user(tg_id)
    if not user: return _web.json_response({"error":"not found"}, status=404)
    balance = await get_user_balance(tg_id)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        txs = await _fetchall(db,
            "SELECT * FROM transactions WHERE user_id=? ORDER BY created_at DESC LIMIT 30",
            (user["id"],))
    return _j({"balance":round(balance,2),"transactions":txs})

@_auth
async def api_referral(request):
    from services import get_user, DB_PATH, _fetchall, _fetchone, get_or_create_referral_code
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    user = await get_user(tg_id)
    if not user: return _web.json_response({"error":"not found"}, status=404)
    code = await get_or_create_referral_code(tg_id)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        refs = await _fetchall(db,
            "SELECT u.username, u.first_name, u.telegram_id, u.created_at, "
            "r.status, COALESCE(r.bonus_amount, 0.0) as bonus_amount "
            "FROM referrals r JOIN users u ON u.id = r.referred_id "
            "WHERE r.referrer_id = ?", (user["id"],))
        total = len(refs)
        earned_row = await _fetchone(db,
            "SELECT COALESCE(SUM(bonus_amount),0) as e FROM referrals WHERE referrer_id=? AND status='paid'",
            (user["id"],))
    earned = float(earned_row["e"]) if earned_row else 0.0
    return _j({"code":code,"stats":{"total":total,"earned":earned},"referrals":refs})

SLOT_PRICE = float(__import__('os').getenv("SLOT_PRICE","2.0"))

@_auth
async def api_pay_config(request):
    from services import MANAGER_USERNAME, CRYPTO_TOKEN
    return _j({"price_per_slot":SLOT_PRICE,"manager":MANAGER_USERNAME,"crypto_available":bool(CRYPTO_TOKEN)})

@_auth
async def api_pay_crypto(request):
    from services import create_invoice, get_user, BASE_PRICE, get_user_channels
    tg_id = request["tg_user"]["id"]
    body = await request.json()
    ch_id = body.get("channel_id")  # May be None for balance top-up
    days = int(body.get("days",30))
    amount = float(body.get("amount", BASE_PRICE))
    if amount < 0.5: amount = BASE_PRICE
    user = await get_user(tg_id)
    if not user: return _web.json_response({"error":"user not found"}, status=404)
    if ch_id and int(ch_id) != 0:
        channels = await get_user_channels(tg_id)
        if not any(c["id"]==int(ch_id) for c in channels):
            return _web.json_response({"error":"forbidden"}, status=403)
    # channel_id=0 means "balance top-up" (no specific channel)
    invoice = await create_invoice(amount, int(ch_id) if ch_id else 0, days, user["id"])
    if not invoice: return _web.json_response({"error":"CryptoBot error"}, status=500)
    return _j({"ok":True,"pay_url":invoice["pay_url"],"invoice_id":invoice["invoice_id"],"amount":amount})

@_auth
async def api_pay_check(request):
    from services import check_invoice_status
    invoice_id = request.match_info["invoice_id"]
    status = await check_invoice_status(invoice_id)
    return _j({"status":status,"paid":status=="paid"})

@_auth
async def api_pay_from_balance(request):
    from services import (get_user, get_user_balance, adjust_user_balance,
                          activate_subscription, get_user_channels, DB_PATH)
    import aiosqlite
    tg_id = request["tg_user"]["id"]
    body = await request.json()
    ch_id = body.get("channel_id")
    days = int(body.get("days",30))
    price = SLOT_PRICE * (days/30)
    if not ch_id:
        return _web.json_response({"error":"no_channel","message":"Выберите канал"}, status=400)
    user = await get_user(tg_id)
    if not user: return _web.json_response({"error":"user not found"}, status=404)
    channels = await get_user_channels(tg_id)
    ch = next((c for c in channels if c["id"]==int(ch_id)), None)
    if not ch: return _web.json_response({"error":"forbidden"}, status=403)
    balance = await get_user_balance(tg_id)
    if balance < price:
        return _web.json_response({"error":"insufficient_balance",
            "message":f"Недостаточно средств: ${balance:.2f} < ${price:.2f}",
            "balance":balance,"required":price}, status=400)
    new_bal = await adjust_user_balance(tg_id, -price)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO transactions(user_id, amount, type, description) VALUES(?,?,?,?)",
            (user["id"], -price, "subscription", f"Подписка @{ch.get('username') or ch_id} · {days}д"))
        await db.commit()
    await activate_subscription(int(ch_id), days)
    # Trigger referral bonus
    try:
        from services import process_referral_bonus
        await process_referral_bonus(tg_id, int(ch_id), price)
    except Exception as e:
        log.warning(f"referral bonus error: {e}")
    return _j({"ok":True,"new_balance":round(new_bal,2),"days":days})

@_auth
async def api_admin_stats(request):
    from services import ADMIN_IDS, get_admin_stats
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    return _j(await get_admin_stats())

@_auth
async def api_admin_users(request):
    from services import ADMIN_IDS, get_user_balance, DB_PATH, _fetchall, search_users
    import aiosqlite
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    # Support search query
    q = request.rel_url.query.get("q", "").strip()
    if q:
        users = await search_users(q)
    else:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            users = await _fetchall(db, "SELECT * FROM users ORDER BY created_at DESC LIMIT 50")
    result = []
    for u in users:
        bal = await get_user_balance(u["telegram_id"])
        result.append({**u,"balance":round(bal,2)})
    return _j(result)

@_auth
async def api_admin_user_get(request):
    from services import ADMIN_IDS, get_user, get_user_balance, get_user_channels, get_channel_settings, DB_PATH, _fetchone
    import aiosqlite
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    raw = request.match_info["tg_id"].strip().lstrip("@")
    # Try as telegram_id (number) first, then as username
    user = None
    if raw.isdigit():
        user = await get_user(int(raw))
    if not user:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            user = await _fetchone(db, "SELECT * FROM users WHERE LOWER(username)=LOWER(?)", (raw,))
    if not user: return _web.json_response({"error":"not found"}, status=404)
    tg_id = user["telegram_id"]
    balance = await get_user_balance(tg_id)
    channels = await get_user_channels(tg_id)
    # Enrich channels with settings
    enriched_channels = []
    for ch in channels:
        s = await get_channel_settings(ch["id"])
        enriched_channels.append({**ch, "settings": s})
    return _j({**user, "balance": round(balance, 2), "channels": enriched_channels})

@_auth
async def api_admin_balance(request):
    from services import ADMIN_IDS, get_user, adjust_user_balance, DB_PATH
    import aiosqlite
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    tg_id = body.get("telegram_id")
    delta = float(body.get("delta",0))
    note = body.get("note","Ручное изменение (admin)")
    user = await get_user(int(tg_id))
    if not user: return _web.json_response({"error":"user not found"}, status=404)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        await db.execute(
            "INSERT INTO transactions(user_id,amount,type,description) VALUES(?,?,?,?)",
            (user["id"],delta,"admin_adjust",note))
        await db.commit()
    new_bal = await adjust_user_balance(int(tg_id), delta)
    return _j({"ok":True,"new_balance":round(new_bal,2),"telegram_id":tg_id})

@_auth
async def api_admin_setsub(request):
    """Admin: set subscription status + days for any channel."""
    from services import ADMIN_IDS, DB_PATH
    import aiosqlite
    from datetime import datetime, timedelta, timezone
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    ch_id = int(body.get("channel_id", 0))
    status = body.get("status","active")
    days = int(body.get("days", 30))
    if not ch_id:
        return _web.json_response({"error":"channel_id required"}, status=400)
    if status not in ("active","trial","restricted","pending"):
        return _web.json_response({"error":"invalid status"}, status=400)
    end_dt = (datetime.now(timezone.utc) + timedelta(days=days)).replace(tzinfo=None).isoformat()
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        # Check channel exists FIRST
        row = await db.execute("SELECT id FROM channels WHERE id=?", (ch_id,))
        if not await row.fetchone():
            return _web.json_response({"error":"channel not found"}, status=404)
        await db.execute(
            "UPDATE channels SET subscription_status=?, subscription_end=? WHERE id=?",
            (status, end_dt if status in ("active","trial") else None, ch_id))
        # If restricting — disable autopost in settings JSON
        if status in ("restricted","pending"):
            settings_row = await db.execute("SELECT settings FROM channels WHERE id=?", (ch_id,))
            settings_row = await settings_row.fetchone()
            if settings_row and settings_row[0]:
                import json as _jj
                s = _jj.loads(settings_row[0] or "{}")
                s["autopost_enabled"] = False
                await db.execute("UPDATE channels SET settings=? WHERE id=?",
                    (_jj.dumps(s, ensure_ascii=False), ch_id))
        await db.commit()
    return _j({"ok":True,"channel_id":ch_id,"status":status,"days":days})

@_auth
async def api_admin_broadcast(request):
    """Admin: broadcast message to all users."""
    from services import ADMIN_IDS, DB_PATH, _fetchall, _bot_instance
    import aiosqlite
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    text = body.get("text","").strip()
    if not text:
        return _web.json_response({"error":"text required"}, status=400)
    async with aiosqlite.connect(DB_PATH, timeout=10) as db:
        users = await _fetchall(db, "SELECT telegram_id FROM users WHERE is_blocked=0")
    sent = errors = 0
    for u in users:
        try:
            await _bot_instance.send_message(u["telegram_id"], text, parse_mode="HTML")
            sent += 1
            await asyncio.sleep(0.05)  # rate limit ~20/sec
        except Exception:
            errors += 1
    return _j({"ok":True,"sent":sent,"errors":errors})

@_auth
async def api_channel_photo(request):
    import httpx as _hxp
    from services import get_user_channels, _bot_instance
    from urllib.parse import unquote as _unqp
    tg_id = request["tg_user"]["id"]
    ch_id = int(request.match_info["id"])
    channels = await get_user_channels(tg_id)
    ch = next((c for c in channels if c["id"]==ch_id), None)
    if not ch: return _web.json_response({"error":"forbidden"}, status=403)
    try:
        chat = await _bot_instance.get_chat(ch["chat_id"])
        if chat.photo:
            file = await _bot_instance.get_file(chat.photo.small_file_id)
            url = f"https://api.telegram.org/file/bot{_bot_instance.token}/{file.file_path}"
            async with _hxp.AsyncClient() as client:
                r = await client.get(url, timeout=10)
            return _web.Response(body=r.content, content_type="image/jpeg",
                headers={"Cache-Control":"public, max-age=3600"})
    except Exception as e:
        log.warning(f"Channel photo error: {e}")
    return _web.json_response({"error":"no photo"}, status=404)

async def _cors_middleware(app, handler):
    async def middleware(request):
        if request.method == "OPTIONS":
            return _web.Response(headers={"Access-Control-Allow-Origin":"*",
                "Access-Control-Allow-Headers":"Content-Type,X-Init-Data,X-Dev-Id",
                "Access-Control-Allow-Methods":"GET,POST,DELETE,OPTIONS"})
        try:
            resp = await handler(request)
        except _web.HTTPException:
            raise
        except json.JSONDecodeError:
            resp = _web.json_response({"error": "Invalid JSON"}, status=400)
        except (ValueError, TypeError) as e:
            resp = _web.json_response({"error": str(e)}, status=400)
        except Exception as e:
            log.exception(f"Unhandled error in {request.method} {request.path}: {e}")
            resp = _web.json_response({"error": "Internal server error"}, status=500)
        resp.headers.setdefault("Access-Control-Allow-Origin","*")
        return resp
    return middleware

@_auth
async def api_set_language(request):
    """Set user interface language (ru/uk)."""
    from services import set_user_language
    tg_id = request["tg_user"]["id"]
    body = await request.json()
    lang = body.get("lang", "ru")
    await set_user_language(tg_id, lang)
    return _j({"ok": True, "lang": lang})

@_auth
async def api_set_notifications(request):
    """Set user notification preference."""
    from services import set_user_notifications
    tg_id = request["tg_user"]["id"]
    body = await request.json()
    enabled = body.get("enabled", True)
    await set_user_notifications(tg_id, enabled)
    return _j({"ok": True, "enabled": enabled})

@_auth
async def api_check_subscriptions(request):
    """Check if user is subscribed to required channels."""
    from services import check_user_channel_subscriptions, _bot_instance, REQUIRED_CHANNELS
    tg_id = request["tg_user"]["id"]
    if not _bot_instance or not REQUIRED_CHANNELS:
        return _j({"ok": True, "channels": []})
    result = await check_user_channel_subscriptions(_bot_instance, tg_id)
    return _j(result)

@_auth
async def api_admin_all_channels(request):
    """Admin: list all channels with owner info."""
    from services import ADMIN_IDS, get_all_channels_with_owners, get_channel_settings
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    channels = await get_all_channels_with_owners()
    result = []
    for ch in channels:
        s = await get_channel_settings(ch["id"])
        result.append({**ch, "settings": s})
    return _j(result)

@_auth
async def api_admin_user_block(request):
    """Admin: block/unblock user."""
    from services import ADMIN_IDS, set_user_blocked
    if request["tg_user"]["id"] not in ADMIN_IDS:
        return _web.json_response({"error":"forbidden"}, status=403)
    body = await request.json()
    tg_id = int(body.get("telegram_id", 0))
    blocked = bool(body.get("blocked", False))
    await set_user_blocked(tg_id, blocked)
    return _j({"ok": True, "telegram_id": tg_id, "blocked": blocked})

async def api_media_stream(request):
    """Redirect to Telegram file URL directly — avoids loading video into RAM (OOM prevention)."""
    from services import _bot_instance
    from urllib.parse import unquote as _unqs
    file_id = _unqs(request.match_info["file_id"])
    if not file_id:
        return _web.Response(body=b"", status=404)
    try:
        file = await _bot_instance.get_file(file_id)
        if not file or not file.file_path:
            return _web.Response(body=b"", status=404)
        tg_url = f"https://api.telegram.org/file/bot{_bot_instance.token}/{file.file_path}"
        return _web.Response(status=302, headers={"Location": tg_url})
    except Exception as e:
        log.debug(f"media_stream error: {e}")
        return _web.Response(body=b"", status=404)

async def api_media_thumb(request):
    """Proxy media thumbnail. NO AUTH. Only photos (AgAC...), blocks videos (BAAC...) to prevent OOM."""
    import httpx as _hxt
    from services import _bot_instance
    from urllib.parse import unquote as _unq
    file_id = _unq(request.match_info["file_id"])
    if not file_id:
        return _web.Response(body=b"", status=204)
    # BAAC... = video/document — large files, skip to prevent OOM kill
    if file_id.startswith("BAAC"):
        return _web.Response(body=b"", status=204)
    try:
        file = await _bot_instance.get_file(file_id)
        if not file or not file.file_path:
            return _web.Response(body=b"", status=204)
        if file.file_size and file.file_size > 5 * 1024 * 1024:
            return _web.Response(body=b"", status=204)
        url = f"https://api.telegram.org/file/bot{_bot_instance.token}/{file.file_path}"
        async with _hxt.AsyncClient(timeout=10) as _c:
            r = await _c.get(url)
        if r.status_code == 200 and 100 < len(r.content) < 10 * 1024 * 1024:
            ct = r.headers.get("content-type", "image/jpeg")
            return _web.Response(body=r.content, content_type=ct,
                headers={"Cache-Control": "public, max-age=3600"})
        return _web.Response(body=b"", status=204)
    except Exception as e:
        log.debug(f"media_thumb {file_id[:20]}: {type(e).__name__}")
        return _web.Response(body=b"", status=204)

@_auth
async def api_marketplace(request):
    from services import get_marketplace_channels
    channels = await get_marketplace_channels()
    return _j(channels)

async def api_marketplace_photo(request):
    """Public endpoint — returns channel avatar for marketplace cards."""
    from services import _bot_instance as _mp_bot
    import aiosqlite
    from services import DB_PATH, _fetchone
    ch_id = int(request.match_info["id"])
    try:
        async with aiosqlite.connect(DB_PATH, timeout=10) as db:
            row = await _fetchone(db, "SELECT chat_id, settings FROM channels WHERE id=?", (ch_id,))
        if not row:
            return _web.json_response({"error": "not found"}, status=404)
        import json as _json_mp
        s = _json_mp.loads(row.get("settings") or "{}")
        if not s.get("is_listed"):
            return _web.json_response({"error": "not listed"}, status=403)
        chat = await _mp_bot.get_chat(row["chat_id"])
        if chat.photo:
            file = await _mp_bot.get_file(chat.photo.small_file_id)
            url = f"https://api.telegram.org/file/bot{_mp_bot.token}/{file.file_path}"
            async with aiohttp.ClientSession() as sess:
                async with sess.get(url, timeout=aiohttp.ClientTimeout(total=10)) as resp:
                    data = await resp.read()
            return _web.Response(body=data, content_type="image/jpeg",
                headers={"Cache-Control": "public, max-age=3600"})
    except Exception as e:
        log.warning(f"Marketplace photo error: {e}")
    return _web.json_response({"error": "no photo"}, status=404)

async def start_webapp():
    app = _web.Application(middlewares=[_cors_middleware])
    app.router.add_get("/favicon.ico", lambda r: _web.Response(status=204))
    app.router.add_get("/",           _serve_miniapp)
    app.router.add_get("/miniapp",    _serve_miniapp)
    # API
    app.router.add_get   ("/api/me",                       api_me)
    app.router.add_get   ("/api/channels",                 api_channels)
    app.router.add_post  ("/api/channel/add",              api_channel_add)
    app.router.add_post  ("/api/channel/{id}/parse",       api_channel_parse)
    app.router.add_get   ("/api/channel/{id}",             api_channel_get)
    app.router.add_post  ("/api/channel/{id}/settings",    api_channel_settings_save)
    app.router.add_delete("/api/channel/{id}",             api_channel_delete)
    app.router.add_get   ("/api/channel/{id}/queue",       api_queue)
    app.router.add_post  ("/api/queue/{post_id}/action",   api_queue_action)
    app.router.add_get   ("/api/channel/{id}/sources",     api_sources)
    app.router.add_post  ("/api/channel/{id}/sources",     api_source_add)
    app.router.add_delete("/api/source/{src_id}",          api_source_delete)
    app.router.add_post  ("/api/source/{src_id}/patterns", api_pattern_add)
    app.router.add_post  ("/api/source/{src_id}/reset_sig",api_source_reset_sig)
    app.router.add_post  ("/api/channel/{id}/reprocess_sig", api_channel_reprocess_sig)
    app.router.add_delete("/api/pattern/{pat_id}",         api_pattern_delete)
    app.router.add_get   ("/api/finance",                  api_finance)
    app.router.add_get   ("/api/referral",                 api_referral)
    app.router.add_get   ("/api/admin/stats",              api_admin_stats)
    app.router.add_get   ("/api/admin/users",              api_admin_users)
    app.router.add_post  ("/api/admin/balance",            api_admin_balance)
    app.router.add_post  ("/api/admin/setsub",             api_admin_setsub)
    app.router.add_post  ("/api/admin/broadcast",          api_admin_broadcast)
    app.router.add_get   ("/api/admin/user/{tg_id}",       api_admin_user_get)
    app.router.add_post  ("/api/pay/crypto",               api_pay_crypto)
    app.router.add_get   ("/api/pay/check/{invoice_id}",   api_pay_check)
    app.router.add_post  ("/api/pay/from_balance",         api_pay_from_balance)
    app.router.add_get   ("/api/pay/config",               api_pay_config)
    app.router.add_get   ("/api/media/{file_id}/thumb",    api_media_thumb)
    app.router.add_get   ("/api/media/{file_id}/stream",   api_media_stream)
    app.router.add_get   ("/api/channel/{id}/photo",       api_channel_photo)
    # New endpoints
    app.router.add_post  ("/api/set_language",              api_set_language)
    app.router.add_post  ("/api/set_notifications",         api_set_notifications)
    app.router.add_get   ("/api/check_subscriptions",       api_check_subscriptions)
    app.router.add_get   ("/api/admin/channels",            api_admin_all_channels)
    app.router.add_post  ("/api/admin/user/block",          api_admin_user_block)
    app.router.add_get   ("/api/marketplace",               api_marketplace)
    app.router.add_get   ("/api/marketplace/photo/{id}",    api_marketplace_photo)
    runner = _web.AppRunner(app)
    await runner.setup()
    site = _web.TCPSite(runner, "0.0.0.0", 8080)
    await site.start()
    log.info("Mini App + API running on :8080")


async def main():
    setup_logging()
    log.info("Starting AutoPost Bot...")
    await init_db()
    await reset_stale_awaiting_confirm()
    log.info("DB ready")

    bot = Bot(token=BOT_TOKEN, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dp  = Dispatcher(storage=MemoryStorage())
    dp.include_router(router)

    set_bot_instance(bot)
    log.info("Bot instance registered")

    scheduler = AsyncIOScheduler()
    scheduler.add_job(run_parse,           "interval", minutes=PARSE_INTERVAL_MINUTES, id="run_parse",        args=[bot])
    scheduler.add_job(run_autopost,        "interval", minutes=1,  id="run_autopost",   args=[bot])
    scheduler.add_job(check_subscriptions, "interval", hours=1,    id="check_subscriptions")
    scheduler.add_job(run_expire,          "interval", minutes=10, id="run_expire")
    scheduler.add_job(run_autorenew,       "interval", minutes=30, id="run_autorenew")
    scheduler.add_job(run_midnight,        trigger=CronTrigger(hour=0, minute=0), id="run_midnight", args=[bot])
    scheduler.add_job(run_media_cleanup,   "interval", hours=1,    id="run_media_cleanup")
    scheduler.add_job(run_check_join_requests, "interval", minutes=5, id="run_check_joins")
    scheduler.start()
    log.info(f"Scheduler started (parse every {PARSE_INTERVAL_MINUTES}min, cleanup at 00:00 UTC)")

    log.info("Bot polling started")
    await start_webapp()

    await dp.start_polling(bot, allowed_updates=["message", "callback_query"])


if __name__ == "__main__":
    asyncio.run(main())
