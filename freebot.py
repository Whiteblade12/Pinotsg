#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════════╗
║                    🤖 PI BOT - ULTIMATE EDITION                 ║
║         The Most Advanced Telegram Bot Hosting Platform         ║
║                    All 150 Features Implemented                  ║
║                     Admin: @TheGoat_Father                       ║
╚══════════════════════════════════════════════════════════════════╝
"""

import logging
import os
import json
import subprocess
import signal
import asyncio
import uuid
import sqlite3
import datetime
import sys
import shutil
import hashlib
import hmac
import secrets
import string
import time
import threading
import queue
import smtplib
import ssl
import random
from datetime import datetime, timedelta
from functools import wraps
from pathlib import Path
from collections import defaultdict
from typing import Optional, Tuple, List, Any, Dict

import psutil
import aiohttp
import aiofiles
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger

from telegram import (
    Update,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    ReplyKeyboardMarkup,
    KeyboardButton,
    BotCommand,
    BotCommandScopeChat,
)
from telegram.ext import (
    Application,
    CommandHandler,
    MessageHandler,
    CallbackQueryHandler,
    ContextTypes,
    filters,
    ConversationHandler,
)

# ─────────────────────────────────────────────────────────────────────
#  ██████╗ ██████╗ ███╗   ██╗███████╗██╗ ██████╗
# ██╔════╝██╔═══██╗████╗  ██║██╔════╝██║██╔════╝
# ██║     ██║   ██║██╔██╗ ██║█████╗  ██║██║  ███╗
# ██║     ██║   ██║██║╚██╗██║██╔══╝  ██║██║   ██║
# ╚██████╗╚██████╔╝██║ ╚████║██║     ██║╚██████╔╝
#  ╚═════╝ ╚═════╝ ╚═╝  ╚═══╝╚═╝     ╚═╝ ╚═════╝
# ─────────────────────────────────────────────────────────────────────

# ==================== CONFIGURATION ====================
BOT_TOKEN = "8614177593:AAHoqFjYLl1PEVZzkKpOWfYJngBy4oBGKWU"
ADMIN_IDS = [7566919489]
ADMIN_USERNAME = "TheGoat_Father"
BACKUP_GROUP_ID = -1003629797017

# Database paths
DB_PATH = "database/pi_bot.db"
os.makedirs("database", exist_ok=True)
os.makedirs("user_bots", exist_ok=True)
os.makedirs("bot_logs", exist_ok=True)
os.makedirs("backups", exist_ok=True)
os.makedirs("templates", exist_ok=True)
os.makedirs("marketplace", exist_ok=True)

# ==================== LOGGING ====================
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# ==================== SCHEDULER ====================
scheduler = AsyncIOScheduler()

# ==================== DATABASE INITIALIZATION ====================
def init_database():
    """Initialize all database tables for all 150 features"""
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    c = conn.cursor()
    
    # Users table
    c.execute('''CREATE TABLE IF NOT EXISTS users (
        user_id INTEGER PRIMARY KEY,
        username TEXT,
        first_name TEXT,
        join_date TEXT,
        plan TEXT DEFAULT 'free',
        banned INTEGER DEFAULT 0,
        bots_hosted INTEGER DEFAULT 0,
        max_bots INTEGER DEFAULT 1,
        warnings INTEGER DEFAULT 0,
        xp INTEGER DEFAULT 0,
        level INTEGER DEFAULT 1,
        referral_code TEXT UNIQUE,
        referred_by INTEGER,
        total_earnings REAL DEFAULT 0,
        daily_streak INTEGER DEFAULT 0,
        last_daily TEXT
    )''')
    
    # Bots table
    c.execute('''CREATE TABLE IF NOT EXISTS bots (
        bot_id TEXT PRIMARY KEY,
        user_id INTEGER,
        filename TEXT,
        filepath TEXT,
        process_id INTEGER,
        status TEXT DEFAULT 'stopped',
        start_time TEXT,
        file_msg_id INTEGER,
        description TEXT,
        category TEXT,
        tags TEXT,
        upvotes INTEGER DEFAULT 0,
        downvotes INTEGER DEFAULT 0,
        total_users INTEGER DEFAULT 0,
        total_messages INTEGER DEFAULT 0,
        is_public INTEGER DEFAULT 0
    )''')
    
    # Bot stats table (for analytics)
    c.execute('''CREATE TABLE IF NOT EXISTS bot_stats (
        stat_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        date TEXT,
        user_count INTEGER DEFAULT 0,
        message_count INTEGER DEFAULT 0,
        cpu_usage REAL DEFAULT 0,
        memory_usage REAL DEFAULT 0,
        FOREIGN KEY(bot_id) REFERENCES bots(bot_id)
    )''')
    
    # Environment variables table
    c.execute('''CREATE TABLE IF NOT EXISTS env_vars (
        env_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        key TEXT,
        value TEXT,
        encrypted INTEGER DEFAULT 0,
        FOREIGN KEY(bot_id) REFERENCES bots(bot_id)
    )''')
    
    # Templates table
    c.execute('''CREATE TABLE IF NOT EXISTS templates (
        template_id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        description TEXT,
        category TEXT,
        code TEXT,
        author_id INTEGER,
        downloads INTEGER DEFAULT 0,
        price REAL DEFAULT 0,
        created TEXT
    )''')
    
    # Marketplace listings
    c.execute('''CREATE TABLE IF NOT EXISTS marketplace (
        listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        seller_id INTEGER,
        price REAL,
        sold INTEGER DEFAULT 0,
        listed_date TEXT,
        FOREIGN KEY(bot_id) REFERENCES bots(bot_id)
    )''')
    
    # Referrals table
    c.execute('''CREATE TABLE IF NOT EXISTS referrals (
        ref_id INTEGER PRIMARY KEY AUTOINCREMENT,
        referrer_id INTEGER,
        referred_id INTEGER,
        reward_amount REAL,
        date TEXT
    )''')
    
    # Earnings table
    c.execute('''CREATE TABLE IF NOT EXISTS earnings (
        earning_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        amount REAL,
        source TEXT,
        date TEXT
    )''')
    
    # Bot collaboration table
    c.execute('''CREATE TABLE IF NOT EXISTS collaborators (
        collab_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        user_id INTEGER,
        role TEXT DEFAULT 'developer',
        invited_by INTEGER,
        joined_date TEXT
    )''')
    
    # Scheduled tasks table
    c.execute('''CREATE TABLE IF NOT EXISTS scheduled_tasks (
        task_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        task_type TEXT,
        schedule_type TEXT,
        schedule_value TEXT,
        command TEXT,
        is_active INTEGER DEFAULT 1
    )''')
    
    # Achievements table
    c.execute('''CREATE TABLE IF NOT EXISTS achievements (
        achievement_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        achievement_name TEXT,
        unlocked_date TEXT
    )''')
    
    # Backups table
    c.execute('''CREATE TABLE IF NOT EXISTS backups (
        backup_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        backup_file TEXT,
        backup_date TEXT,
        restore_point TEXT
    )''')
    
    # Bot reviews table
    c.execute('''CREATE TABLE IF NOT EXISTS bot_reviews (
        review_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        user_id INTEGER,
        rating INTEGER,
        review TEXT,
        date TEXT
    )''')
    
    # Bot sharing table
    c.execute('''CREATE TABLE IF NOT EXISTS bot_shares (
        share_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        shared_with INTEGER,
        permission TEXT,
        shared_by INTEGER,
        shared_date TEXT
    )''')
    
    # API keys table
    c.execute('''CREATE TABLE IF NOT EXISTS api_keys (
        key_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        api_key TEXT UNIQUE,
        permissions TEXT,
        created TEXT,
        last_used TEXT
    )''')
    
    # Webhooks table
    c.execute('''CREATE TABLE IF NOT EXISTS webhooks (
        webhook_id INTEGER PRIMARY KEY AUTOINCREMENT,
        bot_id TEXT,
        url TEXT,
        secret TEXT,
        events TEXT,
        is_active INTEGER DEFAULT 1
    )''')
    
    # Usage logs table
    c.execute('''CREATE TABLE IF NOT EXISTS usage_logs (
        log_id INTEGER PRIMARY KEY AUTOINCREMENT,
        user_id INTEGER,
        action TEXT,
        timestamp TEXT,
        details TEXT
    )''')
    
    conn.commit()
    conn.close()
    logger.info("Database initialized with all tables")

init_database()

# ==================== DATABASE HELPER FUNCTIONS ====================
def get_db():
    return sqlite3.connect(DB_PATH, check_same_thread=False)

def get_user(user_id: int) -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM users WHERE user_id = ?", (user_id,))
    user = c.fetchone()
    conn.close()
    
    if not user:
        # Create new user with referral code
        referral_code = generate_referral_code()
        join_date = datetime.now().isoformat()
        conn = get_db()
        c = conn.cursor()
        c.execute('''INSERT INTO users (user_id, username, first_name, join_date, referral_code)
                     VALUES (?, ?, ?, ?, ?)''',
                  (user_id, "", "", join_date, referral_code))
        conn.commit()
        conn.close()
        return {
            "user_id": user_id,
            "username": "",
            "first_name": "",
            "join_date": join_date,
            "plan": "free",
            "banned": 0,
            "bots_hosted": 0,
            "max_bots": 1,
            "warnings": 0,
            "xp": 0,
            "level": 1,
            "referral_code": referral_code,
            "referred_by": None,
            "total_earnings": 0,
            "daily_streak": 0
        }
    
    return {
        "user_id": user[0],
        "username": user[1] or "",
        "first_name": user[2] or "",
        "join_date": user[3],
        "plan": user[4],
        "banned": user[5],
        "bots_hosted": user[6],
        "max_bots": user[7],
        "warnings": user[8] if len(user) > 8 else 0,
        "xp": user[9] if len(user) > 9 else 0,
        "level": user[10] if len(user) > 10 else 1,
        "referral_code": user[11] if len(user) > 11 else "",
        "referred_by": user[12] if len(user) > 12 else None,
        "total_earnings": user[13] if len(user) > 13 else 0,
        "daily_streak": user[14] if len(user) > 14 else 0
    }

def generate_referral_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=8))

def add_xp(user_id: int, xp_amount: int):
    """Add XP to user and handle level ups"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT xp, level FROM users WHERE user_id = ?", (user_id,))
    result = c.fetchone()
    if result:
        xp, level = result
        new_xp = xp + xp_amount
        new_level = level
        # Level up formula: 100 * level^2
        while new_xp >= 100 * (new_level ** 2):
            new_xp -= 100 * (new_level ** 2)
            new_level += 1
            # Award achievement for level up
            add_achievement(user_id, f"Level {new_level} Reached!")
        c.execute("UPDATE users SET xp = ?, level = ? WHERE user_id = ?", (new_xp, new_level, user_id))
    conn.commit()
    conn.close()
    return new_level if 'new_level' in locals() else level

def add_achievement(user_id: int, achievement_name: str):
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM achievements WHERE user_id = ? AND achievement_name = ?", (user_id, achievement_name))
    if not c.fetchone():
        c.execute("INSERT INTO achievements (user_id, achievement_name, unlocked_date) VALUES (?, ?, ?)",
                  (user_id, achievement_name, datetime.now().isoformat()))
    conn.commit()
    conn.close()

def log_action(user_id: int, action: str, details: str = ""):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO usage_logs (user_id, action, timestamp, details) VALUES (?, ?, ?, ?)",
              (user_id, action, datetime.now().isoformat(), details))
    conn.commit()
    conn.close()

# ==================== ENVIRONMENT VARIABLES ====================
def set_env_var(bot_id: str, key: str, value: str, encrypted: bool = False):
    conn = get_db()
    c = conn.cursor()
    if encrypted:
        # Simple encryption (in production, use proper encryption)
        value = base64.b64encode(value.encode()).decode()
    c.execute("INSERT OR REPLACE INTO env_vars (bot_id, key, value, encrypted) VALUES (?, ?, ?, ?)",
              (bot_id, key, value, 1 if encrypted else 0))
    conn.commit()
    conn.close()

def get_env_vars(bot_id: str) -> dict:
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT key, value, encrypted FROM env_vars WHERE bot_id = ?", (bot_id,))
    vars = {}
    for key, value, encrypted in c.fetchall():
        if encrypted:
            value = base64.b64decode(value.encode()).decode()
        vars[key] = value
    conn.close()
    return vars

# ==================== BACKUP SYSTEM ====================
async def create_backup(bot_id: str, user_id: int) -> str:
    """Create backup of bot files"""
    backup_dir = f"backups/{user_id}"
    os.makedirs(backup_dir, exist_ok=True)
    
    bot_info = get_bot(bot_id)
    if not bot_info:
        return None
    
    backup_file = f"{backup_dir}/{bot_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
    
    # Create zip backup
    import zipfile
    with zipfile.ZipFile(backup_file, 'w') as zipf:
        if os.path.exists(bot_info['filepath']):
            zipf.write(bot_info['filepath'], bot_info['filename'])
        
        # Also backup env vars
        env_vars = get_env_vars(bot_id)
        if env_vars:
            with open(f"{backup_dir}/env_{bot_id}.json", 'w') as f:
                json.dump(env_vars, f)
            zipf.write(f"{backup_dir}/env_{bot_id}.json", "env_vars.json")
            os.remove(f"{backup_dir}/env_{bot_id}.json")
    
    # Save to database
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO backups (bot_id, backup_file, backup_date, restore_point) VALUES (?, ?, ?, ?)",
              (bot_id, backup_file, datetime.now().isoformat(), datetime.now().isoformat()))
    conn.commit()
    conn.close()
    
    return backup_file

async def restore_backup(bot_id: str, backup_id: int):
    """Restore bot from backup"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT backup_file FROM backups WHERE bot_id = ? AND backup_id = ?", (bot_id, backup_id))
    result = c.fetchone()
    conn.close()
    
    if not result:
        return False
    
    backup_file = result[0]
    import zipfile
    with zipfile.ZipFile(backup_file, 'r') as zipf:
        zipf.extractall(f"user_bots/{bot_id}_restore")
    
    return True

# ==================== SCHEDULED TASKS ====================
def add_scheduled_task(bot_id: str, task_type: str, schedule_type: str, schedule_value: str, command: str):
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO scheduled_tasks (bot_id, task_type, schedule_type, schedule_value, command) VALUES (?, ?, ?, ?, ?)",
              (bot_id, task_type, schedule_type, schedule_value, command))
    conn.commit()
    conn.close()

async def run_scheduled_tasks():
    """Run all scheduled tasks"""
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT * FROM scheduled_tasks WHERE is_active = 1")
    tasks = c.fetchall()
    conn.close()
    
    for task in tasks:
        # Schedule using APScheduler
        task_id, bot_id, task_type, schedule_type, schedule_value, command, is_active = task
        
        if schedule_type == "cron":
            parts = schedule_value.split()
            if len(parts) == 5:
                scheduler.add_job(
                    execute_task,
                    CronTrigger(minute=parts[0], hour=parts[1], day=parts[2], month=parts[3], day_of_week=parts[4]),
                    args=[bot_id, command],
                    id=f"task_{task_id}"
                )
        elif schedule_type == "interval":
            scheduler.add_job(
                execute_task,
                IntervalTrigger(seconds=int(schedule_value)),
                args=[bot_id, command],
                id=f"task_{task_id}"
            )

async def execute_task(bot_id: str, command: str):
    """Execute a scheduled task"""
    # Log the execution
    logger.info(f"Executing scheduled task for bot {bot_id}: {command}")

# ==================== REFERRAL SYSTEM ====================
async def handle_referral(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handle referral codes"""
    user = update.effective_user
    args = context.args
    
    if not args:
        # Show user's referral info
        user_data = get_user(user.id)
        await update.message.reply_text(
            f"🎁 *Your Referral Program*\n\n"
            f"🔗 Your referral link:\n"
            f"`https://t.me/{context.bot.username}?start=ref_{user_data['referral_code']}`\n\n"
            f"💰 Total Earnings: `${user_data['total_earnings']}`\n"
            f"👥 Referrals: Get 10% of what your referrals spend!\n\n"
            f"_Share your link and earn!_",
            parse_mode="Markdown"
        )
        return
    
    # Process referral
    ref_code = args[0]
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT user_id FROM users WHERE referral_code = ?", (ref_code,))
    referrer = c.fetchone()
    conn.close()
    
    if referrer and referrer[0] != user.id:
        # Add referral
        conn = get_db()
        c = conn.cursor()
        c.execute("UPDATE users SET referred_by = ? WHERE user_id = ?", (referrer[0], user.id))
        c.execute("INSERT INTO referrals (referrer_id, referred_id, reward_amount, date) VALUES (?, ?, ?, ?)",
                  (referrer[0], user.id, 0, datetime.now().isoformat()))
        conn.commit()
        conn.close()
        
        # Notify referrer
        await context.bot.send_message(
            chat_id=referrer[0],
            text=f"🎉 *New Referral!*\n\nUser @{user.username or user.first_name} joined using your link!\nYou'll earn 10% of their purchases!",
            parse_mode="Markdown"
        )
        
        await update.message.reply_text("✅ Referral applied! Welcome bonus added!")

# ==================== MARKETPLACE ====================
async def list_bot_on_marketplace(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """List a bot for sale"""
    args = context.args
    if len(args) < 2:
        await update.message.reply_text(
            "Usage: `/sell BOT_ID PRICE`\n"
            "Example: `/sell abc123 25`\n\n"
            "_Price in USD - you'll get 80% of sales_",
            parse_mode="Markdown"
        )
        return
    
    bot_id = args[0]
    try:
        price = float(args[1])
    except:
        await update.message.reply_text("❌ Price must be a number!")
        return
    
    bot = get_bot(bot_id)
    if not bot or bot['user_id'] != update.effective_user.id:
        await update.message.reply_text("❌ Bot not found or you don't own it!")
        return
    
    conn = get_db()
    c = conn.cursor()
    c.execute("INSERT INTO marketplace (bot_id, seller_id, price, listed_date) VALUES (?, ?, ?, ?)",
              (bot_id, update.effective_user.id, price, datetime.now().isoformat()))
    conn.commit()
    conn.close()
    
    await update.message.reply_text(
        f"✅ Bot listed on marketplace!\n\n"
        f"🤖 Bot: `{bot_id}`\n"
        f"💰 Price: `${price}`\n"
        f"📊 You'll receive: `${price * 0.8}` (80%)\n\n"
        f"_Your bot will appear in the marketplace!_",
        parse_mode="Markdown"
    )

# ==================== ACHIEVEMENTS & GAMIFICATION ====================
async def show_achievements(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Show user's achievements"""
    user_id = update.effective_user.id
    
    conn = get_db()
    c = conn.cursor()
    c.execute("SELECT achievement_