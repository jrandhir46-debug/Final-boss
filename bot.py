import telebot
from telebot import types
import pymongo
from datetime import datetime, date, timedelta
import time
import os
import shutil
import threading
import logging
import secrets
import hashlib
import json
import traceback  # Added for better error logging
import requests  # Added for Sathi API calls
from dotenv import load_dotenv  # For .env support (optional)
from bson import ObjectId

# Load .env if present (no error if missing)
load_dotenv()

# ==================== CONFIGURATION ====================
BOT_TOKEN = os.environ.get('BOT_TOKEN', "8563403763:AAHiThot0trBCVg1bAO6IXF9UHMYvrcFHK8")
ADMIN_ID = int(os.environ.get('ADMIN_ID', "8149503660"))  # Updated to new admin ID
CHANNEL_ID = int(os.environ.get('CHANNEL_ID', "-1002964225991"))
CHANNEL_LINK = os.environ.get('CHANNEL_LINK', "https://t.me/+hVihcFzM0dw3Mjlh")
TASK_APP_LINK = os.environ.get('TASK_APP_LINK', "https://rushsms.xyz/landingPage.html?inviteCode=ABvHjG")
INVITE_CODE = os.environ.get('INVITE_CODE', "ABvHjG")
BOT_USERNAME = os.environ.get('BOT_USERNAME', "Upi_money2_bot")
WEB_APP_URL = os.environ.get('WEB_APP_URL', "https://calm-black-3n0qcvyckd.edgeone.app/")  # Placeholder - User will update this
MONGO_URI = os.environ.get('MONGO_URI', "mongodb+srv://jrandhir46_db_user:Bxy323s-wNZegga@cluster0.qv2tiqy.mongodb.net/earning_bot?retryWrites=true&w=majority&tlsAllowInvalidCertificates=true")
SATHI_TOKEN = os.environ.get('SATHI_TOKEN', "GCM7Z79XZ1N06NOJ")
SATHI_KEY = os.environ.get('SATHI_KEY', "LuSqrkkTPi73SdXsFbhAOtSy")
SATHI_BASE_URL = "https://saathigateway.com/api"
MIN_WITHDRAW = 10  # Minimum withdrawal is always 10, options for 10 and 30 always available
REFERRAL_BONUS = 2  # Updated to 2 rupees per invitation
TASK_BONUS = 10  # Global for WA/signup, SMS separate in captions
SMS_TASK_BONUS = 20  # For SMS
WELCOME_BONUS = 0

# No WEB_APP_URL needed anymore (removed hosting dependency)

# Logging setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = telebot.TeleBot(BOT_TOKEN, parse_mode="MARKDOWN")

# ==================== DATABASE CLASS ====================
class Database:
    def __init__(self):
        self.client = None
        self.db = None
        self.collections = {}
        self.init_database()
        
    def init_database(self):
        try:
            self.client = pymongo.MongoClient(
                MONGO_URI,
                tlsAllowInvalidCertificates=True,  # SSL fix for hosting
                serverSelectionTimeoutMS=60000  # Longer timeout
            )
            self.db = self.client['earning_bot']
            self.collections = {
                'users': self.db['users'],
                'withdrawals': self.db['withdrawals'],
                'task_submissions': self.db['task_submissions'],
                'tutorials': self.db['tutorials'],
                'broadcasts': self.db['broadcasts'],
                'demo_videos': self.db['demo_videos']
            }
            # Create indexes for performance
            self.create_indexes()
            logger.info("🎯 MongoDB initialized successfully")
        except Exception as e:
            logger.error(f"❌ Database init error: {e}\n{traceback.format_exc()}")
            
    def create_indexes(self):
        try:
            self.collections['users'].create_index('user_id', unique=True)
            self.collections['withdrawals'].create_index('user_id')
            self.collections['task_submissions'].create_index('user_id')
            self.collections['tutorials'].create_index('task_type')
            self.collections['broadcasts'].create_index('status')
            self.collections['demo_videos'].create_index('is_active')
            logger.info("✅ Indexes created successfully")
        except Exception as e:
            logger.error(f"❌ Index creation error: {e}")

    def create_backup(self):
        # For MongoDB, backups can be done via mongodump externally. Skipping auto-backup for now.
        logger.info("📦 MongoDB backup: Use mongodump command externally for cloud DB")

    def get_user(self, user_id):
        try:
            return self.collections['users'].find_one({'user_id': user_id})
        except Exception as e:
            logger.error(f"❌ Get user error for {user_id}: {e}\n{traceback.format_exc()}")
            return None

    def create_user(self, user_id, first_name, username=None, referred_by=None):
        try:
            joined_date = datetime.now().isoformat()
            user_doc = {
                'user_id': user_id,
                'first_name': first_name,
                'username': username,
                'points': WELCOME_BONUS,
                'referred_by': referred_by,
                'upi_id': None,
                'joined_date': joined_date,
                'last_active': joined_date,
                'has_withdrawn': 0,
                'referral_count': 0,
                'last_task_date': None,
                'total_earned': WELCOME_BONUS,
                'total_withdrawn': 0,
                'task_completed': 0,
                'channel_joined': False,  # New field for channel join
                'has_penalty': False,
                'deducted_amount': 0
            }
            result = self.collections['users'].update_one(
                {'user_id': user_id},
                {'$setOnInsert': user_doc},
                upsert=True
            )
            if result.upserted_id:
                if referred_by and referred_by != user_id:
                    self.update_points(referred_by, REFERRAL_BONUS)
                    self.update_referral_count(referred_by)
                logger.info(f"👤 New user created: {user_id}")
                return True
            return False
        except Exception as e:
            logger.error(f"❌ Create user error: {e}\n{traceback.format_exc()}")
            return False

    def update_points(self, user_id, points):
        try:
            if points > 0:
                self.collections['users'].update_one(
                    {'user_id': user_id},
                    {'$inc': {'points': points, 'total_earned': points}}
                )
            else:
                self.collections['users'].update_one(
                    {'user_id': user_id},
                    {'$inc': {'points': points}}
                )
            return True
        except Exception as e:
            logger.error(f"❌ Update points error: {e}\n{traceback.format_exc()}")
            return False

    def update_referral_count(self, user_id):
        try:
            # Count only users who have joined the channel
            count = self.collections['users'].count_documents({'referred_by': user_id, 'channel_joined': True})
            self.collections['users'].update_one(
                {'user_id': user_id},
                {'$set': {'referral_count': count}}
            )
            return count
        except Exception as e:
            logger.error(f"❌ Update referral count error: {e}\n{traceback.format_exc()}")
            return 0

    def get_referral_count(self, user_id):
        try:
            user = self.get_user(user_id)
            return user['referral_count'] if user else 0
        except Exception as e:
            logger.error(f"❌ Get referral count error: {e}\n{traceback.format_exc()}")
            return 0

    def get_referrals_list(self, user_id, limit=10):
        """Get list of referrals for a user (recent first)"""
        try:
            return list(self.collections['users'].find(
                {'referred_by': user_id, 'channel_joined': True},
                {'first_name': 1, 'username': 1, 'points': 1, 'joined_date': 1, 'referral_count': 1}
            ).sort([('joined_date', -1)]).limit(limit))
        except Exception as e:
            logger.error(f"❌ Get referrals list error: {e}\n{traceback.format_exc()}")
            return []

    def update_user(self, user_id, **kwargs):
        try:
            update_dict = {'$set': kwargs}
            self.collections['users'].update_one({'user_id': user_id}, update_dict)
            return True
        except Exception as e:
            logger.error(f"❌ Update user error: {e}\n{traceback.format_exc()}")
            return False

    def get_all_users(self):
        try:
            return [doc['user_id'] for doc in self.collections['users'].find({}, {'user_id': 1})]
        except Exception as e:
            logger.error(f"❌ Get all users error: {e}\n{traceback.format_exc()}")
            return []

    def is_user_in_channel(self, user_id):
        try:
            member = bot.get_chat_member(CHANNEL_ID, user_id)
            return member.status in ['member', 'administrator', 'creator']
        except Exception as e:
            logger.error(f"❌ Check channel membership error: {e}")
            return False

    def add_withdrawal(self, user_id, amount, upi_id, method='upi'):
        try:
            request_date = datetime.now().isoformat()
            wd_doc = {
                'user_id': user_id,
                'amount': amount,
                'upi_id': upi_id,
                'status': 'pending',
                'request_date': request_date,
                'process_date': None,
                'admin_id': None,
                'method': method
            }
            result = self.collections['withdrawals'].insert_one(wd_doc)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ Add withdrawal error: {e}\n{traceback.format_exc()}")
            return None

    def get_pending_withdrawals(self):
        try:
            pipeline = [
                {'$match': {'status': 'pending'}},
                {'$lookup': {
                    'from': 'users',
                    'localField': 'user_id',
                    'foreignField': 'user_id',
                    'as': 'user'
                }},
                {'$unwind': '$user'},
                {'$project': {
                    '_id': 1, 'user_id': 1, 'amount': 1, 'upi_id': 1, 'status': 1,
                    'request_date': 1, 'process_date': 1, 'admin_id': 1, 'method': 1,
                    'first_name': '$user.first_name', 'username': '$user.username'
                }},
                {'$sort': {'request_date': 1}}
            ]
            return list(self.db['withdrawals'].aggregate(pipeline))
        except Exception as e:
            logger.error(f"❌ Get pending withdrawals error: {e}\n{traceback.format_exc()}")
            return []

    def update_withdrawal_status(self, withdrawal_id, status, admin_id=None):
        try:
            process_date = datetime.now().isoformat()
            update_dict = {'$set': {'status': status, 'process_date': process_date}}
            if admin_id:
                update_dict['$set']['admin_id'] = admin_id
            self.collections['withdrawals'].update_one({'_id': ObjectId(withdrawal_id)}, update_dict)
            return True
        except Exception as e:
            logger.error(f"❌ Update withdrawal status error: {e}\n{traceback.format_exc()}")
            return False

    def add_broadcast(self, type, content=None, media_id=None):
        try:
            start_time = datetime.now().isoformat()
            bc_doc = {
                'type': type,
                'content': content,
                'media_id': media_id,
                'status': 'pending',
                'start_time': start_time,
                'success_count': 0,
                'failed_count': 0,
                'total_users': 0
            }
            result = self.collections['broadcasts'].insert_one(bc_doc)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ Add broadcast error: {e}\n{traceback.format_exc()}")
            return None

    def update_broadcast_status(self, broadcast_id, status, success_count=0, failed_count=0, total_users=0):
        try:
            update_dict = {'$set': {'status': status, 'success_count': success_count, 'failed_count': failed_count, 'total_users': total_users}}
            self.collections['broadcasts'].update_one({'_id': ObjectId(broadcast_id)}, update_dict)
            return True
        except Exception as e:
            logger.error(f"❌ Update broadcast status error: {e}\n{traceback.format_exc()}")
            return False

    def get_task_submission_count(self, user_id):
        try:
            return self.collections['task_submissions'].count_documents({'user_id': user_id, 'status': 'pending'})
        except Exception as e:
            logger.error(f"❌ Get task submission count error: {e}\n{traceback.format_exc()}")
            return 0

    def get_today_submission_count(self, user_id):
        try:
            today_start = datetime.combine(date.today(), datetime.min.time())
            today_end = datetime.combine(date.today(), datetime.max.time())
            return self.collections['task_submissions'].count_documents({
                'user_id': user_id,
                'submission_date': {'$gte': today_start, '$lte': today_end}
            })
        except Exception as e:
            logger.error(f"❌ Get today submission count error: {e}\n{traceback.format_exc()}")
            return 0

    def get_today_approved_count(self, user_id):
        try:
            today_start = datetime.combine(date.today(), datetime.min.time())
            today_end = datetime.combine(date.today(), datetime.max.time())
            return self.collections['task_submissions'].count_documents({
                'user_id': user_id,
                'status': 'approved',
                'submission_date': {'$gte': today_start, '$lte': today_end}
            })
        except Exception as e:
            logger.error(f"❌ Get today approved count error: {e}\n{traceback.format_exc()}")
            return 0

    def add_tutorial(self, task_type, media_type, media_id, caption):
        try:
            created_date = datetime.now().isoformat()
            tut_doc = {
                'task_type': task_type,
                'media_type': media_type,
                'media_id': media_id,
                'caption': caption,
                'is_active': 1,
                'created_date': created_date
            }
            result = self.collections['tutorials'].insert_one(tut_doc)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ Add tutorial error: {e}\n{traceback.format_exc()}")
            return None

    def get_tutorial(self, task_type):
        try:
            return self.collections['tutorials'].find_one(
                {'task_type': task_type, 'is_active': 1},
                sort=[('_id', -1)]
            )
        except Exception as e:
            logger.error(f"❌ Get tutorial error: {e}\n{traceback.format_exc()}")
            return None

    def add_demo_video(self, media_id, caption, media_type):
        try:
            created_date = datetime.now().isoformat()
            demo_doc = {
                'video_id': media_id,
                'caption': caption,
                'media_type': media_type,
                'is_active': 1,
                'created_date': created_date
            }
            result = self.collections['demo_videos'].insert_one(demo_doc)
            return str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ Add demo video error: {e}\n{traceback.format_exc()}")
            return None

    def get_demo_video(self):
        try:
            return self.collections['demo_videos'].find_one({'is_active': 1}, sort=[('_id', -1)])
        except Exception as e:
            logger.error(f"❌ Get demo video error: {e}\n{traceback.format_exc()}")
            return None

    def get_user_field_safe(self, user_data, key, default=None):
        """Safe field getter for dict"""
        if not user_data:
            return default
        return user_data.get(key, default)

    # New method for stats
    def get_stats(self):
        try:
            total_users = self.collections['users'].count_documents({})
            channel_joined_users = self.collections['users'].count_documents({'channel_joined': True})
            total_submissions = self.collections['task_submissions'].count_documents({})
            pending_submissions = self.collections['task_submissions'].count_documents({'status': 'pending'})
            approved_submissions = self.collections['task_submissions'].count_documents({'status': 'approved'})
            rejected_submissions = self.collections['task_submissions'].count_documents({'status': 'rejected'})
            total_withdrawals = self.collections['withdrawals'].count_documents({})
            pending_withdrawals = self.collections['withdrawals'].count_documents({'status': 'pending'})
            completed_withdrawals = self.collections['withdrawals'].count_documents({'status': 'completed'})
            total_referrals = sum(doc.get('referral_count', 0) for doc in self.collections['users'].find({})) if total_users > 0 else 0
            total_earned = sum(doc.get('total_earned', 0) for doc in self.collections['users'].find({})) if total_users > 0 else 0

            return {
                'total_users': total_users,
                'channel_joined_users': channel_joined_users,
                'total_submissions': total_submissions,
                'pending_submissions': pending_submissions,
                'approved_submissions': approved_submissions,
                'rejected_submissions': rejected_submissions,
                'total_withdrawals': total_withdrawals,
                'pending_withdrawals': pending_withdrawals,
                'completed_withdrawals': completed_withdrawals,
                'total_referrals': total_referrals,
                'total_earned': total_earned
            }
        except Exception as e:
            logger.error(f"❌ Get stats error: {e}\n{traceback.format_exc()}")
            return {}

    # New method for storage stats (MongoDB dbStats)
    def get_storage_stats(self):
        try:
            db_stats = self.db.command('dbStats')
            # Assuming Atlas M0 free tier quota of 512MB; adjust if different plan
            quota_mb = 512  # Free tier default; can be env var if needed
            data_size_mb = db_stats.get('dataSize', 0) / (1024 * 1024)
            storage_size_mb = db_stats.get('storageSize', 0) / (1024 * 1024)
            indexes_size_mb = db_stats.get('indexSize', 0) / (1024 * 1024)
            used_mb = storage_size_mb  # Storage size is the allocated/used on disk
            remaining_mb = quota_mb - used_mb
            used_percent = (used_mb / quota_mb) * 100

            return {
                'quota_mb': quota_mb,
                'data_size_mb': round(data_size_mb, 2),
                'storage_size_mb': round(storage_size_mb, 2),
                'indexes_size_mb': round(indexes_size_mb, 2),
                'used_mb': round(used_mb, 2),
                'remaining_mb': round(remaining_mb, 2),
                'used_percent': round(used_percent, 2),
                'collections': db_stats.get('collections', 0),
                'objects': db_stats.get('objects', 0)
            }
        except Exception as e:
            logger.error(f"❌ Get storage stats error: {e}\n{traceback.format_exc()}")
            return None

# Initialize database
db = Database()

# ==================== SECURITY CHECK ====================
def safe_execute(func):
    def wrapper(*args, **kwargs):
        try:
            if args and hasattr(args[0], 'from_user'):
                user_id = args[0].from_user.id
                message_text = args[0].text if hasattr(args[0], 'text') else ""
                
                # ADMIN KO SAB ALLOW - Full unrestricted for admin
                if user_id == ADMIN_ID:
                    # For admin, skip ALL checks including channel join
                    db.update_user(user_id, channel_joined=True)  # Force channel joined for admin
                    return func(*args, **kwargs)
                
                # For /start command, skip user checks and let it create account
                if message_text.startswith('/start'):
                    return func(*args, **kwargs)
                
                # CHANNEL CHECK - Skip for specific join button to avoid loop
                if message_text != "👥 Join Official Channel" and not db.is_user_in_channel(user_id):
                    join_text = f"""⚠️ *Channel Membership Required!* 📢

🌟 Dear user, please join our official channel to unlock all features! 🚀

👉 [Join Now & Get ₹{WELCOME_BONUS} Bonus!]({CHANNEL_LINK}) 💰

*🎁 Exclusive Benefits:*
- ₹{WELCOME_BONUS} Welcome Bonus on Join! 🎉
- ₹{TASK_BONUS} per Daily Task 📱
- ₹{REFERRAL_BONUS} per Friend Referral 👥
- Latest Updates & Tips! 📈

*🔥 Join now and start earning instantly!* 🌟"""
                    
                    markup = types.InlineKeyboardMarkup()
                    markup.add(types.InlineKeyboardButton("👉 Join Official Channel", url=CHANNEL_LINK))
                    markup.add(types.InlineKeyboardButton("✅ I've Joined! 🟢", callback_data="check_join"))
                    
                    bot.send_message(user_id, join_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")
                    return
                
                # Get user data safely - Now safe since /start is skipped
                user_data = db.get_user(user_id)
                if not user_data:
                    # Fallback: Try to create if missing (edge case)
                    db.create_user(user_id, args[0].from_user.first_name, args[0].from_user.username)
                    user_data = db.get_user(user_id)
                    if not user_data:
                        bot.send_message(user_id, "❌ *Account creation failed. Contact support.* 🔧", parse_mode="MARKDOWN")
                        return
                
                # Removed device verification check
                
                # Update last active
                db.update_user(user_id, last_active=datetime.now().isoformat())
                
            return func(*args, **kwargs)
        except Exception as e:
            logger.error(f"❌ Safe execute error: {e}\n{traceback.format_exc()}")
            if args and hasattr(args[0], 'chat'):
                try:
                    bot.send_message(args[0].chat.id, "❌ Error occurred. Contact support. 🔧", parse_mode="MARKDOWN")
                except:
                    pass
    return wrapper

# ==================== MENU SYSTEM ====================
def main_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=2)
    markup.add("💰 Check Balance", "👥 Refer & Earn")
    markup.add("🏦 Withdraw Funds", "📱 Signup Task")
    markup.add("🎬 Task Tutorial", "🏆 Leaderboard")
    markup.add("👥 Join Official Channel")
    return markup

def withdraw_method_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    markup.add("💳 UPI Withdrawal")
    markup.add("⬅️ Back to Menu")
    return markup

def withdraw_amount_menu():
    markup = types.ReplyKeyboardMarkup(resize_keyboard=True, row_width=1)
    markup.add("₹10 Withdraw")
    markup.add("₹30 Withdraw")
    markup.add("⬅️ Back to Menu")
    return markup

# ==================== BOT COMMANDS ====================
@bot.message_handler(commands=['start'])
@safe_execute
def start_cmd(message):
    user_id = message.from_user.id
    first_name = message.from_user.first_name
    username = message.from_user.username
    
    referred_by = None
    if len(message.text.split()) > 1:
        ref_code = message.text.split()[1]
        if ref_code.isdigit():
            referred_by = int(ref_code)
    
    user_data = db.get_user(user_id)
    
    # Create user if not exists
    if not user_data:
        success = db.create_user(user_id, first_name, username, referred_by)
        if not success:
            bot.send_message(user_id, "❌ *Failed to create account. Try again later or contact support.* 🔧", parse_mode="MARKDOWN")
            return
        user_data = db.get_user(user_id)
        if not user_data:
            bot.send_message(user_id, "❌ *Account creation failed. Contact support.* 🔧", parse_mode="MARKDOWN")
            return
    
    # Channel check - Enforce before anything else (except admin)
    if not db.is_user_in_channel(user_id):
        join_text = f"""⚠️ *Channel Membership Required!* 📢

🌟 Dear user, please join our official channel to unlock all features! 🚀

👉 [Join Now & Get ₹{WELCOME_BONUS} Bonus!]({CHANNEL_LINK}) 💰

*🎁 Exclusive Benefits:*
- ₹{WELCOME_BONUS} Welcome Bonus on Join! 🎉
- ₹{TASK_BONUS} per Daily Task 📱
- ₹{REFERRAL_BONUS} per Friend Referral 👥
- Latest Updates & Tips! 📈

*🔥 Join now and start earning instantly!* 🌟"""
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("👉 Join Official Channel", url=CHANNEL_LINK))
        markup.add(types.InlineKeyboardButton("✅ I've Joined! 🟢", callback_data="check_join"))
        
        bot.send_message(user_id, join_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")
        return
    
    # Set channel_joined = True after successful channel check
    db.update_user(user_id, channel_joined=True)
    # Update referrer count if applicable and notify referrer
    if referred_by and referred_by != user_id:
        old_count = db.get_referral_count(referred_by)
        db.update_referral_count(referred_by)
        new_count = db.get_referral_count(referred_by)
        if new_count > old_count:
            # Modified: Make user_id clickable to open profile
            notification_text = f"[{user_id}](tg://user?id={user_id}) Got Invited By Your Url: +{REFERRAL_BONUS} Rs"
            try:
                bot.send_message(referred_by, notification_text, parse_mode="MARKDOWN", disable_web_page_preview=True)
            except Exception as e:
                logger.error(f"Failed to notify referrer {referred_by}: {e}")
                # Fallback without link
                bot.send_message(referred_by, f"{user_id} Got Invited By Your Url: +{REFERRAL_BONUS} Rs", parse_mode="MARKDOWN")
    
    # If user is admin, show admin menu
    if user_id == ADMIN_ID:
        welcome_text = f"""✅ *Welcome back, Admin {first_name}!* 👑

Your account is active. 🚀

Start managing! 🌟"""
        bot.send_message(user_id, welcome_text, reply_markup=main_menu(), parse_mode="MARKDOWN")
        return
    
    # Removed verification - directly show welcome
    # Updated welcome text - Removed Download App, added Withdrawal 100 button
    welcome_text = f"""🌟 *Thanks for using Rupeerush Bot, {first_name}!* 🎉

*🚀 Quick Start Guide:*
- 💰 Check Balance: See your earnings instantly!
- 📱 Signup Task: Complete tasks for ₹{TASK_BONUS} bonus! 🎁
- 👥 Refer & Earn: Invite friends for ₹{REFERRAL_BONUS} each! 📈

*🔥 Start earning now and reach your goals!* 🌟"""
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("🏦 Withdrawal 100", callback_data="withdraw_100_ad"))
    
    bot.send_message(user_id, welcome_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")
    bot.send_message(user_id, "Use the menu below to get started!", reply_markup=main_menu(), parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data == "withdraw_100_ad")
@safe_execute
def withdraw_100_ad_callback(call):
    user_id = call.from_user.id
    
    # Advertising: Show Rupeerush app link
    ad_text = f"""🚀 *Unlock Instant ₹100 Withdrawal!* 💰

*🔥 Special Offer:*
Complete this quick task to withdraw ₹100 directly!

*📱 Steps:*
1. Open the Rupeerush App 👇
2. Signup with code: `{INVITE_CODE}` 🔑
3. Earn & Withdraw Instantly! ⚡

*👉 [Download Rupeerush Now]({TASK_APP_LINK}) 📥*

*💡 Tip:* Approved in minutes! No waiting. 🎉"""
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📱 Open Rupeerush App", url=TASK_APP_LINK))
    # Removed the "📱 Signup Task" button below it as per request
    
    bot.send_message(user_id, ad_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")
    bot.answer_callback_query(call.id, "Check out the offer! 🚀")

@bot.callback_query_handler(func=lambda call: call.data == "check_join")
def check_join_callback(call):  # Removed @safe_execute to avoid loop
    user_id = call.from_user.id
    if db.is_user_in_channel(user_id):
        bot.delete_message(call.message.chat.id, call.message.message_id)
        # Ensure user exists
        user_data = db.get_user(user_id)
        if not user_data:
            db.create_user(user_id, call.from_user.first_name, call.from_user.username)
        # Set channel joined
        db.update_user(user_id, channel_joined=True)
        # Update referrer if applicable
        referred_by = user_data.get('referred_by')
        if referred_by and referred_by != user_id:
            old_count = db.get_referral_count(referred_by)
            db.update_referral_count(referred_by)
            new_count = db.get_referral_count(referred_by)
            if new_count > old_count:
                # Modified: Make user_id clickable to open profile
                notification_text = f"[{user_id}](tg://user?id={user_id}) Got Invited By Your Url: +{REFERRAL_BONUS} Rs"
                try:
                    bot.send_message(referred_by, notification_text, parse_mode="MARKDOWN", disable_web_page_preview=True)
                except Exception as e:
                    logger.error(f"Failed to notify referrer {referred_by}: {e}")
                    # Fallback without link
                    bot.send_message(referred_by, f"{user_id} Got Invited By Your Url: +{REFERRAL_BONUS} Rs", parse_mode="MARKDOWN")
        # Directly show welcome, no verification
        start_cmd(call.message)  # Reuse start logic
    else:
        bot.answer_callback_query(call.id, "❌ Please join the channel first! 📢", show_alert=True)

# ==================== JOIN CHANNEL HANDLER ====================
@bot.message_handler(func=lambda m: m.text == "👥 Join Official Channel")
@safe_execute
def join_official_channel(message):
    user_id = message.from_user.id
    official_channel_link = "https://t.me/RupeeRush666666"
    join_text = f"""👥 *Join Our Official Channel* 📢

🌟 Stay updated with the latest news, tasks, bonuses, and announcements! 🚀

👉 [Join Now]({official_channel_link}) 💬

*🎁 Benefits:*
- Exclusive updates & promotions! 🎉
- Special bonuses & tips! 💰
- Community support & fun! 👥

*🔥 Join now and never miss out!* 🌟"""
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("👉 Join Official Channel", url=official_channel_link))
    markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu_channel"))
    
    bot.send_message(user_id, join_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data == "back_to_menu_channel")
@safe_execute
def back_to_menu_channel(call):
    user_id = call.from_user.id
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    bot.send_message(user_id, "↩️ Back to main menu. 🌟", reply_markup=main_menu(), parse_mode="MARKDOWN")
    bot.answer_callback_query(call.id)

# ==================== BALANCE & REFERRAL ====================
@bot.message_handler(func=lambda m: m.text == "💰 Check Balance")
@safe_execute
def balance(message):
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start to create account. 🚀", parse_mode="MARKDOWN")
        return
    
    ref_count = db.get_referral_count(user_id)
    total_earned = db.get_user_field_safe(user_data, 'total_earned', 0)  # Safe access
    
    balance_text = f"""💳 *Balance Overview* 📊

*💰 Current Balance:* ₹{db.get_user_field_safe(user_data, 'points', 0):.2f} 💸
*📈 Total Earned:* ₹{total_earned:.2f} 🌟
*👥 Referrals:* {ref_count} 👤
*💎 Referral Earnings:* ₹{ref_count * REFERRAL_BONUS:.2f} 🎁"""

    bot.send_message(user_id, balance_text, reply_markup=main_menu(), parse_mode="MARKDOWN")

@bot.message_handler(func=lambda m: m.text == "👥 Refer & Earn")
@safe_execute
def refer(message):
    user_id = message.from_user.id
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    ref_count = db.get_referral_count(user_id)
    
    refer_text = f"""👥 *Refer & Earn* 📈

*🔗 Your Referral Link:* `{ref_link}` 📲

*📊 Your Stats:*
- Total Referrals: {ref_count} 👤
- Referral Earnings: ₹{ref_count * REFERRAL_BONUS:.2f} 💰

*💡 How it Works:*
1. Share your link with friends 📤
2. They join using your link and channel ✅
3. You earn ₹{REFERRAL_BONUS} for each! 🎉
4. No limit on referrals! ∞

*🎯 Benefits:*
- Easy money earning! 💸
- Fast rewards! ⚡"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📤 Share Link", 
        url=f"https://t.me/share/url?url={ref_link}&text=Join%20Rupeerush%20Bot%20to%20earn%20money%20daily!%20Use%20my%20link:%20{ref_link}"))
    markup.add(types.InlineKeyboardButton("🔄 Check Referrals", callback_data="check_refs"))
    
    bot.send_message(user_id, refer_text, reply_markup=markup, parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data == "check_refs")
@safe_execute
def check_refs(call):
    user_id = call.from_user.id
    ref_count = db.get_referral_count(user_id)
    referrals = db.get_referrals_list(user_id)
    
    if ref_count == 0:
        bot.answer_callback_query(call.id, f"✅ You have {ref_count} referrals! 👥")
        bot.send_message(user_id, f"📊 *Your Referrals:* {ref_count} 👤\n\nKeep sharing to earn more! 🚀", reply_markup=main_menu(), parse_mode="MARKDOWN")
        return
    
    # Show list with clickable user IDs
    refs_text = f"📊 *Your Referrals ({ref_count})* 👥\n\n"
    for ref in referrals[:10]:  # Limit to 10
        ref_id = ref.get('user_id', 'N/A')
        name = ref.get('first_name', 'N/A')
        joined = ref.get('joined_date', 'N/A')[:10] if ref.get('joined_date') else 'N/A'
        refs_text += f"[{ref_id}](tg://user?id={ref_id}) - {name}\n📅 Joined: {joined}\n\n"
    
    if len(referrals) > 10:
        refs_text += f"... and {ref_count - 10} more!"
    
    refs_text += f"\n*💰 Total Earnings from Referrals:* ₹{ref_count * REFERRAL_BONUS:.2f}"
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    
    bot.edit_message_text(refs_text, call.message.chat.id, call.message.message_id, reply_markup=markup, parse_mode="MARKDOWN", disable_web_page_preview=True)
    bot.answer_callback_query(call.id, f"Loaded {ref_count} referrals! 👥")

# ==================== WITHDRAWAL SYSTEM ====================
@bot.message_handler(func=lambda m: m.text == "🏦 Withdraw Funds")
@safe_execute
def withdraw_start(message):
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start. 🚀", parse_mode="MARKDOWN")
        return
    
    balance = db.get_user_field_safe(user_data, 'points', 0)
    
    if balance < MIN_WITHDRAW:
        bot.send_message(user_id, 
            f"""⚠️ *Insufficient Balance* 💳

*💰 Current Balance:* ₹{balance:.2f} 💸
*📏 Minimum Required:* ₹{MIN_WITHDRAW} 📊

*💡 Ways to Earn:*
- Complete daily tasks: ₹{TASK_BONUS}/task 📱
- Refer friends: ₹{REFERRAL_BONUS}/referral 👥
- Welcome bonus: ₹{WELCOME_BONUS} 🎉""", parse_mode="MARKDOWN")
        return
    
    withdraw_info = f"""🏦 *Withdrawal Options* 💸

*💰 Available Balance:* ₹{balance:.2f} 💳
*💎 Withdrawal Fee:* ₹0 (Free!) 🎉

*📋 Choose your method:*
- 💳 UPI: Manual approval (1-24 hours)

Select below:"""
    
    bot.send_message(user_id, withdraw_info, reply_markup=withdraw_method_menu(), parse_mode="MARKDOWN")

@bot.message_handler(func=lambda m: m.text == "⬅️ Back to Menu")
@safe_execute
def back_to_menu_from_withdraw(message):
    bot.send_message(message.from_user.id, "↩️ Returning to main menu... 🌟", reply_markup=main_menu(), parse_mode="MARKDOWN")

@bot.message_handler(func=lambda m: m.text == "💳 UPI Withdrawal")
@safe_execute
def upi_withdraw(message):
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    upi = db.get_user_field_safe(user_data, 'upi_id', None)
    
    if not upi:
        bot.send_message(user_id, 
            f"""💳 *UPI ID Required* 🔑

Please send your UPI ID to set up withdrawals. 📲

*📝 Format:* `yournumber@upi` 
*📋 Example:* `1234567890@ybl` or `yourname@okaxis` 💳""", parse_mode="MARKDOWN")
        bot.register_next_step_handler(message, save_upi)
        return
    
    markup = withdraw_amount_menu()
    
    amount_info = f"""💳 *UPI Withdrawal* 📱

*🔗 Registered UPI:* `{upi}` 💳

*📋 Select withdrawal amount:*"""
    
    bot.send_message(user_id, amount_info, reply_markup=markup, parse_mode="MARKDOWN")
    bot.register_next_step_handler(message, process_upi_amount)  # Fixed: Register handler

def save_upi(message):
    user_id = message.from_user.id
    upi = message.text.strip()
    
    # Basic UPI validation
    if '@' in upi and len(upi) > 5:
        if db.update_user(user_id, upi_id=upi):
            bot.send_message(user_id, 
                f"""✅ *UPI Saved Successfully!* 🎉

*🔗 Your UPI:* `{upi}` 💳

You can now proceed with UPI withdrawals. 🚀 

Go to 🏦 Withdraw Funds > 💳 UPI to continue! 🌟""", 
                reply_markup=withdraw_method_menu(), parse_mode="MARKDOWN")
        else:
            bot.send_message(user_id, "❌ Failed to save UPI. Try again. 🔄", parse_mode="MARKDOWN")
    else:
        bot.send_message(user_id, 
            f"""❌ *Invalid UPI Format* ⚠️

Please send a valid UPI ID. 📝

*📋 Correct Formats:*
- `1234567890@ybl` 💳
- `yourname@okaxis` 
- `username@paytm`

*🔄 Try again:*""", parse_mode="MARKDOWN")
        bot.register_next_step_handler(message, save_upi)

def process_upi_amount(message):
    user_id = message.from_user.id
    if message.text == "⬅️ Back to Menu":
        bot.send_message(user_id, "↩️ Returning to main menu... 🌟", reply_markup=main_menu(), parse_mode="MARKDOWN")
        return
    
    user_data = db.get_user(user_id)
    if not user_data:
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start. 🚀", parse_mode="MARKDOWN")
        return
    
    amount = 10 if "₹10" in message.text else 30
    balance = db.get_user_field_safe(user_data, 'points', 0)
    
    if balance < amount:
        bot.send_message(user_id, f"❌ *Insufficient Balance* ⚠️\n\n*💰 Available:* ₹{balance:.2f} 💸\n*📏 Required:* ₹{amount} 📊", parse_mode="MARKDOWN")
        return
    
    # Process UPI withdrawal - pending
    upi = db.get_user_field_safe(user_data, 'upi_id', '')
    withdrawal_id = db.add_withdrawal(user_id, amount, upi, 'upi')
    
    if withdrawal_id:
        # Deduct points immediately (refund on reject)
        db.update_points(user_id, -amount)
        total_withdrawn = db.get_user_field_safe(user_data, 'total_withdrawn', 0) + amount
        db.update_user(user_id, has_withdrawn=1, total_withdrawn=total_withdrawn)
        
        # Notify admin - Improved with fallback
        admin_msg = f"""💸 *UPI Withdrawal Request #{withdrawal_id}* 🏦

*👤 User:* {db.get_user_field_safe(user_data, 'first_name', 'User')} (@{db.get_user_field_safe(user_data, 'username', '') or 'No username'})
*🆔 User ID:* `{user_id}`
*💰 Amount:* ₹{amount:.2f}
*🔗 UPI:* `{upi}`
*📊 Balance After:* ₹{balance - amount:.2f}
*⏰ Time:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
        
        markup = types.InlineKeyboardMarkup()
        markup.row(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"accept_{withdrawal_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"reject_{withdrawal_id}")
        )
        
        notify_success = False
        try:
            bot.send_message(ADMIN_ID, admin_msg, reply_markup=markup, parse_mode="MARKDOWN")
            notify_success = True
            logger.info(f"✅ Admin notified for withdrawal #{withdrawal_id}")
        except Exception as e:
            logger.error(f"❌ Failed to notify admin on withdrawal #{withdrawal_id}: {e}\n{traceback.format_exc()}")
            # Fallback: Send simple text without markup if markup fails
            try:
                bot.send_message(ADMIN_ID, f"🚨 EMERGENCY: Withdrawal Request #{withdrawal_id} - Check DB manually! User ID: {user_id}, Amount: ₹{amount}", parse_mode="MARKDOWN")
                logger.info("✅ Fallback admin notify sent")
                notify_success = True
            except:
                logger.error("❌ Even fallback failed - Check ADMIN_ID!")
        
        if not notify_success:
            # Last resort: Log and perhaps broadcast to another ID if you have backup admin
            logger.critical(f"🚨 CRITICAL: No admin notify for #{withdrawal_id} - Manual check required in MongoDB!")
        
        # Notify user
        bot.send_message(user_id, 
            f"""✅ *UPI Withdrawal Request Submitted!* 🎉

*🆔 Request ID:* #{withdrawal_id}
*💰 Amount:* ₹{amount:.2f}
*🔗 UPI:* `{upi}`
*⏳ Status:* Pending Approval 📋

*📝 Important:*
- Stay in our channel 📢
- Don't change username 👤
- Processing time: 1-24 hours ⏰
- You'll be notified when processed! 🔔""",
            reply_markup=main_menu(), parse_mode="MARKDOWN")
    else:
        bot.send_message(user_id, "❌ *Withdrawal Failed* ⚠️\n\nPlease try again or contact support. 🔧", parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data.startswith(('accept_', 'reject_')))
@safe_execute
def handle_withdrawal_approval(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Admin only. 👑", show_alert=True)
        return
    
    action = call.data.split('_')[0]
    withdrawal_id = call.data.split('_')[1]
    
    # Get withdrawal data
    try:
        wd_doc = db.collections['withdrawals'].find_one({'_id': ObjectId(withdrawal_id)})
        if not wd_doc:
            bot.answer_callback_query(call.id, "❌ Request not found. 🔍", show_alert=True)
            return
        
        user_id = wd_doc['user_id']
        amount = wd_doc['amount']
        upi = wd_doc['upi_id']
        user_data = db.get_user(user_id)
        user_name = db.get_user_field_safe(user_data, 'first_name', "User")
        
        if action == 'accept':
            db.update_withdrawal_status(withdrawal_id, 'completed', ADMIN_ID)
            new_balance = db.get_user_field_safe(user_data, 'points', 0)
            bot.send_message(user_id, 
                f"""✅ *UPI Withdrawal Approved!* 🎉

*🆔 Request ID:* #{withdrawal_id}
*💰 Amount:* ₹{amount:.2f}
*🔗 UPI:* `{upi}`
*✅ Status:* Completed! 🌟

💰 Payment processed successfully! 
Check your UPI account within few hours. 📱

Thank you for using Rupeerush! 🙏 🎈""", parse_mode="MARKDOWN")
            
            bot.edit_message_text(
                f"✅ *Approved UPI Withdrawal #{withdrawal_id}* 🏦\n\n*👤 User:* {user_name} (ID: `{user_id}`)\n*💰 Amount:* ₹{amount:.2f}\n*🔗 UPI:* `{upi}`", 
                call.message.chat.id, 
                call.message.message_id, parse_mode="MARKDOWN"
            )
            bot.answer_callback_query(call.id, "✅ Approved! 🌟")
        
        else:
            # Refund points
            db.update_points(user_id, amount)
            db.update_withdrawal_status(withdrawal_id, 'rejected', ADMIN_ID)
            
            bot.send_message(user_id, 
                f"""❌ *UPI Withdrawal Rejected* ⚠️

*🆔 Request ID:* #{withdrawal_id}
*💰 Amount:* ₹{amount:.2f}
*❌ Status:* Rejected 🔒

*📝 Reason:* Administrative decision 👑
*💸 Action:* ₹{amount:.2f} refunded to your balance! 🔄

Contact support for more information. 📞""", parse_mode="MARKDOWN")

            bot.edit_message_text(
                f"❌ *Rejected UPI Withdrawal #{withdrawal_id}* ⚠️\n\n*👤 User:* {user_name} (ID: `{user_id}`)\n*💰 Amount:* ₹{amount:.2f}", 
                call.message.chat.id, 
                call.message.message_id, parse_mode="MARKDOWN"
            )
            bot.answer_callback_query(call.id, "❌ Rejected. ⚠️")
    
    except Exception as e:
        logger.error(f"❌ Approval error: {e}")
        bot.answer_callback_query(call.id, "❌ Error processing. 🔧", show_alert=True)

# ==================== TASK TUTORIAL SYSTEM ====================
@bot.message_handler(func=lambda m: m.text == "🎬 Task Tutorial")
@safe_execute
def task_tutorial(message):
    user_id = message.from_user.id
    
    tutorial_text = f"""📚 *Task Tutorial Center* 🎓

Choose your task type to learn how to complete it: 📋

*📱 SMS Task* - Send SMS verification tasks (Earn ₹{SMS_TASK_BONUS} daily!) 📨
*📱 WA Task* - WhatsApp promotion tasks (Earn ₹{TASK_BONUS} per task) 💬  

Select a task to view tutorial: 🔍"""

    markup = types.InlineKeyboardMarkup(row_width=2)
    markup.add(
        types.InlineKeyboardButton("📱 SMS Task", callback_data="tutorial_sms"),
        types.InlineKeyboardButton("📱 WA Task", callback_data="tutorial_wa")
    )
    markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    
    bot.send_message(user_id, tutorial_text, reply_markup=markup, parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data in ["tutorial_sms", "tutorial_wa"])
@safe_execute
def show_task_tutorial(call):
    user_id = call.from_user.id
    task_type = "sms_task" if call.data == "tutorial_sms" else "wa_task"
    
    tutorial = db.get_tutorial(task_type)
    
    if tutorial:
        media_type = tutorial['media_type']
        media_id = tutorial['media_id']
        caption = tutorial['caption']
        
        try:
            if media_type == "video":
                bot.send_video(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
            elif media_type == "photo":
                bot.send_photo(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
        except Exception as e:
            logger.error(f"❌ Send tutorial media error: {e}")
            bot.send_message(user_id, caption, parse_mode="MARKDOWN")  # Fallback text
        bot.answer_callback_query(call.id)
    else:
        task_name = "SMS" if task_type == "sms_task" else "WhatsApp"
        bonus = SMS_TASK_BONUS if task_type == "sms_task" else TASK_BONUS
        bot.send_message(user_id, f"❌ *No tutorial available for {task_name} task yet.* 📚\n\nContact admin for updates. (Earn ₹{bonus} per completion) 💰", parse_mode="MARKDOWN")
        bot.answer_callback_query(call.id)

@bot.message_handler(commands=['sms', 'wa'])
@safe_execute
def set_task_tutorial(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    task_type = "sms_task" if message.text.startswith('/sms') else "wa_task"
    task_name = "SMS" if task_type == "sms_task" else "WhatsApp"
    bonus = SMS_TASK_BONUS if task_type == "sms_task" else TASK_BONUS
    
    bot.send_message(ADMIN_ID, f"🎬 *Send {task_name} Task Tutorial* 📚\n\nPlease send the video with caption that users will see for {task_name} task tutorial. 🎥", parse_mode="MARKDOWN")
    bot.register_next_step_handler(message, lambda msg: save_task_tutorial(msg, task_type, task_name, bonus))

def save_task_tutorial(message, task_type, task_name, bonus):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.content_type != 'video':
        bot.send_message(ADMIN_ID, "❌ Please send a video file only. 🎥", parse_mode="MARKDOWN")
        return
    
    if task_type == "wa_task":
        caption = message.caption if message.caption else f"Watch this tutorial to learn how to complete the {task_name} task and earn ₹200-500 per day 💰"
    else:
        caption = message.caption if message.caption else f"Watch this tutorial to learn how to complete the {task_name} task and earn ₹{bonus}! 🎉"
    media_id = message.video.file_id
    media_type = "video"
    
    tutorial_id = db.add_tutorial(task_type, media_type, media_id, caption)
    
    if tutorial_id:
        bot.send_message(ADMIN_ID, f"✅ *{task_name} Task Tutorial Set Successfully!* 🎓\n\n*🆔 ID:* #{tutorial_id}\nUsers will see this video when they select the {task_name} task tutorial. 📱", parse_mode="MARKDOWN")
    else:
        bot.send_message(ADMIN_ID, f"❌ Failed to save {task_name} task tutorial. 🔧", parse_mode="MARKDOWN")

# ==================== ADMIN COMMANDS ====================
@bot.message_handler(commands=['add'])
@safe_execute
def add_points(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if len(message.text.split()) < 3:
        help_text = f"""💎 *Add Points Command* 📊

*📝 Usage:*
- `/add user_id points` - Add points to user 👤

*📋 Example:*
`/add 123456789 50` - Adds 50 points to user 123456789 💰"""
        bot.send_message(ADMIN_ID, help_text, parse_mode="MARKDOWN")
        return
    
    try:
        user_id = int(message.text.split()[1])
        points = float(message.text.split()[2])
        
        user_data = db.get_user(user_id)
        if not user_data:
            bot.send_message(ADMIN_ID, f"❌ User ID `{user_id}` not found. 🔍", parse_mode="MARKDOWN")
            return
        
        if db.update_points(user_id, points):
            new_balance = db.get_user_field_safe(user_data, 'points', 0) + points
            bot.send_message(ADMIN_ID, f"✅ *Points Added Successfully!* 🎉\n\n*👤 User:* {db.get_user_field_safe(user_data, 'first_name', 'User')} (ID: `{user_id}`)\n*💰 Points Added:* ₹{points:.2f}\n*📊 New Balance:* ₹{new_balance:.2f}", parse_mode="MARKDOWN")
            
            # Notify user
            bot.send_message(user_id, f"🎉 *Points Credited!* 💰\n\n*💸 Amount:* ₹{points:.2f}\n*📊 New Balance:* ₹{new_balance:.2f}\n\nThank you! 🙏", parse_mode="MARKDOWN")
        else:
            bot.send_message(ADMIN_ID, "❌ Failed to add points. 🔧", parse_mode="MARKDOWN")
            
    except ValueError:
        bot.send_message(ADMIN_ID, "❌ Invalid format. Use: `/add user_id points` 📝", parse_mode="MARKDOWN")

@bot.message_handler(commands=['deduct'])
@safe_execute
def deduct_points(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if len(message.text.split()) < 3:
        help_text = f"""💸 *Deduct Points Command* 📉

*📝 Usage:*
- `/deduct user_id amount` - Deduct points from user 👤

*📋 Example:*
`/deduct 123456789 50` - Deducts 50 points from user 123456789 💰"""
        bot.send_message(ADMIN_ID, help_text, parse_mode="MARKDOWN")
        return
    
    try:
        user_id = int(message.text.split()[1])
        amount = float(message.text.split()[2])
        
        user_data = db.get_user(user_id)
        if not user_data:
            bot.send_message(ADMIN_ID, f"❌ User ID `{user_id}` not found. 🔍", parse_mode="MARKDOWN")
            return
        
        current_points = db.get_user_field_safe(user_data, 'points', 0)
        if current_points < amount:
            bot.send_message(ADMIN_ID, f"❌ Insufficient points to deduct. Current: ₹{current_points:.2f}", parse_mode="MARKDOWN")
            return
        
        if db.update_points(user_id, -amount):
            new_balance = current_points - amount
            bot.send_message(ADMIN_ID, f"✅ *Points Deducted Successfully!* 🎉\n\n*👤 User:* {db.get_user_field_safe(user_data, 'first_name', 'User')} (ID: `{user_id}`)\n*💸 Points Deducted:* ₹{amount:.2f}\n*📊 New Balance:* ₹{new_balance:.2f}", parse_mode="MARKDOWN")
            
            # Notify user
            bot.send_message(user_id, f"⚠️ *Points Deducted!* 💸\n\n*💰 Amount:* ₹{amount:.2f}\n*📊 New Balance:* ₹{new_balance:.2f}\n\nReason: Administrative action. Contact support if needed. 📞", parse_mode="MARKDOWN")
        else:
            bot.send_message(ADMIN_ID, "❌ Failed to deduct points. 🔧", parse_mode="MARKDOWN")
            
    except ValueError:
        bot.send_message(ADMIN_ID, "❌ Invalid format. Use: `/deduct user_id amount` 📝", parse_mode="MARKDOWN")

@bot.message_handler(commands=['demo'])
@safe_execute
def set_demo_photo(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    bot.send_message(ADMIN_ID, "📸 *Send Demo Photo/Video* 🎥\n\nPlease send the demo media that users will see before submission. 📱", parse_mode="MARKDOWN")
    bot.register_next_step_handler(message, save_demo_media)

def save_demo_media(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.content_type not in ['photo', 'video']:
        bot.send_message(ADMIN_ID, "❌ Please send a photo or video file only. 📸", parse_mode="MARKDOWN")
        return
    
    caption = message.caption if message.caption else "Everyone see demo before submission. Exactly like this, submit your screenshot! 📱"
    
    if message.content_type == 'photo':
        media_id = message.photo[-1].file_id
        media_type = "photo"
    else:
        media_id = message.video.file_id
        media_type = "video"
    
    # Store as video_id but use for both (table name is misleading, but works)
    demo_id = db.add_demo_video(media_id, caption, media_type)
    
    if demo_id:
        bot.send_message(ADMIN_ID, f"✅ *Demo Media Set Successfully!* 📸\n\n*🆔 ID:* #{demo_id}\nUsers can now view this anytime via menu or signup task. 🚀", parse_mode="MARKDOWN")
    else:
        bot.send_message(ADMIN_ID, "❌ Failed to save demo media. 🔧", parse_mode="MARKDOWN")

# New /stats command for admin
@bot.message_handler(commands=['stats'])
@safe_execute
def admin_stats(message):
    if message.from_user.id != ADMIN_ID:
        bot.send_message(message.from_user.id, "❌ *Admin Only Command* 👑", parse_mode="MARKDOWN")
        return
    
    stats = db.get_stats()
    if not stats:
        bot.send_message(ADMIN_ID, "❌ *Failed to load stats. Check logs.* 🔧", parse_mode="MARKDOWN")
        return
    
    stats_text = f"""📊 *Bot Statistics* 📈

*👥 Users:*
- Total Users: {stats['total_users']}
- Channel Joined: {stats['channel_joined_users']}

*📱 Tasks:*
- Total Submissions: {stats['total_submissions']}
- Pending: {stats['pending_submissions']}
- Approved: {stats['approved_submissions']}
- Rejected: {stats['rejected_submissions']}

*🏦 Withdrawals:*
- Total Requests: {stats['total_withdrawals']}
- Pending: {stats['pending_withdrawals']}
- Completed: {stats['completed_withdrawals']}

*👥 Referrals & Earnings:*
- Total Referrals: {stats['total_referrals']}
- Total Earned (All Users): ₹{stats['total_earned']:.2f}

*⏰ Updated:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    
    bot.send_message(ADMIN_ID, stats_text, parse_mode="MARKDOWN")

# New /storage command for admin
@bot.message_handler(commands=['storage'])
@safe_execute
def admin_storage(message):
    if message.from_user.id != ADMIN_ID:
        bot.send_message(message.from_user.id, "❌ *Admin Only Command* 👑", parse_mode="MARKDOWN")
        return
    
    storage_stats = db.get_storage_stats()
    if not storage_stats:
        bot.send_message(ADMIN_ID, "❌ *Failed to load storage stats. Check logs.* 🔧", parse_mode="MARKDOWN")
        return
    
    storage_text = f"""☁️ *Cloud Storage Details (MongoDB Atlas)* 📊

*📈 Quota:* {storage_stats['quota_mb']} MB (Free Tier)

*💾 Used Storage:* {storage_stats['used_mb']} MB ({storage_stats['used_percent']}%) 📈
*💾 Remaining:* {storage_stats['remaining_mb']} MB 📉

*📂 Data Size:* {storage_stats['data_size_mb']} MB (Raw data)
*💼 Allocated Storage:* {storage_stats['storage_size_mb']} MB (On disk)
*🔍 Indexes Size:* {storage_stats['indexes_size_mb']} MB

*🔢 Collections:* {storage_stats['collections']}
*📄 Objects:* {storage_stats['objects']}

*⚠️ Warning:* If usage >90%, upgrade plan! 🚀

*⏰ Updated:* {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"""
    
    bot.send_message(ADMIN_ID, storage_text, parse_mode="MARKDOWN")

# ==================== SIGNUP TASK SYSTEM ====================
@bot.message_handler(func=lambda m: m.text == "📱 Signup Task")
@safe_execute
def signup_task(message):
    user_id = message.from_user.id
    user_data = db.get_user(user_id)
    
    if not user_data:
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start. 🚀", parse_mode="MARKDOWN")
        return
    
    today = date.today().isoformat()
    
    # Check if user already submitted today
    pending_count = db.get_today_submission_count(user_id)
    
    if pending_count > 0:
        bot.send_message(user_id, "⏰ *Already Submitted Today!* 📱\n\nWait for approval or try again tomorrow. 🔄", parse_mode="MARKDOWN")
        return
    
    # Check if user has completed task today
    completed_count = db.get_today_approved_count(user_id)
    
    if completed_count > 0:
        bot.send_message(user_id, "✅ *Task Completed Today!* 🎉\n\nCome back tomorrow for new task! 🚀", parse_mode="MARKDOWN")
        return

    task_text = f"""📱 *Signup Task - Earn ₹{TASK_BONUS}* 💰

*📝 Task Steps:*
1. Download app: {TASK_APP_LINK} 📥
2. Register app using this code: (`{INVITE_CODE}`) 🔑
3. Take screenshot and submit 📸 

*📸 Requirements:*
- Clear screenshot showing completion 📷
- Invitation code/email must be visible 👀
- `{INVITE_CODE}` use this code is mandatory! ⚠️ 
- Check DM for proof 📩

*💎 Reward:* ₹{TASK_BONUS} per approved task! 🎁

*📢 Note: Everyone see demo before submission!* 🌟"""

    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📸 Submit Screenshot", callback_data="submit_task"))
    markup.add(types.InlineKeyboardButton("🎬 Watch Demo", callback_data="watch_demo"))
    markup.add(types.InlineKeyboardButton("⬅️ Back to Menu", callback_data="back_to_menu"))
    
    bot.send_message(user_id, task_text, reply_markup=markup, disable_web_page_preview=True, parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data == "watch_demo")
@safe_execute
def show_demo(call):
    user_id = call.from_user.id
    
    demo = db.get_demo_video()
    
    if demo:
        media_id = demo['video_id']
        caption = demo['caption']
        media_type = demo.get('media_type', 'video')
        
        try:
            if media_type == "photo":
                bot.send_photo(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
            else:
                bot.send_video(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
        except Exception as e:
            logger.error(f"❌ Send demo error: {e}")
            bot.send_message(user_id, caption, parse_mode="MARKDOWN")
        bot.answer_callback_query(call.id, "✅ Demo shown! 📸")
    else:
        bot.send_message(user_id, "❌ *No demo available yet.* 📚\n\nContact admin for updates. 🔧", parse_mode="MARKDOWN")
        bot.answer_callback_query(call.id)

@bot.callback_query_handler(func=lambda call: call.data == "submit_task")
@safe_execute
def request_screenshot(call):
    user_id = call.from_user.id
    # Ensure user data exists
    user_data = db.get_user(user_id)
    if not user_data:
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start to create account. 🚀", parse_mode="MARKDOWN")
        bot.answer_callback_query(call.id, "Account issue. Use /start. 🔄")
        return
    bot.answer_callback_query(call.id)
    bot.send_message(user_id, "📸 *Send Screenshot* 📷\n\nSubmit your task completion screenshot as proof:\n\n*Make sure:*\n- Username/email is visible 👀\n- Image is clear 📷\n- Shows completion proof (like demo) ✅", parse_mode="MARKDOWN")
    bot.register_next_step_handler(call.message, handle_screenshot)

@bot.callback_query_handler(func=lambda call: call.data == "back_to_menu")
@safe_execute
def back_to_menu(call):
    user_id = call.from_user.id
    try:
        bot.delete_message(call.message.chat.id, call.message.message_id)
    except:
        pass
    bot.send_message(user_id, "↩️ Back to main menu. 🌟", reply_markup=main_menu(), parse_mode="MARKDOWN")
    bot.answer_callback_query(call.id)

def handle_screenshot(message):
    # Ensure it's a valid message with user
    if not hasattr(message, 'from_user') or not hasattr(message, 'chat'):
        logger.error("❌ Invalid message in handle_screenshot")
        return
    
    user_id = message.from_user.id
    
    # Re-fetch user data to ensure it's there
    user_data = db.get_user(user_id)
    if not user_data:
        logger.error(f"❌ Account not found in screenshot for {user_id}")
        bot.send_message(user_id, "❌ *Account Not Found* 🔍\n\nUse /start to recreate account. 🚀", parse_mode="MARKDOWN")
        # Force recreate if missing
        db.create_user(user_id, message.from_user.first_name, message.from_user.username)
        bot.send_message(user_id, "🔄 Account recreated. Please try submitting again. 📱", parse_mode="MARKDOWN")
        return
    
    if message.content_type == 'photo':
        today = date.today()
        today_start = datetime.combine(today, datetime.min.time())
        today_end = datetime.combine(today, datetime.max.time())
        
        # Check daily limit
        pending_count = db.get_today_submission_count(user_id)
        completed_count = db.get_today_approved_count(user_id)
        
        if pending_count > 0:
            bot.send_message(user_id, "⏰ *Already Submitted Today!* 📱\n\nWait for approval or try again tomorrow. 🔄", parse_mode="MARKDOWN")
            return
        
        if completed_count > 0:
            bot.send_message(user_id, "✅ *Task Completed Today!* 🎉\n\nCome back tomorrow for new task! 🚀", parse_mode="MARKDOWN")
            return
        
        # Save submission
        try:
            submission_date = datetime.now()
            photo_id = message.photo[-1].file_id
            sub_doc = {
                'user_id': user_id,
                'photo_id': photo_id,
                'status': 'pending',
                'submission_date': submission_date
            }
            result = db.collections['task_submissions'].insert_one(sub_doc)
            submission_id = str(result.inserted_id)
        except Exception as e:
            logger.error(f"❌ DB insert error in screenshot: {e}\n{traceback.format_exc()}")
            bot.send_message(user_id, "❌ Submission failed. Try again. 🔄", parse_mode="MARKDOWN")
            return
        
        # Send demo after submission as reminder
        demo = db.get_demo_video()
        if demo:
            media_id = demo['video_id']
            media_type = demo.get('media_type', 'video')
            caption = "Great submission! Remember, demos like this help approvals go faster. 🚀"
            try:
                if media_type == "photo":
                    bot.send_photo(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
                else:
                    bot.send_video(user_id, media_id, caption=caption, parse_mode="MARKDOWN")
            except:
                pass  # Non-critical
        
        bot.send_message(user_id, 
            f"""✅ *Submission Received!* 📸

*🆔 Submission ID:* #{submission_id}
*⏳ Status:* Pending Approval 📋
*💰 Reward:* ₹{TASK_BONUS} on approval! 🎁
*⏰ Time:* {submission_date.strftime('%Y-%m-%d %H:%M:%S')}

*📝 Note:*
- Approval time: 1-12 hours ⏳
- You'll be notified when approved 🔔
- Come back tomorrow for new task! 🚀""",
            reply_markup=main_menu(), parse_mode="MARKDOWN")
        
        # Notify admin - FIXED: Separate try-excepts for send_message and forward_message, use HTML for reliability
        admin_msg_html = f"""<b>📸 New Task Submission #{submission_id}</b> 📱

🆔 <b>User ID:</b> <code>{user_id}</code>
👤 <b>Username:</b> @{db.get_user_field_safe(user_data, 'username', '') or 'No username'}
⏰ <b>Time:</b> {submission_date.strftime('%Y-%m-%d %H:%M:%S')}"""
        
        markup = types.InlineKeyboardMarkup(row_width=2)
        markup.add(
            types.InlineKeyboardButton("✅ Approve", callback_data=f"task_approve_{submission_id}"),
            types.InlineKeyboardButton("❌ Reject", callback_data=f"task_reject_{submission_id}")
        )
        
        try:
            bot.send_message(ADMIN_ID, admin_msg_html, reply_markup=markup, parse_mode="HTML")
            logger.info(f"✅ Admin notification sent for submission #{submission_id}")
        except Exception as send_e:
            logger.error(f"❌ Failed to send admin notification for #{submission_id}: {send_e}\n{traceback.format_exc()}")
            # Fallback: Send plain text without markup
            try:
                bot.send_message(ADMIN_ID, f"🚨 EMERGENCY: New Task Submission #{submission_id} - User ID: {user_id}. Check DB manually!", parse_mode="MARKDOWN")
                logger.info("✅ Fallback admin notification sent")
            except fallback_e:
                logger.error(f"❌ Fallback notification also failed for #{submission_id}: {fallback_e}")
        
        # Separate forward for screenshot
        try:
            bot.forward_message(ADMIN_ID, message.chat.id, message.message_id)
            logger.info(f"✅ Screenshot forwarded to admin for #{submission_id}")
        except Exception as forward_e:
            logger.error(f"❌ Failed to forward screenshot for #{submission_id}: {forward_e}\n{traceback.format_exc()}")
            # Fallback: Send the photo directly if forward fails
            try:
                bot.send_photo(ADMIN_ID, message.photo[-1].file_id, caption=f"Screenshot for submission #{submission_id} (User: {user_id})", parse_mode="MARKDOWN")
                logger.info("✅ Fallback photo sent to admin")
            except photo_e:
                logger.error(f"❌ Fallback photo send also failed for #{submission_id}: {photo_e}")
        
    else:
        bot.send_message(user_id, "❌ *Please send a screenshot photo.* 📷\n\nMake sure it shows task completion proof clearly (like the demo). ✅\n\n*🔄 Try again:*", parse_mode="MARKDOWN")
        bot.register_next_step_handler(message, handle_screenshot)

@bot.callback_query_handler(func=lambda call: call.data.startswith(('task_approve_', 'task_reject_')))
@safe_execute
def handle_task_approval(call):
    if call.from_user.id != ADMIN_ID:
        bot.answer_callback_query(call.id, "❌ Admin only. 👑", show_alert=True)
        return
    
    action = call.data.split('_')[1]
    submission_id = call.data.split('_')[2]
    
    try:
        submission = db.collections['task_submissions'].find_one({'_id': ObjectId(submission_id)})
        
        if not submission:
            bot.answer_callback_query(call.id, "❌ Submission not found. 🔍", show_alert=True)
            return
        
        user_id = submission['user_id']
        user_data = db.get_user(user_id)
        user_name = db.get_user_field_safe(user_data, 'first_name', "User") if user_data else "User"
        
        if action == 'approve':
            db.collections['task_submissions'].update_one(
                {'_id': ObjectId(submission_id)},
                {'$set': {'status': 'approved'}}
            )
            
            # Add points to user
            db.update_points(user_id, TASK_BONUS)
            # Increment task_completed
            current_tasks = db.get_user_field_safe(user_data, 'task_completed', 0)
            db.update_user(user_id, task_completed=current_tasks + 1)
            
            new_balance = db.get_user_field_safe(user_data, 'points', 0) + TASK_BONUS
            
            # Check for penalty restore
            penalty_msg = ""
            if user_data.get('has_penalty', False):
                penalty = user_data.get('deducted_amount', 0)
                db.update_points(user_id, penalty)
                db.update_user(user_id, has_penalty=False, deducted_amount=0)
                new_balance += penalty
                penalty_msg = f"\n\n💸 *Penalty Restored:* ₹{penalty:.2f} added back! (After rejoin + task) 🔄"
            
            bot.send_message(user_id, 
                f"""✅ *Task Approved!* 🎉

*🆔 Submission ID:* #{submission_id}
*💰 Reward:* ₹{TASK_BONUS} credited! 💸{penalty_msg}
*📊 New Balance:* ₹{new_balance:.2f} 🌟

*🎉 Congratulations!*
Task completed successfully. 
Come back tomorrow for new task! 🚀""", parse_mode="MARKDOWN")
            
            bot.edit_message_text(
                f"✅ *Approved Task #{submission_id}* 📱\n\n*👤 User:* {user_name} (ID: `{user_id}`)\n*💰 Reward:* ₹{TASK_BONUS}", 
                call.message.chat.id, 
                call.message.message_id, parse_mode="MARKDOWN"
            )
            bot.answer_callback_query(call.id, "✅ Approved! 🌟")
        
        else:
            db.collections['task_submissions'].update_one(
                {'_id': ObjectId(submission_id)},
                {'$set': {'status': 'rejected'}}
            )
            
            bot.send_message(user_id, 
                f"""❌ *Task Rejected* ⚠️

*🆔 Submission ID:* #{submission_id}
*📝 Reason:* Invalid screenshot 📸

*💡 Tips for next time:*
- Make sure username is visible 👀
- Clear and readable screenshot 📷
- Show completion proof (like demo) ✅

You can try again tomorrow. 🔄""", parse_mode="MARKDOWN")
            
            bot.edit_message_text(
                f"❌ *Rejected Task #{submission_id}* ⚠️\n\n*👤 User:* {user_name} (ID: `{user_id}`)", 
                call.message.chat.id, 
                call.message.message_id, parse_mode="MARKDOWN"
            )
            bot.answer_callback_query(call.id, "❌ Rejected. ⚠️")
    
    except Exception as e:
        logger.error(f"❌ Task approval error: {e}\n{traceback.format_exc()}")
        bot.answer_callback_query(call.id, "❌ Error processing. 🔧", show_alert=True)

# ==================== LEADERBOARD ====================
@bot.message_handler(func=lambda m: m.text == "🏆 Leaderboard")
@safe_execute
def leaderboard(message):
    user_id = message.from_user.id
    
    try:
        results = list(db.collections['users'].find(
            {'channel_joined': True},  # Only channel joined users
            {'first_name': 1, 'referral_count': 1, 'points': 1}
        ).sort([('referral_count', -1), ('points', -1)]).limit(10))
        
        leaderboard_text = f"🏆 *Top Referrers Leaderboard* 📈\n\n"
        
        if results:
            medals = ["🥇", "🥈", "🥉", "4️⃣", "5️⃣", "6️⃣", "7️⃣", "8️⃣", "9️⃣", "🔟"]
            for i, doc in enumerate(results):
                medal = medals[i] if i < len(medals) else f"{i+1}."
                # Hide points in leaderboard
                leaderboard_text += f"{medal} {doc.get('first_name', 'User')} - {doc.get('referral_count', 0)} refs\n"
        else:
            leaderboard_text += "No rankings yet. Start referring! 🚀"
        
        user_refs = db.get_referral_count(user_id)
        user_data = db.get_user(user_id)
        user_points = db.get_user_field_safe(user_data, 'points', 0)
        
        leaderboard_text += f"\n*📊 Your Stats:* 👤\n"
        leaderboard_text += f"• Referrals: {user_refs} 👥\n"
        leaderboard_text += f"• Balance: ₹{user_points:.2f} 💰\n"
        leaderboard_text += f"• Rank: Top {user_refs * 20}% 📊"
        
        markup = types.InlineKeyboardMarkup()
        markup.add(types.InlineKeyboardButton("👥 Invite Friends", callback_data="invite_friends"))
        
        bot.send_message(user_id, leaderboard_text, reply_markup=markup, parse_mode="MARKDOWN")
    except Exception as e:
        logger.error(f"❌ Leaderboard error: {e}")
        bot.send_message(user_id, "❌ Failed to load leaderboard. Try again. 🔄", parse_mode="MARKDOWN")

@bot.callback_query_handler(func=lambda call: call.data == "invite_friends")
@safe_execute
def invite_friends(call):
    user_id = call.from_user.id
    ref_link = f"https://t.me/{BOT_USERNAME}?start={user_id}"
    
    bot.send_message(user_id,
        f"""👥 *Invite Friends & Earn* 📈

*🔗 Your Referral Link:*
`{ref_link}` 📲

*💰 Earn ₹{REFERRAL_BONUS} per referral* 👤

*📤 Share this message:*""", parse_mode="MARKDOWN")
    
    markup = types.InlineKeyboardMarkup()
    markup.add(types.InlineKeyboardButton("📤 Share Link", 
        url=f"https://t.me/share/url?url={ref_link}&text=Join%20Rupeerush%20Bot%20to%20earn%20money%20daily!%20Free%20₹{WELCOME_BONUS}%20bonus!%20Use%20my%20link:%20{ref_link}"))
    
    bot.send_message(user_id, "Click below to share: 📤", reply_markup=markup, parse_mode="MARKDOWN")
    bot.answer_callback_query(call.id)

# ==================== BROADCAST SYSTEM ====================
ongoing_broadcast = {'id': None, 'cancelled': False}

def broadcast_worker(broadcast_id, users, content, media_id=None, media_type='text'):
    total = len(users)
    success = 0
    failed = 0
    
    try:
        progress_msg = bot.send_message(ADMIN_ID, f"📤 *Broadcast #{broadcast_id} Started* 🚀\n\n*📊 Total Users:* {total}\n*📈 Progress:* 0%\n*✅ Success:* 0\n*❌ Failed:* 0", parse_mode="MARKDOWN")
    except:
        logger.error("❌ Failed to send broadcast progress message")
        return
    
    for i, uid in enumerate(users):
        if ongoing_broadcast['cancelled']:
            db.update_broadcast_status(broadcast_id, 'cancelled', success, failed, total)
            try:
                bot.edit_message_text(f"❌ *Broadcast #{broadcast_id} Cancelled* 🛑\n\n*📊 Total:* {total}\n*✅ Success:* {success}\n*❌ Failed:* {failed}", progress_msg.chat.id, progress_msg.message_id, parse_mode="MARKDOWN")
            except:
                pass
            ongoing_broadcast['id'] = None
            return
        
        try:
            if db.is_user_in_channel(uid):
                if media_id:
                    if media_type == 'video':
                        if content:
                            bot.send_video(uid, media_id, caption=content, parse_mode="HTML")
                        else:
                            bot.send_video(uid, media_id, parse_mode="HTML")
                    else:  # photo or other
                        if content:
                            bot.send_photo(uid, media_id, caption=content, parse_mode="HTML")
                        else:
                            bot.send_photo(uid, media_id, parse_mode="HTML")
                elif content:
                    bot.send_message(uid, content, parse_mode="HTML")
                success += 1
            else:
                failed += 1
        except Exception as e:
            logger.error(f"❌ Broadcast error for {uid}: {e}")
            failed += 1
        
        # Update progress every 10 users or at the end
        if i % 10 == 0 or i == total - 1:
            progress = int((i + 1) / total * 100)
            try:
                bot.edit_message_text(
                    f"📤 *Broadcast #{broadcast_id}* 🚀\n\n*📊 Total:* {total}\n*📈 Progress:* {progress}%\n*✅ Success:* {success}\n*❌ Failed:* {failed}", 
                    progress_msg.chat.id, 
                    progress_msg.message_id, parse_mode="MARKDOWN"
                )
            except:
                pass  # Ignore edit fails
        time.sleep(0.2)  # Rate limiting
    
    db.update_broadcast_status(broadcast_id, 'completed', success, failed, total)
    success_rate = int(success/total*100) if total > 0 else 0
    try:
        bot.edit_message_text(
            f"✅ *Broadcast #{broadcast_id} Completed!* 🎉\n\n*📊 Total:* {total}\n*✅ Success:* {success}\n*❌ Failed:* {failed}\n*📈 Success Rate:* {success_rate}% 🌟", 
            progress_msg.chat.id, 
            progress_msg.message_id, parse_mode="MARKDOWN"
        )
    except:
        pass
    ongoing_broadcast['id'] = None

@bot.message_handler(commands=['broadcast'])
@safe_execute
def admin_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if ongoing_broadcast['id']:
        bot.send_message(ADMIN_ID, "❌ *Broadcast Running* 📤\n\nUse /cancel to stop current broadcast. 🛑", parse_mode="MARKDOWN")
        return
    
    broadcast_text = f"""📢 *Broadcast System* 🚀

*📋 Options:*
- Send text message for text broadcast 📝
- Send photo with caption for photo broadcast 📸  
- Send video with caption for video broadcast 🎥
- Send /cancel to cancel broadcast 🛑

*💡 Tip for Stylish Fonts:* Use HTML tags like &lt;b&gt;bold&lt;/b&gt;, &lt;i&gt;italic&lt;/i&gt;, or &lt;u&gt;underline&lt;/u&gt; in your text/caption for formatting! 🎨

*📤 Send your broadcast content now:*"""
    
    bot.send_message(ADMIN_ID, broadcast_text, parse_mode="MARKDOWN")
    bot.register_next_step_handler(message, process_broadcast_content)

def process_broadcast_content(message):
    if message.from_user.id != ADMIN_ID:
        return
    
    if message.text == '/cancel':
        bot.send_message(ADMIN_ID, "❌ Broadcast cancelled. 🛑", parse_mode="MARKDOWN")
        return
    
    broadcast_id = db.add_broadcast('mixed')
    if not broadcast_id:
        bot.send_message(ADMIN_ID, "❌ Failed to start broadcast. 🔧", parse_mode="MARKDOWN")
        return
    ongoing_broadcast['id'] = broadcast_id
    ongoing_broadcast['cancelled'] = False
    
    users = db.get_all_users()
    
    content = None
    media_id = None
    media_type = 'text'
    
    if message.content_type == 'text':
        content = message.text
        media_type = 'text'
    elif message.content_type == 'photo':
        media_id = message.photo[-1].file_id
        content = message.caption if message.caption else ""
        media_type = 'photo'
    elif message.content_type == 'video':
        media_id = message.video.file_id
        content = message.caption if message.caption else ""
        media_type = 'video'
    else:
        bot.send_message(ADMIN_ID, "❌ *Unsupported content type* ⚠️\n\nOnly text, photos, and videos supported. 📝📸🎥", parse_mode="MARKDOWN")
        ongoing_broadcast['id'] = None  # Reset if error
        return
    
    bot.send_message(ADMIN_ID, f"🚀 *Starting broadcast to {len(users)} users...* 📤", parse_mode="MARKDOWN")
    
    # Start broadcast in separate thread
    threading.Thread(target=broadcast_worker, args=(broadcast_id, users, content, media_id, media_type), daemon=True).start()

@bot.message_handler(commands=['cancel'])
@safe_execute
def cancel_broadcast(message):
    if message.from_user.id != ADMIN_ID:
        return
    if ongoing_broadcast['id']:
        ongoing_broadcast['cancelled'] = True
        bot.send_message(ADMIN_ID, f"🛑 *Broadcast #{ongoing_broadcast['id']} Cancellation Initiated* ❌", parse_mode="MARKDOWN")
    else:
        bot.send_message(ADMIN_ID, "❌ *No broadcast running* 📤", parse_mode="MARKDOWN")

# ==================== CHANNEL PENALTY & WITHDRAWAL CHECKERS ====================
def check_withdrawals():
    while True:
        try:
            pending_withdrawals = db.get_pending_withdrawals()
            for wd in pending_withdrawals:
                user_id = wd['user_id']
                if not db.is_user_in_channel(user_id):
                    withdrawal_id = str(wd['_id'])
                    amount = wd['amount']
                    # Refund points
                    db.update_points(user_id, amount)
                    db.update_withdrawal_status(withdrawal_id, 'rejected', ADMIN_ID)
                    
                    try:
                        bot.send_message(user_id, 
                            f"""❌ *Withdrawal Auto-Rejected* ⚠️

*🆔 Request ID:* #{withdrawal_id}
*💰 Amount:* ₹{amount:.2f}
*📝 Reason:* Left the channel 📢

*💸 Action:* ₹{amount:.2f} refunded to your balance! 🔄

*✅ To withdraw:*
1. Rejoin channel: {CHANNEL_LINK} 👥
2. Stay in channel until withdrawal processed ⏳""", 
                            disable_web_page_preview=True, parse_mode="MARKDOWN")
                    except:
                        pass
                    
                    try:
                        bot.send_message(ADMIN_ID, 
                            f"❌ *Auto-Rejected #{withdrawal_id}* ⚠️\n\nUser left channel. Amount refunded. 💸", parse_mode="MARKDOWN")
                    except:
                        pass
                    
            time.sleep(300)  # Check every 5 minutes
        except Exception as e:
            logger.error(f"❌ Withdrawal check error: {e}\n{traceback.format_exc()}")
            time.sleep(300)

def check_channel_penalties():
    while True:
        try:
            users = db.get_all_users()
            for uid in users:
                if not db.is_user_in_channel(uid):
                    user_data = db.get_user(uid)
                    if user_data and user_data.get('points', 0) > 0 and not user_data.get('has_penalty', False):
                        deduct = user_data['points']
                        db.update_points(uid, -deduct)
                        db.update_user(uid, has_penalty=True, deducted_amount=deduct)
                        
                        try:
                            bot.send_message(uid, 
                                f"""💸 *Penalty Applied!* ❌

You left our channel, so your entire balance of ₹{deduct:.2f} has been deducted.

To get it back:
1. [Rejoin Channel Now]({CHANNEL_LINK}) 👥
2. Complete the 📱 Signup Task (must be approved) ✅

After rejoining and task approval, your money will be restored automatically! 🔄""",
                                disable_web_page_preview=True, reply_markup=types.InlineKeyboardMarkup().add(types.InlineKeyboardButton("👉 Rejoin Channel", url=CHANNEL_LINK)), parse_mode="MARKDOWN")
                        except:
                            pass
            time.sleep(600)  # Check every 10 minutes for penalties
        except Exception as e:
            logger.error(f"❌ Penalty check error: {e}\n{traceback.format_exc()}")
            time.sleep(600)

# Start threads
# threading.Thread(target=check_withdrawals, daemon=True).start()  # Disabled auto-reject
threading.Thread(target=check_channel_penalties, daemon=True).start()

# ==================== ANTI-CRASH SYSTEM ====================
def start_bot():
    logger.info("🎯 Bot Starting...")
    logger.info("✅ All Features Loaded")
    logger.info("🔒 Security System Active (No Verification)")
    logger.info("💰 Withdrawal System Ready (UPI Only)")
    logger.info("📢 Broadcast  Initialized (Supports Video)")
    logger.info("🎬 Tutorial System Ready")
    logger.info("📸 Demo System Active")
    logger.info("⚠️ Channel Penalty System Active")
    logger.info("👥 Referral Count: Channel Joined Only")
    logger.info("🚀 Bot Ready to Use!")
    
    while True:
        try:
            bot.polling(none_stop=True, timeout=60)
        except Exception as e:
            logger.error(f"❌ Bot Crashed: {e}\n{traceback.format_exc()}")
            logger.info("🔄 Restarting in 10 seconds...")
            time.sleep(10)

if __name__ == "__main__":
    start_bot()
