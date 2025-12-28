# database/users_chats_db.py - Smart Bot (Admin/Premium Only)

from pymongo import MongoClient
from datetime import datetime, timedelta
from info import (
    BOT_ID, DATABASE_NAME, DATA_DATABASE_URL, 
    FILES_DATABASE_URL, SECOND_FILES_DATABASE_URL,
    WELCOME_TEXT, FILE_CAPTION, 
    WELCOME, SPELL_CHECK, PROTECT_CONTENT, 
    AUTO_DELETE
)

# Database Connections
files_db_client = MongoClient(FILES_DATABASE_URL)
files_db = files_db_client[DATABASE_NAME]

data_db_client = MongoClient(DATA_DATABASE_URL)
data_db = data_db_client[DATABASE_NAME]

if SECOND_FILES_DATABASE_URL:
    second_files_db_client = MongoClient(SECOND_FILES_DATABASE_URL)
    second_files_db = second_files_db_client[DATABASE_NAME]


class Database:
    """Smart Bot Database - Admin/Premium Auto-filter + Public Group Management"""
    
    # Default Settings for Groups
    default_group_settings = {
        # Auto-filter settings (Admin/Premium only - for files search)
        'file_secure': PROTECT_CONTENT,
        'spell_check': SPELL_CHECK,
        'auto_delete': AUTO_DELETE,
        'caption': FILE_CAPTION,
        
        # Group Management settings (Public users - no file access)
        'welcome': WELCOME,
        'welcome_text': WELCOME_TEXT,
        'antiflood': False,
        'antiflood_limit': 5,
        'filters_enabled': True,
        'notes_enabled': True,
        'rules': '',
        'log_channel': None,
        'antibot': False,
        'antispam': False
    }
    
    # User roles: admin, premium, public
    default_user_role = {
        'role': 'public',  # admin, premium, public
        'premium_expire': None,
        'premium_start': None,
        'trial_used': False,
        'search_access': []  # Which collections user can search: primary, clouds, archive
    }

    def __init__(self):
        # Collections
        self.col = data_db.Users              # Users data
        self.grp = data_db.Groups             # Groups data
        self.req = data_db.Requests           # Join requests
        self.con = data_db.Connections        # User-group connections
        self.stg = data_db.Settings           # Bot settings
        self.notes = data_db.Notes            # Group notes (for group management)
        self.filters = data_db.Filters        # Group filters (for group management)
        
        # Create indexes
        self._create_indexes()
    
    def _create_indexes(self):
        """Create necessary indexes"""
        # User indexes
        self.col.create_index('id', unique=True)
        self.col.create_index('role.role')
        self.col.create_index('role.premium_expire')
        
        # Group indexes
        self.grp.create_index('id', unique=True)
        
        # Notes indexes
        self.notes.create_index([('group_id', 1), ('note_name', 1)], unique=True)
        
        # Filters indexes
        self.filters.create_index([('group_id', 1), ('keyword', 1)], unique=True)

    # ==================== User Methods ====================
    
    def new_user(self, id, name):
        """Create new user document"""
        return dict(
            id=id,
            name=name,
            username=None,
            joined_date=datetime.now(),
            last_active=datetime.now(),
            ban_status=dict(
                is_banned=False,
                ban_reason="",
            ),
            role=self.default_user_role.copy()
        )

    async def add_user(self, id, name, username=None):
        """Add new user"""
        user = self.new_user(id, name)
        if username:
            user['username'] = username
        try:
            self.col.insert_one(user)
            return True
        except:
            return False
    
    async def is_user_exist(self, id):
        """Check if user exists"""
        user = self.col.find_one({'id': int(id)})
        return bool(user)
    
    async def get_user(self, id):
        """Get user data"""
        return self.col.find_one({'id': int(id)})
    
    async def update_user(self, id, update_data):
        """Update user data"""
        self.col.update_one({'id': int(id)}, {'$set': update_data})
    
    async def total_users_count(self):
        """Get total users count"""
        return self.col.count_documents({})
    
    async def get_all_users(self):
        """Get all users"""
        return self.col.find({})
    
    async def delete_user(self, user_id):
        """Delete user"""
        self.col.delete_many({'id': int(user_id)})
    
    async def update_last_active(self, id):
        """Update user's last active time"""
        self.col.update_one(
            {'id': int(id)},
            {'$set': {'last_active': datetime.now()}}
        )

    # ==================== User Role Management ====================
    
    async def is_admin(self, user_id):
        """Check if user is admin (has file search access)"""
        user = await self.get_user(user_id)
        if not user:
            return False
        return user.get('role', {}).get('role') == 'admin'
    
    async def is_premium(self, user_id):
        """Check if user has premium (has file search access)"""
        user = await self.get_user(user_id)
        if not user:
            return False
        
        role_info = user.get('role', {})
        
        # Admins always have premium
        if role_info.get('role') == 'admin':
            return True
        
        # Check premium status
        if role_info.get('role') == 'premium':
            expire = role_info.get('premium_expire')
            if not expire:  # Lifetime premium
                return True
            if expire > datetime.now():
                return True
            else:
                # Expired - downgrade to public
                await self.set_role(user_id, 'public')
                return False
        
        return False
    
    async def has_file_access(self, user_id):
        """Check if user can search files (admin or premium)"""
        return await self.is_admin(user_id) or await self.is_premium(user_id)
    
    async def set_role(self, user_id, role, expire_date=None):
        """Set user role (admin, premium, public)
        
        Args:
            user_id: User ID
            role: 'admin', 'premium', or 'public'
            expire_date: datetime for premium expiry (None = lifetime)
        """
        role_data = {
            'role': role,
            'premium_expire': expire_date if role == 'premium' else None,
            'premium_start': datetime.now() if role == 'premium' else None,
            'trial_used': False
        }
        
        # Set search access based on role
        if role == 'admin':
            role_data['search_access'] = ['primary', 'clouds', 'archive']
        elif role == 'premium':
            role_data['search_access'] = ['primary', 'clouds', 'archive']
        else:  # public - no file access
            role_data['search_access'] = []
        
        self.col.update_one(
            {'id': int(user_id)},
            {'$set': {'role': role_data}}
        )
    
    async def get_user_role(self, user_id):
        """Get user role"""
        user = await self.get_user(user_id)
        if not user:
            return 'public'
        return user.get('role', {}).get('role', 'public')
    
    async def get_search_access(self, user_id):
        """Get which collections user can access"""
        user = await self.get_user(user_id)
        if not user:
            return []
        return user.get('role', {}).get('search_access', [])
    
    async def get_all_admins(self):
        """Get all admin users"""
        return list(self.col.find({'role.role': 'admin'}))
    
    async def get_all_premium(self):
        """Get all premium users"""
        return list(self.col.find({'role.role': 'premium'}))
    
    async def get_user_stats(self):
        """Get user statistics"""
        total = await self.total_users_count()
        admins = self.col.count_documents({'role.role': 'admin'})
        premium = self.col.count_documents({'role.role': 'premium'})
        public = self.col.count_documents({'role.role': 'public'})
        
        return {
            'total': total,
            'admins': admins,
            'premium': premium,
            'public': public
        }
    
    async def get_premium_count(self):
        """Get premium users count"""
        return self.col.count_documents({'role.role': 'premium'})

    # ==================== Ban Methods ====================
    
    async def remove_ban(self, id):
        """Remove ban from user"""
        ban_status = dict(
            is_banned=False,
            ban_reason=''
        )
        self.col.update_one({'id': id}, {'$set': {'ban_status': ban_status}})
    
    async def ban_user(self, user_id, ban_reason="No Reason"):
        """Ban user"""
        ban_status = dict(
            is_banned=True,
            ban_reason=ban_reason
        )
        self.col.update_one({'id': user_id}, {'$set': {'ban_status': ban_status}})

    async def get_ban_status(self, id):
        """Get user ban status"""
        default = dict(
            is_banned=False,
            ban_reason=''
        )
        user = self.col.find_one({'id': int(id)})
        if not user:
            return default
        return user.get('ban_status', default)

    async def get_banned(self):
        """Get all banned users and chats"""
        users = self.col.find({'ban_status.is_banned': True})
        chats = self.grp.find({'chat_status.is_disabled': True})
        b_chats = [chat['id'] for chat in chats]
        b_users = [user['id'] for user in users]
        return b_users, b_chats

    # ==================== Group Methods ====================
    
    def new_group(self, id, title):
        """Create new group document"""
        return dict(
            id=id,
            title=title,
            added_date=datetime.now(),
            chat_status=dict(
                is_disabled=False,
                reason="",
            ),
            settings=self.default_group_settings.copy()
        )

    async def add_chat(self, chat, title):
        """Add new group"""
        chat_doc = self.new_group(chat, title)
        try:
            self.grp.insert_one(chat_doc)
            return True
        except:
            return False

    async def get_chat(self, chat):
        """Get chat status"""
        chat_doc = self.grp.find_one({'id': int(chat)})
        return False if not chat_doc else chat_doc.get('chat_status')
    
    async def get_chat_full(self, chat):
        """Get full chat document"""
        return self.grp.find_one({'id': int(chat)})
    
    async def delete_chat(self, grp_id):
        """Delete group and all its data"""
        self.grp.delete_many({'id': int(grp_id)})
        # Also delete notes and filters
        self.notes.delete_many({'group_id': int(grp_id)})
        self.filters.delete_many({'group_id': int(grp_id)})
    
    async def total_chat_count(self):
        """Get total chats count"""
        return self.grp.count_documents({})
    
    async def get_all_chats(self):
        """Get all chats"""
        return self.grp.find({})
    
    async def get_all_chats_count(self):
        """Get all chats count"""
        return self.grp.count_documents({})

    # ==================== Group Status Methods ====================
    
    async def re_enable_chat(self, id):
        """Re-enable disabled chat"""
        chat_status = dict(
            is_disabled=False,
            reason="",
        )
        self.grp.update_one({'id': int(id)}, {'$set': {'chat_status': chat_status}})
    
    async def disable_chat(self, chat, reason="No Reason"):
        """Disable chat"""
        chat_status = dict(
            is_disabled=True,
            reason=reason,
        )
        self.grp.update_one({'id': int(chat)}, {'$set': {'chat_status': chat_status}})

    # ==================== Group Settings Methods ====================
    
    async def update_settings(self, id, settings):
        """Update group settings"""
        self.grp.update_one({'id': int(id)}, {'$set': {'settings': settings}})
    
    async def get_settings(self, id):
        """Get group settings"""
        chat = self.grp.find_one({'id': int(id)})
        if chat:
            return chat.get('settings', self.default_group_settings)
        return self.default_group_settings

    # ==================== Join Request Methods ====================
    
    def find_join_req(self, id):
        """Find join request"""
        return bool(self.req.find_one({'id': id}))

    def add_join_req(self, id):
        """Add join request"""
        try:
            self.req.insert_one({'id': id, 'date': datetime.now()})
            return True
        except:
            return False

    def del_join_req(self):
        """Delete all join requests"""
        self.req.drop()
    
    def get_all_join_reqs(self):
        """Get all pending join requests"""
        return list(self.req.find({}))

    # ==================== Connection Methods ====================
    
    def add_connect(self, group_id, user_id):
        """Add user-group connection"""
        user = self.con.find_one({'_id': user_id})
        if user:
            if group_id not in user["group_ids"]:
                self.con.update_one(
                    {'_id': user_id},
                    {"$push": {"group_ids": group_id}}
                )
        else:
            self.con.insert_one({'_id': user_id, 'group_ids': [group_id]})

    def get_connections(self, user_id):
        """Get user's connected groups"""
        user = self.con.find_one({'_id': user_id})
        if user:
            return user["group_ids"]
        else:
            return []
    
    def remove_connection(self, group_id, user_id):
        """Remove user-group connection"""
        self.con.update_one(
            {'_id': user_id},
            {"$pull": {"group_ids": group_id}}
        )

    # ==================== Database Stats Methods ====================
    
    async def get_files_db_size(self):
        """Get files database size"""
        return (files_db.command("dbstats"))['dataSize']
    
    async def get_second_files_db_size(self):
        """Get second files database size"""
        if SECOND_FILES_DATABASE_URL:
            return (second_files_db.command("dbstats"))['dataSize']
        return 0
    
    async def get_data_db_size(self):
        """Get data database size"""
        return (data_db.command("dbstats"))['dataSize']
    
    async def get_all_stats(self):
        """Get complete bot statistics"""
        user_stats = await self.get_user_stats()
        
        return {
            'users': user_stats,
            'groups': await self.total_chat_count(),
            'files_db_size': await self.get_files_db_size(),
            'data_db_size': await self.get_data_db_size()
        }

    # ==================== Bot Settings Methods ====================
    
    def update_bot_sttgs(self, var, val):
        """Update bot settings"""
        if not self.stg.find_one({'id': BOT_ID}):
            self.stg.insert_one({'id': BOT_ID, var: val})
        self.stg.update_one({'id': BOT_ID}, {'$set': {var: val}})

    def get_bot_sttgs(self):
        """Get bot settings"""
        return self.stg.find_one({'id': BOT_ID})


# Initialize database
db = Database()
