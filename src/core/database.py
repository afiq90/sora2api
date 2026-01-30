"""Database storage layer - PostgreSQL version"""
import asyncpg
import asyncio
import json
import os
from datetime import datetime
from typing import Optional, List
from urllib.parse import urlparse
from .models import Token, TokenStats, Task, RequestLog, AdminConfig, ProxyConfig, WatermarkFreeConfig, CacheConfig, GenerationConfig, TokenRefreshConfig


class Database:
    """PostgreSQL database manager"""

    def __init__(self, db_url: str = None):
        if db_url is None:
            db_url = os.environ.get("DATABASE_URL")
        self.db_url = db_url
        self._pool: Optional[asyncpg.Pool] = None

        if self.db_url:
            try:
                parsed = urlparse(self.db_url)
                print(
                    f"Database configured: host={parsed.hostname}, port={parsed.port}, db={parsed.path[1:] if parsed.path else 'unknown'}"
                )
            except Exception as e:
                print(f"Warning: Could not parse DATABASE_URL: {e}")

    async def get_pool(self) -> asyncpg.Pool:
        """Get or create connection pool with retry logic"""
        if self._pool is None:
            max_retries = 5
            retry_delay = 2

            for attempt in range(max_retries):
                try:
                    print(
                        f"Attempting database connection (attempt {attempt + 1}/{max_retries})..."
                    )
                    self._pool = await asyncpg.create_pool(self.db_url,
                                                           min_size=1,
                                                           max_size=10,
                                                           command_timeout=60,
                                                           timeout=30)
                    print("Database connection pool created successfully")
                    break
                except Exception as e:
                    error_msg = str(e)
                    print(
                        f"Database connection attempt {attempt + 1} failed: {error_msg}"
                    )

                    if attempt < max_retries - 1:
                        wait_time = retry_delay * (2**attempt)
                        print(f"Retrying in {wait_time} seconds...")
                        await asyncio.sleep(wait_time)
                    else:
                        print(f"All {max_retries} connection attempts failed")
                        raise Exception(
                            f"Failed to connect to database after {max_retries} attempts: {error_msg}"
                        )

        return self._pool

    async def close(self):
        """Close the connection pool"""
        if self._pool:
            await self._pool.close()
            self._pool = None

    def db_exists(self) -> bool:
        """Check if database connection is configured"""
        return self.db_url is not None and len(self.db_url) > 0

    async def _table_exists(self, conn, table_name: str) -> bool:
        """Check if a table exists in the database"""
        result = await conn.fetchval(
            "SELECT EXISTS(SELECT 1 FROM information_schema.tables WHERE table_name = $1)",
            table_name)
        return result

    async def _column_exists(self, conn, table_name: str,
                             column_name: str) -> bool:
        """Check if a column exists in a table"""
        try:
            result = await conn.fetchval(
                """SELECT EXISTS(
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name = $1 AND column_name = $2
                )""", table_name, column_name)
            return result
        except:
            return False

    async def _ensure_config_rows(self, conn, config_dict: dict = None):
        """Ensure all config tables have their default rows"""
        count = await conn.fetchval("SELECT COUNT(*) FROM admin_config")
        if count == 0:
            admin_username = "admin"
            admin_password = "admin"
            api_key = "han1234"
            error_ban_threshold = 3
            task_retry_enabled = True
            task_max_retries = 3
            auto_disable_on_401 = True

            if config_dict:
                global_config = config_dict.get("global", {})
                admin_username = global_config.get("admin_username", "admin")
                admin_password = global_config.get("admin_password", "admin")
                api_key = global_config.get("api_key", "han1234")

                admin_config = config_dict.get("admin", {})
                error_ban_threshold = admin_config.get("error_ban_threshold", 3)
                task_retry_enabled = admin_config.get("task_retry_enabled", True)
                task_max_retries = admin_config.get("task_max_retries", 3)
                auto_disable_on_401 = admin_config.get("auto_disable_on_401", True)

            await conn.execute("""
                INSERT INTO admin_config (id, admin_username, admin_password, api_key, error_ban_threshold, task_retry_enabled, task_max_retries, auto_disable_on_401)
                VALUES (1, $1, $2, $3, $4, $5, $6, $7)
            """, admin_username, admin_password, api_key, error_ban_threshold, task_retry_enabled, task_max_retries, auto_disable_on_401)

        count = await conn.fetchval("SELECT COUNT(*) FROM proxy_config")
        if count == 0:
            proxy_enabled = False
            proxy_url = None

            if config_dict:
                proxy_config = config_dict.get("proxy", {})
                proxy_enabled = proxy_config.get("proxy_enabled", False)
                proxy_url = proxy_config.get("proxy_url", "")
                proxy_url = proxy_url if proxy_url else None

            await conn.execute(
                """
                INSERT INTO proxy_config (id, proxy_enabled, proxy_url)
                VALUES (1, $1, $2)
            """, proxy_enabled, proxy_url)

        count = await conn.fetchval(
            "SELECT COUNT(*) FROM watermark_free_config")
        if count == 0:
            watermark_free_enabled = False
            parse_method = "third_party"
            custom_parse_url = None
            custom_parse_token = None
            fallback_on_failure = True

            if config_dict:
                watermark_config = config_dict.get("watermark_free", {})
                watermark_free_enabled = watermark_config.get(
                    "watermark_free_enabled", False)
                parse_method = watermark_config.get("parse_method",
                                                    "third_party")
                custom_parse_url = watermark_config.get("custom_parse_url", "")
                custom_parse_token = watermark_config.get("custom_parse_token", "")
                fallback_on_failure = watermark_config.get("fallback_on_failure", True)

                # Convert empty strings to None
                custom_parse_url = custom_parse_url if custom_parse_url else None
                custom_parse_token = custom_parse_token if custom_parse_token else None

            await conn.execute("""
                INSERT INTO watermark_free_config (id, watermark_free_enabled, parse_method, custom_parse_url, custom_parse_token, fallback_on_failure)
                VALUES (1, $1, $2, $3, $4, $5)
            """, watermark_free_enabled, parse_method, custom_parse_url, custom_parse_token, fallback_on_failure)

        count = await conn.fetchval("SELECT COUNT(*) FROM cache_config")
        if count == 0:
            cache_enabled = False
            cache_timeout = 600
            cache_base_url = None

            if config_dict:
                cache_config = config_dict.get("cache", {})
                cache_enabled = cache_config.get("enabled", False)
                cache_timeout = cache_config.get("timeout", 600)
                cache_base_url = cache_config.get("base_url", "")
                cache_base_url = cache_base_url if cache_base_url else None

            await conn.execute(
                """
                INSERT INTO cache_config (id, cache_enabled, cache_timeout, cache_base_url)
                VALUES (1, $1, $2, $3)
            """, cache_enabled, cache_timeout, cache_base_url)

        count = await conn.fetchval("SELECT COUNT(*) FROM generation_config")
        if count == 0:
            image_timeout = 300
            video_timeout = 3000

            if config_dict:
                generation_config = config_dict.get("generation", {})
                image_timeout = generation_config.get("image_timeout", 300)
                video_timeout = generation_config.get("video_timeout", 3000)

            await conn.execute(
                """
                INSERT INTO generation_config (id, image_timeout, video_timeout)
                VALUES (1, $1, $2)
            """, image_timeout, video_timeout)

        count = await conn.fetchval("SELECT COUNT(*) FROM token_refresh_config")
        if count == 0:
            at_auto_refresh_enabled = False

            if config_dict:
                token_refresh_config = config_dict.get("token_refresh", {})
                at_auto_refresh_enabled = token_refresh_config.get(
                    "at_auto_refresh_enabled", False)

            await conn.execute(
                """
                INSERT INTO token_refresh_config (id, at_auto_refresh_enabled)
                VALUES (1, $1)
            """, at_auto_refresh_enabled)

        # Ensure call_logic_config has a row
        count = await conn.fetchval("SELECT COUNT(*) FROM call_logic_config")
        if count == 0:
            # Get call logic config from config_dict if provided, otherwise use defaults
            call_mode = "default"
            polling_mode_enabled = False

            if config_dict:
                call_logic_config = config_dict.get("call_logic", {})
                call_mode = call_logic_config.get("call_mode", "default")
                # Normalize call_mode
                if call_mode not in ("default", "polling"):
                    # Check legacy polling_mode_enabled field
                    polling_mode_enabled = call_logic_config.get("polling_mode_enabled", False)
                    call_mode = "polling" if polling_mode_enabled else "default"
                else:
                    polling_mode_enabled = call_mode == "polling"

            await conn.execute("""
                INSERT INTO call_logic_config (id, call_mode, polling_mode_enabled)
                VALUES (1, $1, $2)
            """, call_mode, polling_mode_enabled)

        # Ensure pow_proxy_config has a row
        count = await conn.fetchval("SELECT COUNT(*) FROM pow_proxy_config")
        if count == 0:
            # Get POW proxy config from config_dict if provided, otherwise use defaults
            pow_proxy_enabled = False
            pow_proxy_url = None

            if config_dict:
                pow_proxy_config = config_dict.get("pow_proxy", {})
                pow_proxy_enabled = pow_proxy_config.get("pow_proxy_enabled", False)
                pow_proxy_url = pow_proxy_config.get("pow_proxy_url", "")
                # Convert empty string to None
                pow_proxy_url = pow_proxy_url if pow_proxy_url else None

            await conn.execute("""
                INSERT INTO pow_proxy_config (id, pow_proxy_enabled, pow_proxy_url)
                VALUES (1, $1, $2)
            """, pow_proxy_enabled, pow_proxy_url)

    async def check_and_migrate_db(self, config_dict: dict = None):
        """Check database integrity and perform migrations if needed"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            print("Checking database integrity and performing migrations...")

            if await self._table_exists(conn, "tokens"):
                columns_to_add = [
                    ("sora2_supported", "BOOLEAN"),
                    ("sora2_invite_code", "TEXT"),
                    ("sora2_redeemed_count", "INTEGER DEFAULT 0"),
                    ("sora2_total_count", "INTEGER DEFAULT 0"),
                    ("sora2_remaining_count", "INTEGER DEFAULT 0"),
                    ("sora2_cooldown_until", "TIMESTAMP"),
                    ("image_enabled", "BOOLEAN DEFAULT TRUE"),
                    ("video_enabled", "BOOLEAN DEFAULT TRUE"),
                    ("image_concurrency", "INTEGER DEFAULT -1"),
                    ("video_concurrency", "INTEGER DEFAULT -1"),
                    ("client_id", "TEXT"),
                    ("proxy_url", "TEXT"),
                    ("is_expired", "BOOLEAN DEFAULT FALSE"),
                ]

                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(conn, "tokens", col_name):
                        try:
                            await conn.execute(
                                f"ALTER TABLE tokens ADD COLUMN {col_name} {col_type}"
                            )
                            print(
                                f"  Added column '{col_name}' to tokens table")
                        except Exception as e:
                            print(f"  Failed to add column '{col_name}': {e}")

            if await self._table_exists(conn, "token_stats"):
                columns_to_add = [
                    ("consecutive_error_count", "INTEGER DEFAULT 0"),
                ]

                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(conn, "token_stats",
                                                     col_name):
                        try:
                            await conn.execute(
                                f"ALTER TABLE token_stats ADD COLUMN {col_name} {col_type}"
                            )
                            print(
                                f"  Added column '{col_name}' to token_stats table"
                            )
                        except Exception as e:
                            print(f"  Failed to add column '{col_name}': {e}")

            if await self._table_exists(conn, "admin_config"):
                columns_to_add = [
                    ("admin_username", "TEXT DEFAULT 'admin'"),
                    ("admin_password", "TEXT DEFAULT 'admin'"),
                    ("api_key", "TEXT DEFAULT 'han1234'"),
                    ("task_retry_enabled", "BOOLEAN DEFAULT TRUE"),
                    ("task_max_retries", "INTEGER DEFAULT 3"),
                    ("auto_disable_on_401", "BOOLEAN DEFAULT TRUE"),
                ]

                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(conn, "admin_config",
                                                     col_name):
                        try:
                            await conn.execute(
                                f"ALTER TABLE admin_config ADD COLUMN {col_name} {col_type}"
                            )
                            print(
                                f"  Added column '{col_name}' to admin_config table"
                            )
                        except Exception as e:
                            print(f"  Failed to add column '{col_name}': {e}")

            if await self._table_exists(conn, "watermark_free_config"):
                columns_to_add = [
                    ("parse_method", "TEXT DEFAULT 'third_party'"),
                    ("custom_parse_url", "TEXT"),
                    ("custom_parse_token", "TEXT"),
                    ("fallback_on_failure", "BOOLEAN DEFAULT TRUE"),
                ]

                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(
                            conn, "watermark_free_config", col_name):
                        try:
                            await conn.execute(
                                f"ALTER TABLE watermark_free_config ADD COLUMN {col_name} {col_type}"
                            )
                            print(
                                f"  Added column '{col_name}' to watermark_free_config table"
                            )
                        except Exception as e:
                            print(f"  Failed to add column '{col_name}': {e}")

            if await self._table_exists(conn, "request_logs"):
                columns_to_add = [
                    ("task_id", "TEXT"),
                    ("updated_at", "TIMESTAMP"),
                ]

                for col_name, col_type in columns_to_add:
                    if not await self._column_exists(conn, "request_logs",
                                                     col_name):
                        try:
                            await conn.execute(
                                f"ALTER TABLE request_logs ADD COLUMN {col_name} {col_type}"
                            )
                            print(
                                f"  Added column '{col_name}' to request_logs table"
                            )
                        except Exception as e:
                            print(f"  Failed to add column '{col_name}': {e}")

            if await self._table_exists(conn, "tasks"):
                # Migration: Add retry_count column to tasks table if it doesn't exist
                if not await self._column_exists(conn, "tasks", "retry_count"):
                    try:
                        await conn.execute("ALTER TABLE tasks ADD COLUMN retry_count INTEGER DEFAULT 0")
                        print("  Added column 'retry_count' to tasks table")
                    except Exception as e:
                        print(f"  Failed to add column 'retry_count': {e}")

            await self._ensure_config_rows(conn, config_dict)
            print("Database migration check completed.")

    async def init_db(self):
        """Initialize database tables"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tokens (
                    id SERIAL PRIMARY KEY,
                    token TEXT UNIQUE NOT NULL,
                    email TEXT NOT NULL,
                    username TEXT NOT NULL,
                    name TEXT NOT NULL,
                    st TEXT,
                    rt TEXT,
                    client_id TEXT,
                    proxy_url TEXT,
                    remark TEXT,
                    expiry_time TIMESTAMP,
                    is_active BOOLEAN DEFAULT TRUE,
                    cooled_until TIMESTAMP,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    last_used_at TIMESTAMP,
                    use_count INTEGER DEFAULT 0,
                    plan_type TEXT,
                    plan_title TEXT,
                    subscription_end TIMESTAMP,
                    sora2_supported BOOLEAN,
                    sora2_invite_code TEXT,
                    sora2_redeemed_count INTEGER DEFAULT 0,
                    sora2_total_count INTEGER DEFAULT 0,
                    sora2_remaining_count INTEGER DEFAULT 0,
                    sora2_cooldown_until TIMESTAMP,
                    image_enabled BOOLEAN DEFAULT TRUE,
                    video_enabled BOOLEAN DEFAULT TRUE,
                    image_concurrency INTEGER DEFAULT -1,
                    video_concurrency INTEGER DEFAULT -1,
                    is_expired BOOLEAN DEFAULT FALSE
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS token_stats (
                    id SERIAL PRIMARY KEY,
                    token_id INTEGER NOT NULL,
                    image_count INTEGER DEFAULT 0,
                    video_count INTEGER DEFAULT 0,
                    error_count INTEGER DEFAULT 0,
                    last_error_at TIMESTAMP,
                    today_image_count INTEGER DEFAULT 0,
                    today_video_count INTEGER DEFAULT 0,
                    today_error_count INTEGER DEFAULT 0,
                    today_date DATE,
                    consecutive_error_count INTEGER DEFAULT 0,
                    FOREIGN KEY (token_id) REFERENCES tokens(id)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS tasks (
                    id SERIAL PRIMARY KEY,
                    task_id TEXT UNIQUE NOT NULL,
                    token_id INTEGER NOT NULL,
                    model TEXT NOT NULL,
                    prompt TEXT NOT NULL,
                    status TEXT NOT NULL DEFAULT 'processing',
                    progress FLOAT DEFAULT 0,
                    result_urls TEXT,
                    error_message TEXT,
                    retry_count INTEGER DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    FOREIGN KEY (token_id) REFERENCES tokens(id)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS request_logs (
                    id SERIAL PRIMARY KEY,
                    token_id INTEGER,
                    task_id TEXT,
                    operation TEXT NOT NULL,
                    request_body TEXT,
                    response_body TEXT,
                    status_code INTEGER NOT NULL,
                    duration FLOAT NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP,
                    FOREIGN KEY (token_id) REFERENCES tokens(id)
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS admin_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    admin_username TEXT DEFAULT 'admin',
                    admin_password TEXT DEFAULT 'admin',
                    api_key TEXT DEFAULT 'han1234',
                    error_ban_threshold INTEGER DEFAULT 3,
                    task_retry_enabled BOOLEAN DEFAULT TRUE,
                    task_max_retries INTEGER DEFAULT 3,
                    auto_disable_on_401 BOOLEAN DEFAULT TRUE,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS proxy_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    proxy_enabled BOOLEAN DEFAULT FALSE,
                    proxy_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS watermark_free_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    watermark_free_enabled BOOLEAN DEFAULT FALSE,
                    parse_method TEXT DEFAULT 'third_party',
                    custom_parse_url TEXT,
                    custom_parse_token TEXT,
                    fallback_on_failure BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    cache_enabled BOOLEAN DEFAULT FALSE,
                    cache_timeout INTEGER DEFAULT 600,
                    cache_base_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS generation_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    image_timeout INTEGER DEFAULT 300,
                    video_timeout INTEGER DEFAULT 3000,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS token_refresh_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    at_auto_refresh_enabled BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Call logic config table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS call_logic_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    call_mode TEXT DEFAULT 'default',
                    polling_mode_enabled BOOLEAN DEFAULT FALSE,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # POW proxy config table
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS pow_proxy_config (
                    id INTEGER PRIMARY KEY DEFAULT 1,
                    pow_proxy_enabled BOOLEAN DEFAULT FALSE,
                    pow_proxy_url TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Create indexes
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_id ON tasks(task_id)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_status ON tasks(status)")
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_token_active ON tokens(is_active)"
            )

    async def init_config_from_toml(self,
                                    config_dict: dict,
                                    is_first_startup: bool = True):
        """
        Initialize database configuration from setting.toml

        Args:
            config_dict: Configuration dictionary from setting.toml
            is_first_startup: If True, initialize all config rows from setting.toml.
                            If False (upgrade mode), only ensure missing config rows exist with default values.
        """
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            if is_first_startup:
                await self._ensure_config_rows(conn, config_dict)
            else:
                await self._ensure_config_rows(conn, config_dict=None)

    def _row_to_dict(self, row: asyncpg.Record) -> dict:
        """Convert asyncpg Record to dictionary and format types for Pydantic"""
        d = dict(row)
        # Convert date to string for Pydantic model compatibility
        if 'today_date' in d and d['today_date']:
            from datetime import date
            if isinstance(d['today_date'], date):
                d['today_date'] = d['today_date'].isoformat()
        return d

    async def add_token(self, token: Token) -> int:
        """Add a new token"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            token_id = await conn.fetchval(
                """
                INSERT INTO tokens (token, email, username, name, st, rt, client_id, proxy_url, remark, expiry_time, is_active,
                                   plan_type, plan_title, subscription_end, sora2_supported, sora2_invite_code,
                                   sora2_redeemed_count, sora2_total_count, sora2_remaining_count, sora2_cooldown_until,
                                   image_enabled, video_enabled, image_concurrency, video_concurrency)
                VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19, $20, $21, $22, $23, $24)
                RETURNING id
            """, token.token, token.email, "", token.name, token.st, token.rt,
                token.client_id, token.proxy_url, token.remark,
                token.expiry_time, token.is_active, token.plan_type,
                token.plan_title, token.subscription_end,
                token.sora2_supported, token.sora2_invite_code,
                token.sora2_redeemed_count, token.sora2_total_count,
                token.sora2_remaining_count, token.sora2_cooldown_until,
                token.image_enabled, token.video_enabled,
                token.image_concurrency, token.video_concurrency)

            await conn.execute(
                """
                INSERT INTO token_stats (token_id) VALUES ($1)
            """, token_id)

            return token_id

    async def get_token(self, token_id: int) -> Optional[Token]:
        """Get token by ID"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tokens WHERE id = $1",
                                      token_id)
            if row:
                return Token(**self._row_to_dict(row))
            return None

    async def get_token_by_value(self, token: str) -> Optional[Token]:
        """Get token by value"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tokens WHERE token = $1",
                                      token)
            if row:
                return Token(**self._row_to_dict(row))
            return None

    async def get_token_by_email(self, email: str) -> Optional[Token]:
        """Get token by email"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tokens WHERE email = $1",
                                      email)
            if row:
                return Token(**self._row_to_dict(row))
            return None

    async def get_active_tokens(self) -> List[Token]:
        """Get all active tokens (enabled, not cooled down, not expired)"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT * FROM tokens
                WHERE is_active = TRUE
                AND (cooled_until IS NULL OR cooled_until < CURRENT_TIMESTAMP)
                AND expiry_time > CURRENT_TIMESTAMP
                ORDER BY last_used_at ASC NULLS FIRST
            """)
            return [Token(**self._row_to_dict(row)) for row in rows]

    async def get_all_tokens(self) -> List[Token]:
        """Get all tokens"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                "SELECT * FROM tokens ORDER BY created_at DESC")
            return [Token(**self._row_to_dict(row)) for row in rows]

    async def update_token_usage(self, token_id: int):
        """Update token usage"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens 
                SET last_used_at = CURRENT_TIMESTAMP, use_count = use_count + 1
                WHERE id = $1
            """, token_id)

    async def update_token_status(self, token_id: int, is_active: bool):
        """Update token status"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET is_active = $1 WHERE id = $2
            """, is_active, token_id)

    async def mark_token_expired(self, token_id: int):
        """Mark token as expired and disable it"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET is_expired = TRUE, is_active = FALSE WHERE id = $1
            """, token_id)

    async def clear_token_expired(self, token_id: int):
        """Clear token expired flag"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET is_expired = FALSE WHERE id = $1
            """, token_id)

    async def update_token_sora2(self,
                                 token_id: int,
                                 supported: bool,
                                 invite_code: Optional[str] = None,
                                 redeemed_count: int = 0,
                                 total_count: int = 0,
                                 remaining_count: int = 0):
        """Update token Sora2 support info"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens
                SET sora2_supported = $1, sora2_invite_code = $2, sora2_redeemed_count = $3, sora2_total_count = $4, sora2_remaining_count = $5
                WHERE id = $6
            """, supported, invite_code, redeemed_count, total_count,
                remaining_count, token_id)

    async def update_token_sora2_remaining(self, token_id: int,
                                           remaining_count: int):
        """Update token Sora2 remaining count"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET sora2_remaining_count = $1 WHERE id = $2
            """, remaining_count, token_id)

    async def update_token_sora2_cooldown(self, token_id: int,
                                          cooldown_until: Optional[datetime]):
        """Update token Sora2 cooldown time"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET sora2_cooldown_until = $1 WHERE id = $2
            """, cooldown_until, token_id)

    async def update_token_cooldown(self, token_id: int,
                                    cooled_until: datetime):
        """Update token cooldown"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE tokens SET cooled_until = $1 WHERE id = $2
            """, cooled_until, token_id)

    async def delete_token(self, token_id: int):
        """Delete token"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM token_stats WHERE token_id = $1",
                               token_id)
            await conn.execute("DELETE FROM tokens WHERE id = $1", token_id)

    async def update_token(self,
                           token_id: int,
                           token: Optional[str] = None,
                           st: Optional[str] = None,
                           rt: Optional[str] = None,
                           client_id: Optional[str] = None,
                           proxy_url: Optional[str] = None,
                           remark: Optional[str] = None,
                           expiry_time: Optional[datetime] = None,
                           plan_type: Optional[str] = None,
                           plan_title: Optional[str] = None,
                           subscription_end: Optional[datetime] = None,
                           image_enabled: Optional[bool] = None,
                           video_enabled: Optional[bool] = None,
                           image_concurrency: Optional[int] = None,
                           video_concurrency: Optional[int] = None):
        """Update token"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            updates = []
            params = []
            param_idx = 1

            if token is not None:
                updates.append(f"token = ${param_idx}")
                params.append(token)
                param_idx += 1

            if st is not None:
                updates.append(f"st = ${param_idx}")
                params.append(st)
                param_idx += 1

            if rt is not None:
                updates.append(f"rt = ${param_idx}")
                params.append(rt)
                param_idx += 1

            if client_id is not None:
                updates.append(f"client_id = ${param_idx}")
                params.append(client_id)
                param_idx += 1

            if proxy_url is not None:
                updates.append(f"proxy_url = ${param_idx}")
                params.append(proxy_url)
                param_idx += 1

            if remark is not None:
                updates.append(f"remark = ${param_idx}")
                params.append(remark)
                param_idx += 1

            if expiry_time is not None:
                updates.append(f"expiry_time = ${param_idx}")
                params.append(expiry_time)
                param_idx += 1

            if plan_type is not None:
                updates.append(f"plan_type = ${param_idx}")
                params.append(plan_type)
                param_idx += 1

            if plan_title is not None:
                updates.append(f"plan_title = ${param_idx}")
                params.append(plan_title)
                param_idx += 1

            if subscription_end is not None:
                updates.append(f"subscription_end = ${param_idx}")
                params.append(subscription_end)
                param_idx += 1

            if image_enabled is not None:
                updates.append(f"image_enabled = ${param_idx}")
                params.append(image_enabled)
                param_idx += 1

            if video_enabled is not None:
                updates.append(f"video_enabled = ${param_idx}")
                params.append(video_enabled)
                param_idx += 1

            if image_concurrency is not None:
                updates.append(f"image_concurrency = ${param_idx}")
                params.append(image_concurrency)
                param_idx += 1

            if video_concurrency is not None:
                updates.append(f"video_concurrency = ${param_idx}")
                params.append(video_concurrency)
                param_idx += 1

            if updates:
                params.append(token_id)
                query = f"UPDATE tokens SET {', '.join(updates)} WHERE id = ${param_idx}"
                await conn.execute(query, *params)

    async def get_token_stats(self, token_id: int) -> Optional[TokenStats]:
        """Get token statistics"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM token_stats WHERE token_id = $1", token_id)
            if row:
                return TokenStats(**self._row_to_dict(row))
            return None

    async def increment_image_count(self, token_id: int):
        """Increment image generation count"""
        from datetime import date
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT today_date FROM token_stats WHERE token_id = $1",
                token_id)
            if row and row['today_date'] != date.today():
                await conn.execute(
                    """
                    UPDATE token_stats
                    SET image_count = image_count + 1,
                        today_image_count = 1,
                        today_date = $1
                    WHERE token_id = $2
                """, date.today(), token_id)
            else:
                await conn.execute(
                    """
                    UPDATE token_stats
                    SET image_count = image_count + 1,
                        today_image_count = today_image_count + 1,
                        today_date = $1
                    WHERE token_id = $2
                """, date.today(), token_id)

    async def increment_video_count(self, token_id: int):
        """Increment video generation count"""
        from datetime import date
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT today_date FROM token_stats WHERE token_id = $1",
                token_id)
            if row and row['today_date'] != date.today():
                await conn.execute(
                    """
                    UPDATE token_stats
                    SET video_count = video_count + 1,
                        today_video_count = 1,
                        today_date = $1
                    WHERE token_id = $2
                """, date.today(), token_id)
            else:
                await conn.execute(
                    """
                    UPDATE token_stats
                    SET video_count = video_count + 1,
                        today_video_count = today_video_count + 1,
                        today_date = $1
                    WHERE token_id = $2
                """, date.today(), token_id)

    async def increment_error_count(self,
                                    token_id: int,
                                    increment_consecutive: bool = True):
        """Increment error count"""
        from datetime import date
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT today_date FROM token_stats WHERE token_id = $1",
                token_id)
            if row and row['today_date'] != date.today():
                if increment_consecutive:
                    await conn.execute(
                        """
                        UPDATE token_stats
                        SET error_count = error_count + 1,
                            consecutive_error_count = consecutive_error_count + 1,
                            today_error_count = 1,
                            today_date = $1,
                            last_error_at = CURRENT_TIMESTAMP
                        WHERE token_id = $2
                    """, date.today(), token_id)
                else:
                    await conn.execute(
                        """
                        UPDATE token_stats
                        SET error_count = error_count + 1,
                            today_error_count = 1,
                            today_date = $1,
                            last_error_at = CURRENT_TIMESTAMP
                        WHERE token_id = $2
                    """, date.today(), token_id)
            else:
                if increment_consecutive:
                    await conn.execute(
                        """
                        UPDATE token_stats
                        SET error_count = error_count + 1,
                            consecutive_error_count = consecutive_error_count + 1,
                            today_error_count = today_error_count + 1,
                            today_date = $1,
                            last_error_at = CURRENT_TIMESTAMP
                        WHERE token_id = $2
                    """, date.today(), token_id)
                else:
                    await conn.execute(
                        """
                        UPDATE token_stats
                        SET error_count = error_count + 1,
                            today_error_count = today_error_count + 1,
                            today_date = $1,
                            last_error_at = CURRENT_TIMESTAMP
                        WHERE token_id = $2
                    """, date.today(), token_id)

    async def reset_error_count(self, token_id: int):
        """Reset consecutive error count"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE token_stats SET consecutive_error_count = 0 WHERE token_id = $1
            """, token_id)

    async def create_task(self, task: Task) -> int:
        """Create a new task"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            task_db_id = await conn.fetchval(
                """
                INSERT INTO tasks (task_id, token_id, model, prompt, status, progress)
                VALUES ($1, $2, $3, $4, $5, $6)
                RETURNING id
            """, task.task_id, task.token_id, task.model, task.prompt,
                task.status, task.progress)
            return task_db_id

    async def update_task(self,
                          task_id: str,
                          status: str,
                          progress: float,
                          result_urls: Optional[str] = None,
                          error_message: Optional[str] = None):
        """Update task status"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            completed_at = datetime.now() if status in ["completed", "failed"
                                                        ] else None
            await conn.execute(
                """
                UPDATE tasks 
                SET status = $1, progress = $2, result_urls = $3, error_message = $4, completed_at = $5
                WHERE task_id = $6
            """, status, progress, result_urls, error_message, completed_at,
                task_id)

    async def get_task(self, task_id: str) -> Optional[Task]:
        """Get task by ID"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM tasks WHERE task_id = $1",
                                      task_id)
            if row:
                return Task(**self._row_to_dict(row))
            return None

    async def log_request(self, log: RequestLog) -> int:
        """Log a request and return log ID"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            log_id = await conn.fetchval(
                """
                INSERT INTO request_logs (token_id, task_id, operation, request_body, response_body, status_code, duration)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
                RETURNING id
            """, log.token_id, log.task_id, log.operation, log.request_body,
                log.response_body, log.status_code, log.duration)
            return log_id

    async def update_request_log(self,
                                 log_id: int,
                                 response_body: Optional[str] = None,
                                 status_code: Optional[int] = None,
                                 duration: Optional[float] = None):
        """Update request log with completion data"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            updates = []
            params = []
            param_idx = 1

            if response_body is not None:
                updates.append(f"response_body = ${param_idx}")
                params.append(response_body)
                param_idx += 1
            if status_code is not None:
                updates.append(f"status_code = ${param_idx}")
                params.append(status_code)
                param_idx += 1
            if duration is not None:
                updates.append(f"duration = ${param_idx}")
                params.append(duration)
                param_idx += 1

            if updates:
                updates.append("updated_at = CURRENT_TIMESTAMP")
                params.append(log_id)
                query = f"UPDATE request_logs SET {', '.join(updates)} WHERE id = ${param_idx}"
                await conn.execute(query, *params)

    async def update_request_log_task_id(self, log_id: int, task_id: str):
        """Update request log with task_id"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE request_logs
                SET task_id = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = $2
            """, task_id, log_id)

    async def get_recent_logs(self, limit: int = 100) -> List[dict]:
        """Get recent logs with token email"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    rl.id,
                    rl.token_id,
                    rl.task_id,
                    rl.operation,
                    rl.request_body,
                    rl.response_body,
                    rl.status_code,
                    rl.duration,
                    rl.created_at,
                    t.email as token_email,
                    t.username as token_username
                FROM request_logs rl
                LEFT JOIN tokens t ON rl.token_id = t.id
                ORDER BY rl.created_at DESC
                LIMIT $1
            """, limit)
            return [self._row_to_dict(row) for row in rows]

    async def clear_all_logs(self):
        """Clear all request logs"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute("DELETE FROM request_logs")

    async def get_admin_config(self) -> AdminConfig:
        """Get admin configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM admin_config WHERE id = 1")
            if row:
                return AdminConfig(**self._row_to_dict(row))
            return AdminConfig(admin_username="admin",
                               admin_password="admin",
                               api_key="han1234")

    async def update_admin_config(self, config: AdminConfig):
        """Update admin configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE admin_config
                SET admin_username = $1, admin_password = $2, api_key = $3, error_ban_threshold = $4,
                    task_retry_enabled = $5, task_max_retries = $6, auto_disable_on_401 = $7,
                    updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, config.admin_username, config.admin_password, config.api_key, config.error_ban_threshold,
                config.task_retry_enabled, config.task_max_retries, config.auto_disable_on_401)

    # Proxy config operations
    async def get_proxy_config(self) -> ProxyConfig:
        """Get proxy configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM proxy_config WHERE id = 1")
            if row:
                return ProxyConfig(**self._row_to_dict(row))
            return ProxyConfig(proxy_enabled=False)

    async def update_proxy_config(self, enabled: bool,
                                  proxy_url: Optional[str]):
        """Update proxy configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE proxy_config
                SET proxy_enabled = $1, proxy_url = $2, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, enabled, proxy_url)

    async def get_watermark_free_config(self) -> WatermarkFreeConfig:
        """Get watermark-free configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM watermark_free_config WHERE id = 1")
            if row:
                return WatermarkFreeConfig(**self._row_to_dict(row))
            return WatermarkFreeConfig(watermark_free_enabled=False,
                                       parse_method="third_party")

    async def update_watermark_free_config(self, enabled: bool, parse_method: str = None,
                                          custom_parse_url: str = None, custom_parse_token: str = None,
                                          fallback_on_failure: bool = None):
        """Update watermark-free configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            if parse_method is None and custom_parse_url is None and custom_parse_token is None and fallback_on_failure is None:
                # Only update enabled status
                await conn.execute("""
                    UPDATE watermark_free_config
                    SET watermark_free_enabled = $1, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, enabled)
            else:
                await conn.execute(
                    """
                    UPDATE watermark_free_config
                    SET watermark_free_enabled = $1, parse_method = $2, custom_parse_url = $3,
                        custom_parse_token = $4, fallback_on_failure = $5, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, enabled, parse_method or "third_party", custom_parse_url, custom_parse_token,
                    fallback_on_failure if fallback_on_failure is not None else True)

    async def get_cache_config(self) -> CacheConfig:
        """Get cache configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM cache_config WHERE id = 1")
            if row:
                return CacheConfig(**self._row_to_dict(row))
            return CacheConfig(cache_enabled=False, cache_timeout=600)

    async def update_cache_config(self,
                                  enabled: bool = None,
                                  timeout: int = None,
                                  base_url: Optional[str] = None):
        """Update cache configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM cache_config WHERE id = 1")

            if row:
                current = self._row_to_dict(row)
                new_enabled = enabled if enabled is not None else current.get(
                    "cache_enabled", False)
                new_timeout = timeout if timeout is not None else current.get(
                    "cache_timeout", 600)
                new_base_url = base_url if base_url is not None else current.get(
                    "cache_base_url")
            else:
                new_enabled = enabled if enabled is not None else False
                new_timeout = timeout if timeout is not None else 600
                new_base_url = base_url

            new_base_url = new_base_url if new_base_url else None

            await conn.execute(
                """
                UPDATE cache_config
                SET cache_enabled = $1, cache_timeout = $2, cache_base_url = $3, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, new_enabled, new_timeout, new_base_url)

    async def get_generation_config(self) -> GenerationConfig:
        """Get generation configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM generation_config WHERE id = 1")
            if row:
                return GenerationConfig(**self._row_to_dict(row))
            return GenerationConfig(image_timeout=300, video_timeout=3000)

    async def update_generation_config(self,
                                       image_timeout: int = None,
                                       video_timeout: int = None):
        """Update generation configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM generation_config WHERE id = 1")

            if row:
                current = self._row_to_dict(row)
                new_image_timeout = image_timeout if image_timeout is not None else current.get(
                    "image_timeout", 300)
                new_video_timeout = video_timeout if video_timeout is not None else current.get(
                    "video_timeout", 3000)
            else:
                new_image_timeout = image_timeout if image_timeout is not None else 300
                new_video_timeout = video_timeout if video_timeout is not None else 3000

            await conn.execute(
                """
                UPDATE generation_config
                SET image_timeout = $1, video_timeout = $2, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, new_image_timeout, new_video_timeout)

    async def get_token_refresh_config(self) -> TokenRefreshConfig:
        """Get token refresh configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM token_refresh_config WHERE id = 1")
            if row:
                return TokenRefreshConfig(**self._row_to_dict(row))
            return TokenRefreshConfig(at_auto_refresh_enabled=False)

    async def update_token_refresh_config(self, at_auto_refresh_enabled: bool):
        """Update token refresh configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE token_refresh_config
                SET at_auto_refresh_enabled = $1, updated_at = CURRENT_TIMESTAMP
                WHERE id = 1
            """, at_auto_refresh_enabled)

    # Call logic config operations
    async def get_call_logic_config(self) -> "CallLogicConfig":
        """Get call logic configuration"""
        from .models import CallLogicConfig
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM call_logic_config WHERE id = 1")
            if row:
                row_dict = self._row_to_dict(row)
                if not row_dict.get("call_mode"):
                    row_dict["call_mode"] = "polling" if row_dict.get("polling_mode_enabled") else "default"
                return CallLogicConfig(**row_dict)
            return CallLogicConfig(call_mode="default", polling_mode_enabled=False)

    async def update_call_logic_config(self, call_mode: str):
        """Update call logic configuration"""
        normalized = "polling" if call_mode == "polling" else "default"
        polling_mode_enabled = normalized == "polling"
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Check if row exists
            count = await conn.fetchval("SELECT COUNT(*) FROM call_logic_config WHERE id = 1")
            if count == 0:
                await conn.execute("""
                    INSERT INTO call_logic_config (id, call_mode, polling_mode_enabled, updated_at)
                    VALUES (1, $1, $2, CURRENT_TIMESTAMP)
                """, normalized, polling_mode_enabled)
            else:
                await conn.execute("""
                    UPDATE call_logic_config
                    SET call_mode = $1, polling_mode_enabled = $2, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, normalized, polling_mode_enabled)

    # POW proxy config operations
    async def get_pow_proxy_config(self) -> "PowProxyConfig":
        """Get POW proxy configuration"""
        from .models import PowProxyConfig
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            row = await conn.fetchrow("SELECT * FROM pow_proxy_config WHERE id = 1")
            if row:
                return PowProxyConfig(**self._row_to_dict(row))
            return PowProxyConfig(pow_proxy_enabled=False, pow_proxy_url=None)

    async def update_pow_proxy_config(self, pow_proxy_enabled: bool, pow_proxy_url: Optional[str] = None):
        """Update POW proxy configuration"""
        pool = await self.get_pool()
        async with pool.acquire() as conn:
            # Check if row exists
            count = await conn.fetchval("SELECT COUNT(*) FROM pow_proxy_config WHERE id = 1")
            if count == 0:
                await conn.execute("""
                    INSERT INTO pow_proxy_config (id, pow_proxy_enabled, pow_proxy_url, updated_at)
                    VALUES (1, $1, $2, CURRENT_TIMESTAMP)
                """, pow_proxy_enabled, pow_proxy_url)
            else:
                await conn.execute("""
                    UPDATE pow_proxy_config
                    SET pow_proxy_enabled = $1, pow_proxy_url = $2, updated_at = CURRENT_TIMESTAMP
                    WHERE id = 1
                """, pow_proxy_enabled, pow_proxy_url)
