# Sora2API

## Overview
Sora2API is an OpenAI-compatible API service for Sora video and image generation. It provides a unified interface for text-to-image, image-to-image, text-to-video, and image-to-video generation.

## Project Architecture

### Tech Stack
- **Backend**: Python 3.11 with FastAPI
- **Database**: PostgreSQL (asyncpg for async operations) - Replit managed database
- **Frontend**: HTML with Tailwind CSS (via CDN)

### Directory Structure
```
├── config/
│   └── setting.toml          # Application configuration
├── src/
│   ├── api/                   # API route handlers
│   │   ├── admin.py           # Admin routes
│   │   └── routes.py          # Main API routes
│   ├── core/                  # Core modules
│   │   ├── auth.py            # Authentication
│   │   ├── config.py          # Configuration management
│   │   ├── database.py        # Database operations
│   │   └── models.py          # Data models
│   ├── services/              # Business logic
│   │   ├── generation_handler.py  # Generation handling
│   │   ├── sora_client.py     # Sora API client
│   │   ├── token_manager.py   # Token management
│   │   └── ...
│   └── main.py                # FastAPI application
├── static/                    # Static assets
│   ├── js/
│   ├── login.html
│   ├── manage.html
│   └── generate.html
├── main.py                    # Application entry point
└── requirements.txt           # Python dependencies
```

### Key Features
- Text-to-image and image-to-image generation
- Text-to-video and image-to-video generation
- Token management with load balancing
- Proxy support (HTTP/SOCKS5)
- PostgreSQL database for persistent data storage
- Admin web interface

### Running Locally
The application runs on port 5000 with:
```
python main.py
```

### Default Credentials
- Username: admin
- Password: admin
- Default API Key: han1234

## Recent Changes
- 2026-01-12: Migrated from SQLite to PostgreSQL
  - Replaced aiosqlite with asyncpg for persistent database storage
  - Data now persists across deployments and restarts
  - Same table structure: tokens, token_stats, tasks, request_logs, admin_config, proxy_config, watermark_free_config, cache_config, generation_config, token_refresh_config
- 2026-01-10: Initial Replit environment setup
  - Configured to run on port 5000
  - Set up Python 3.11 with required dependencies
  - Created workflow for starting the application
