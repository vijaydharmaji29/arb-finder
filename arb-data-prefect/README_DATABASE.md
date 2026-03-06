# Database Configuration

This project uses PostgreSQL (Neon) for storing market event and market data.

## Setting the Database URI

The database connection string is read from the `DATABASE_URL` environment variable. You can set it in several ways:

### Option 1: Using a `.env` file (Recommended)

1. **Location**: Create the `.env` file in the **`src/` directory** (same level as `main.py`):
   ```
   ArbDataPrefect/
   ├── .env.example
   ├── src/
   │   ├── .env          ← Create it here
   │   ├── main.py
   │   └── modules/
   ├── venv/
   └── ...
   ```

2. Copy the example file to the `src/` directory:
   ```bash
   cp .env.example src/.env
   ```

3. Edit `src/.env` and add your Neon database connection string:
   ```bash
   DATABASE_URL=postgresql+asyncpg://user:password@ep-xxx-xxx.region.aws.neon.tech/database
   ```

4. Install python-dotenv (if not already installed):
   ```bash
   pip install python-dotenv
   ```

   The code will automatically load the `.env` file from the `src/` directory when the database module is imported.

### Option 2: Set as Environment Variable

#### On macOS/Linux:
```bash
export DATABASE_URL="postgresql+asyncpg://user:password@ep-xxx-xxx.region.aws.neon.tech/database"
```

#### On Windows (PowerShell):
```powershell
$env:DATABASE_URL="postgresql+asyncpg://user:password@ep-xxx-xxx.region.aws.neon.tech/database"
```

#### On Windows (Command Prompt):
```cmd
set DATABASE_URL=postgresql+asyncpg://user:password@ep-xxx-xxx.region.aws.neon.tech/database
```

### Option 3: Prefect Configuration

You can also set it in Prefect's configuration. Prefect will automatically read environment variables, so setting `DATABASE_URL` as an environment variable will work with Prefect flows.

## Neon Database Connection String Format

For Neon databases, your connection string will look like:
```
postgresql+asyncpg://username:password@ep-xxx-xxx.region.aws.neon.tech/database_name
```

Or if you have a standard PostgreSQL connection string, it will be automatically converted:
```
postgresql://username:password@host:port/database_name
```

## Getting Your Neon Connection String

1. Log in to your Neon dashboard
2. Select your project
3. Go to the "Connection Details" section
4. Copy the connection string
5. Make sure it uses the format: `postgresql://...` or `postgresql+asyncpg://...`

## Initializing the Database

Before running your flows, you need to initialize the database tables:

```python
from src.modules.database import init_db
import asyncio

asyncio.run(init_db())
```

Or you can add this to your flow initialization.

## Security Note

⚠️ **Important**: Never commit your `.env` file to version control! It contains sensitive credentials.

Make sure `.env` is in your `.gitignore` file:
```
.env
```

