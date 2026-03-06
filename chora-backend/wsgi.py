"""
WSGI entry point for Gunicorn
"""
from app import app, init_db_pool

# Initialize database pool when gunicorn starts
init_db_pool()

# Export app for gunicorn
application = app

if __name__ == "__main__":
    app.run()

