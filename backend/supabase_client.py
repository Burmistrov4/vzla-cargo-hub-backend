import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_PUBLISHABLE_KEY = os.getenv("SUPABASE_PUBLISHABLE_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL:
    raise RuntimeError("Falta SUPABASE_URL en el archivo .env")

if not SUPABASE_PUBLISHABLE_KEY:
    raise RuntimeError("Falta SUPABASE_PUBLISHABLE_KEY en el archivo .env")

supabase_public: Client = create_client(
    SUPABASE_URL,
    SUPABASE_PUBLISHABLE_KEY
)

supabase_admin = None
if SUPABASE_SERVICE_ROLE_KEY:
    supabase_admin = create_client(
        SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY
    )