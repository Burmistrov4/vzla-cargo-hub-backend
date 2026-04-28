import os
from dotenv import load_dotenv
from supabase import create_client, Client

load_dotenv()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_PUBLISHABLE_KEY = os.getenv("SUPABASE_PUBLISHABLE_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# En lugar de detener el programa con RuntimeError, imprimimos un aviso.
# Railway ya tiene estas variables en su panel, así que las encontrará.
if not SUPABASE_URL or not SUPABASE_PUBLISHABLE_KEY:
    print("⚠️ Aviso: Las credenciales de Supabase no se detectaron en el entorno.")

# Solo creamos el cliente si las variables NO están vacías
supabase_public = None
if SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY:
    supabase_public = create_client(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY)

supabase_admin = None
if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
    supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)

# Cliente para operaciones normales (el que usas en el 90% de la app)
# Como RLS está desactivado, este cliente puede leer y escribir sin problemas.
supabase_public: Client = create_client(
    SUPABASE_URL,
    SUPABASE_PUBLISHABLE_KEY
)
# Cliente para operaciones administrativas (solo si es estrictamente necesario)
supabase_admin = None
if SUPABASE_SERVICE_ROLE_KEY:
    supabase_admin = create_client(
        SUPABASE_URL,
        SUPABASE_SERVICE_ROLE_KEY
    )