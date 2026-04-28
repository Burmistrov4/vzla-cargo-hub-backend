import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Carga variables desde .env si el archivo existe (útil para desarrollo local)
load_dotenv()

# Obtener variables de entorno
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_PUBLISHABLE_KEY = os.getenv("SUPABASE_PUBLISHABLE_KEY")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Validación de seguridad: Verifica que las variables esenciales existan
if not SUPABASE_URL or not SUPABASE_PUBLISHABLE_KEY:
    # En Railway, esto se verá en los logs si olvidaste configurar una variable
    print("❌ ERROR: No se encontraron las variables de entorno de Supabase.")
    print("Asegúrate de que SUPABASE_URL y SUPABASE_PUBLISHABLE_KEY estén configuradas.")
else:
    print("✅ Conexión con Supabase configurada correctamente.")

# Inicialización del cliente público (para operaciones estándar)
supabase_public: Client = None
if SUPABASE_URL and SUPABASE_PUBLISHABLE_KEY:
    supabase_public = create_client(SUPABASE_URL, SUPABASE_PUBLISHABLE_KEY)

# Inicialización del cliente admin (solo si la Service Role Key está presente)
supabase_admin: Client = None
if SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY:
    supabase_admin = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)