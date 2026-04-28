import os
from dotenv import load_dotenv
from supabase import create_client, Client

# Intentar cargar .env (solo para local)
load_dotenv()

# Obtener variables
url = os.getenv("SUPABASE_URL")
key = os.getenv("SUPABASE_PUBLISHABLE_KEY")
service_key = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Inicializar como None por defecto
supabase_public = None
supabase_admin = None

# Verificación Robusta
if url and key:
    try:
        supabase_public = create_client(url, key)
        print("✅ Cliente Público de Supabase conectado.")
    except Exception as e:
        print(f"❌ Error al inicializar cliente público: {e}")
else:
    print("❌ ERROR CRÍTICO: SUPABASE_URL o KEY no detectadas en el entorno.")

if url and service_key:
    try:
        supabase_admin = create_client(url, service_key)
        print("✅ Cliente Admin de Supabase conectado.")
    except Exception as e:
        print(f"❌ Error al inicializar cliente admin: {e}")

# Esto evita que main.py falle al importar
db_client = supabase_admin or supabase_public