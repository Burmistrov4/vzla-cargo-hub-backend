from datetime import datetime, date, timezone, timedelta
import logging
import unicodedata
from uuid import uuid4
from typing import Literal

from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

from backend.supabase_client import supabase_admin, supabase_public
from backend.calculator import (
    calculate_quote,
    calculate_owc_quote,
    calculate_zoom_quote,
)
from backend.bcv_scraper import fetch_bcv_usd_rate
from backend.owc_scraper import scrape_owc_public_rates, refresh_owc_business_rules
from backend.models import (
    QuoteCalculateRequest,
    QuoteCalculateResponse,
    QuoteSaveRequest,
    QuoteSaveResponse,
    RestrictedItemMatch,
)

logger = logging.getLogger(__name__)

app = FastAPI(title="Vzla Cargo Hub API")
app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:3000",
        "http://127.0.0.1:3000",
        "https://vzla-cargo-hub-frontend.vercel.app",
        "https://vzla-cargo-hub-backend-production.up.railway.app",
        "https://vzla-cargo-hub-api.onrender.com",
    ],
    allow_origin_regex=r"https://.*\.vercel\.app",
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

db_client = supabase_admin or supabase_public


BCV_REFRESH_MAX_AGE_HOURS = 6
OWC_REFRESH_MAX_AGE_HOURS = 6
OWC_TARIFF_SOURCE_URL = "https://onewaycargo.net/tarifas#calculadora"
OWC_RESTRICTED_ITEMS_SOURCE_URL = "https://onewaycargo.net/articulos-prohibidos"

OWC_REQUIRED_RULES = [
    ("air_base_rate_ves", "air", None),
    ("sea_base_rate_ves", "sea", None),
    ("correspondence_rate_ves", "correspondence", None),
    ("handling_fee_ves", "*", "*"),
]


def utc_now() -> datetime:
    return datetime.now(timezone.utc)


def parse_iso_datetime(value) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)

    try:
        text = str(value).strip().replace("Z", "+00:00")
        parsed = datetime.fromisoformat(text)
        return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)
    except Exception:
        return None


def parse_iso_date(value) -> date | None:
    if not value:
        return None

    if isinstance(value, date) and not isinstance(value, datetime):
        return value

    try:
        return date.fromisoformat(str(value)[:10])
    except Exception:
        return None


def is_timestamp_older_than(value, hours: int) -> bool:
    parsed = parse_iso_datetime(value)
    if not parsed:
        return True

    return utc_now() - parsed > timedelta(hours=hours)


def get_exchange_fetched_at(row: dict) -> str | None:
    return row.get("fetched_at") or row.get("updated_at") or row.get("created_at")


def should_refresh_exchange_rate(row: dict | None) -> tuple[bool, list[str]]:
    reasons: list[str] = []

    if not row:
        return True, ["No hay tasa BCV guardada"]

    rate_date = parse_iso_date(row.get("rate_date"))
    fetched_at = get_exchange_fetched_at(row)

    if not row.get("rate"):
        reasons.append("La tasa guardada no tiene valor")

    if not rate_date:
        reasons.append("La tasa guardada no tiene rate_date")

    if not fetched_at:
        reasons.append("La tasa guardada no tiene fetched_at/updated_at")

    if fetched_at and is_timestamp_older_than(fetched_at, BCV_REFRESH_MAX_AGE_HOURS):
        reasons.append(f"La tasa fue consultada hace más de {BCV_REFRESH_MAX_AGE_HOURS} horas")

    # El BCV no siempre publica todos los días a la misma hora. Por eso,
    # si la fecha es anterior a hoy, intentamos refrescar solo si la consulta
    # guardada no fue reciente.
    if rate_date and rate_date < date.today() and (
        not fetched_at or is_timestamp_older_than(fetched_at, 1)
    ):
        reasons.append("La fecha de la tasa BCV es anterior a hoy")

    return len(reasons) > 0, reasons


def save_bcv_exchange_rate(scraped: dict) -> dict:
    if db_client is None:
        raise RuntimeError("Cliente Supabase no disponible para guardar tasa BCV")

    payload = {
        "source": scraped["source"],
        "currency_from": scraped["currency_from"],
        "currency_to": scraped["currency_to"],
        "rate": scraped["rate"],
        "rate_date": scraped["rate_date"],
        "fetched_at": utc_now().isoformat(),
    }

    result = (
        db_client
        .table("exchange_rates")
        .upsert(
            payload,
            on_conflict="source,currency_from,currency_to,rate_date",
        )
        .execute()
    )

    return result.data[0] if result.data else payload


def get_latest_exchange_rate_row() -> dict | None:
    result = (
        supabase_public
        .table("exchange_rates")
        .select("*")
        .order("rate_date", desc=True)
        .order("fetched_at", desc=True)
        .limit(1)
        .execute()
    )

    return result.data[0] if result.data else None


def get_owc_rule_timestamp(row: dict | None) -> str | None:
    if not row:
        return None
    return row.get("updated_at") or row.get("created_at")


def analyze_owc_rules_freshness(
    rules_rows: list[dict],
    region: str,
) -> dict:
    reasons: list[str] = []
    timestamps: list[datetime] = []

    for rule_code, service_type_key, explicit_region_key in OWC_REQUIRED_RULES:
        region_key = explicit_region_key if explicit_region_key is not None else region
        row = _find_rule_row(
            rules_rows,
            rule_code,
            service_type_key,
            region_key,
        )

        if not row:
            reasons.append(f"Falta regla OWC: {rule_code}")
            continue

        numeric_value = row.get("numeric_value")

        if numeric_value is None or float(numeric_value) <= 0:
            reasons.append(f"Regla OWC inválida o en cero: {rule_code}")

        parsed_ts = parse_iso_datetime(get_owc_rule_timestamp(row))
        if parsed_ts:
            timestamps.append(parsed_ts)
        else:
            reasons.append(f"Regla OWC sin timestamp: {rule_code}")

    oldest_updated_at = min(timestamps) if timestamps else None

    if oldest_updated_at and utc_now() - oldest_updated_at > timedelta(hours=OWC_REFRESH_MAX_AGE_HOURS):
        reasons.append(f"Tarifario OWC consultado hace más de {OWC_REFRESH_MAX_AGE_HOURS} horas")

    return {
        "stale": len(reasons) > 0,
        "reasons": reasons,
        "oldest_updated_at": oldest_updated_at.isoformat() if oldest_updated_at else None,
    }


def owc_refresh_updated_any_rules(refresh_result: dict | None) -> bool:
    saved = (refresh_result or {}).get("saved") or {}
    if not isinstance(saved, dict):
        return False

    for value in saved.values():
        if isinstance(value, dict) and int(value.get("updated_count") or 0) > 0:
            return True

    return False


def normalize_search_text(value: str | None) -> str:
    text = (value or "").strip().lower()
    text = unicodedata.normalize("NFD", text)
    text = "".join(ch for ch in text if unicodedata.category(ch) != "Mn")
    return " ".join(text.split())


def restricted_item_to_response(row: dict, matched_input: str) -> dict:
    return {
        "item_name": row.get("item_name") or "",
        "restriction_level": row.get("restriction_level") or "restricted",
        "matched_input": matched_input,
        "reason": row.get("reason"),
        "notes": row.get("notes"),
        "source_url": OWC_RESTRICTED_ITEMS_SOURCE_URL,
        "courier_id": row.get("courier_id"),
    }


# -----------------------------------------------------------------------------
# OWC intelligent restricted-item search
# -----------------------------------------------------------------------------


OWC_SMART_RESTRICTION_CATEGORIES = [
    {
        "id": "cellphones",
        "label": "Celulares y teléfonos",
        "level_hint": "restricted",
        "terms": [
            "celular", "celulares", "telefono", "teléfono", "telefonos", "teléfonos",
            "smartphone", "iphone", "android", "movil", "móvil", "moviles", "móviles",
            "samsung", "xiaomi", "motorola", "huawei", "pixel", "oneplus",
        ],
        "db_terms": ["equipos celulares", "celular", "celulares"],
        "examples": ["celular", "iPhone", "teléfono Android", "smartphone"],
        "user_message": (
            "Los celulares nuevos o usados están bajo régimen especial. "
            "No significa que estén prohibidos automáticamente, pero OWC puede pedir validación previa."
        ),
        "recommendation": (
            "Consulta con OWC antes de enviarlo. Ten factura o comprobante disponible y evita cantidades comerciales."
        ),
    },
    {
        "id": "electronics_laptops",
        "label": "Electrónicos, laptops y computadoras",
        "level_hint": "restricted",
        "terms": [
            "electronico", "electrónico", "electronicos", "electrónicos",
            "laptop", "laptops", "computadora", "computadoras", "pc", "desktop",
            "tablet", "ipad", "consola", "playstation", "ps4", "ps5", "xbox", "nintendo",
            "switch", "audifonos", "audífonos", "camara", "cámara", "smartwatch",
            "reloj inteligente", "monitor", "tarjeta grafica", "tarjeta gráfica", "gpu",
        ],
        "db_terms": ["laptops", "laptop", "electrónicos", "electronicos"],
        "examples": ["laptop", "PC", "tablet", "consola", "audífonos"],
        "user_message": (
            "Los electrónicos nuevos o usados están bajo régimen especial. "
            "El riesgo aumenta si hay varias unidades iguales, alto valor o uso comercial."
        ),
        "recommendation": (
            "Ten factura o comprobante. Si es equipo usado, costoso o son varias unidades, confirma antes con OWC."
        ),
    },
    {
        "id": "medicines",
        "label": "Medicamentos, pastillas y tratamientos",
        "level_hint": "restricted",
        "terms": [
            "medicamento", "medicamentos", "medicina", "medicinas", "remedio", "remedios",
            "pastilla", "pastillas", "pildora", "píldora", "pildoras", "píldoras",
            "tratamiento", "tratamientos", "jarabe", "antibiotico", "antibiótico",
            "farmaco", "fármaco", "receta", "prescripcion", "prescripción", "capsula",
            "cápsula", "capsulas", "cápsulas",
        ],
        "db_terms": ["medicamento", "medicamentos"],
        "examples": ["pastillas", "medicinas", "remedios", "tratamiento"],
        "user_message": (
            "Los medicamentos están bajo régimen especial. Pueden requerir validación previa, "
            "receta, factura o confirmación del courier."
        ),
        "recommendation": (
            "No envíes medicamentos sin confirmar antes con OWC. Indica uso, cantidad, presentación y receta si aplica."
        ),
    },
    {
        "id": "supplements",
        "label": "Suplementos deportivos y vitaminas",
        "level_hint": "restricted",
        "terms": [
            "suplemento", "suplementos", "sumplemento", "sumplementos", "proteina", "proteína",
            "creatina", "vitamina", "vitaminas", "preworkout", "pre workout", "aminoacido",
            "aminoácido", "bcaa", "colageno", "colágeno", "whey", "mass gainer",
            "quemador", "fat burner",
        ],
        "db_terms": ["suplementos deportivos", "suplementos", "suplemento"],
        "examples": ["proteína", "creatina", "vitaminas", "preworkout"],
        "user_message": (
            "Los suplementos deportivos están bajo régimen especial y pueden requerir validación previa."
        ),
        "recommendation": (
            "Consulta antes con OWC, especialmente si son polvos, cápsulas, varias unidades o productos ingeribles."
        ),
    },
    {
        "id": "clothing_footwear",
        "label": "Ropa, calzado y textiles",
        "level_hint": "restricted",
        "terms": [
            "ropa", "textil", "textiles", "jean", "jeans", "pantalon", "pantalón",
            "pantalones", "camisa", "camisas", "franela", "franelas", "short", "shorts",
            "vestido", "vestidos", "chaqueta", "chaquetas", "zapato", "zapatos",
            "tenis", "sneakers", "calzado", "lenceria", "lencería", "linceria",
            "ropa interior", "interior", "medias", "gorra", "gorras", "sueter", "suéter",
        ],
        "db_terms": ["ropa con fines comerciales", "ropa", "calzado"],
        "examples": ["jeans", "camisas", "zapatos", "franelas", "lencería"],
        "user_message": (
            "La ropa o calzado para uso personal normalmente no implica alerta por sí sola. "
            "La restricción aplica cuando parece carga comercial: muchas piezas iguales, tallas repetidas, alto volumen o reventa."
        ),
        "recommendation": (
            "Si es poca cantidad para uso personal, probablemente solo requiere revisión normal. "
            "Si son muchas unidades, tallas repetidas o mercancía para vender, consulta con OWC."
        ),
    },
    {
        "id": "perfumes_cosmetics",
        "label": "Perfumes, cremas, maquillaje y cosméticos",
        "level_hint": "mixed",
        "terms": [
            "perfume", "perfumes", "colonia", "colonias", "fragancia", "fragancias", "splash",
            "maquillaje", "cosmetico", "cosmético", "cosmeticos", "cosméticos", "crema",
            "cremas", "skincare", "labial", "base", "serum", "sérum", "champu", "champú",
            "shampoo", "locion", "loción", "gel", "tonico", "tónico",
        ],
        "db_terms": [
            "perfumes para uso comercial", "perfumes, cremas", "maquillaje y otros cosméticos",
            "cosméticos", "cosmeticos", "cremas", "similares",
        ],
        "examples": ["perfume", "colonia", "maquillaje", "cremas", "skincare"],
        "user_message": (
            "Perfumes, cremas y cosméticos pueden tener tratamiento distinto según sean para uso personal o comercial."
        ),
        "recommendation": (
            "Aclara cantidad y propósito. Para varias unidades o fines comerciales, consulta con OWC antes de enviar."
        ),
    },
    {
        "id": "beverages_energy",
        "label": "Bebidas gaseosas y/o energéticas",
        "level_hint": "restricted",
        "terms": [
            "bebida", "bebidas", "gaseosa", "gaseosas", "refresco", "refrescos",
            "energetica", "energética", "energeticas", "energéticas", "red bull",
            "monster", "soda", "malta",
        ],
        "db_terms": ["bebidas gaseosas", "energéticas", "energeticas"],
        "examples": ["refrescos", "bebidas energéticas", "soda"],
        "user_message": (
            "Las bebidas gaseosas o energéticas están bajo régimen especial y pueden requerir validación previa."
        ),
        "recommendation": (
            "Confirma con OWC cantidad, presentación y condiciones antes de enviar bebidas."
        ),
    },
    {
        "id": "satellite_telecom",
        "label": "Equipos y antenas satelitales",
        "level_hint": "prohibited",
        "terms": [
            "satelital", "satelitales", "antena", "antenas", "starlink", "directv",
            "router satelital", "modem satelital", "módem satelital",
        ],
        "db_terms": ["equipos y/o antenas satelitales", "antenas satelitales"],
        "examples": ["antena satelital", "Starlink", "equipo satelital"],
        "user_message": "Los equipos o antenas satelitales aparecen como artículos prohibidos.",
        "recommendation": "No los envíes por OWC sin confirmación oficial explícita.",
    },
    {
        "id": "weapons_security",
        "label": "Armas, municiones, defensa personal y seguridad",
        "level_hint": "prohibited",
        "terms": [
            "arma", "armas", "pistola", "rifle", "municion", "munición", "municiones",
            "cuchillo", "cuchillos", "navaja", "navajas", "arma blanca", "armas blancas",
            "gas pimienta", "pepper spray", "electroshock", "taser", "airsoft",
            "pistola de aire", "tirolina", "tirolinas", "china", "chinas", "resortera",
            "machete", "machetes", "mazo", "mazos", "batuta", "batutas", "rolo", "rolos",
            "arco", "arcos", "flecha", "flechas", "balines", "perdigones",
        ],
        "db_terms": [
            "pistola de aire", "municiones", "tirolinas", "gas pimienta", "machetes",
            "batutas", "electroshock", "airsoft", "cuchillos", "armas blancas",
            "arcos y flechas", "armas, explosivos",
        ],
        "examples": ["cuchillos", "gas pimienta", "pistola de aire", "airsoft", "machetes"],
        "user_message": (
            "Los artículos de armas, municiones, defensa personal o seguridad aparecen como prohibidos."
        ),
        "recommendation": "No envíes este tipo de artículos por OWC.",
    },
    {
        "id": "protective_security_gear",
        "label": "Protección personal, cascos y camuflaje",
        "level_hint": "prohibited",
        "terms": [
            "mascara de gas", "máscara de gas", "chaleco", "chalecos", "antibalas",
            "bala", "balas", "casco", "cascos", "guante protector", "guantes protectores",
            "protector", "protectores", "articulos deportivos de proteccion",
            "artículos deportivos de protección", "camuflaje", "camuflado", "militar",
        ],
        "db_terms": [
            "máscara de gas", "mascara de gas", "chalecos de protección", "artículos de camuflaje",
            "artículos deportivos de protección", "cascos", "guantes protectores",
        ],
        "examples": ["chaleco antibalas", "máscara de gas", "casco", "guantes protectores"],
        "user_message": "Los artículos de protección personal, camuflaje o seguridad aparecen como prohibidos.",
        "recommendation": "No los envíes por OWC sin confirmación oficial explícita.",
    },
    {
        "id": "projectiles_small_hard_objects",
        "label": "Proyectiles, rodamientos, metras y plomos",
        "level_hint": "prohibited",
        "terms": [
            "rodamiento", "rodamientos", "bola de rodamiento", "bolas de rodamientos",
            "marmol", "mármol", "marmoles", "mármoles", "metra", "metras",
            "canica", "canicas", "plomo", "plomos", "plomos de pesca", "pesas de pesca",
        ],
        "db_terms": ["bolas de rodamientos", "mármoles", "marmoles", "metras", "plomos de pesca"],
        "examples": ["rodamientos", "metras", "plomos de pesca"],
        "user_message": "Estos objetos aparecen en la lista de artículos prohibidos de OWC.",
        "recommendation": "No los envíes por OWC.",
    },
    {
        "id": "chemicals_hazardous",
        "label": "Químicos, inflamables, explosivos y material peligroso",
        "level_hint": "prohibited",
        "terms": [
            "quimico", "químico", "quimicos", "químicos", "liquido peligroso", "líquido peligroso",
            "inflamable", "inflamables", "explosivo", "explosivos", "gas", "gases",
            "polvora", "pólvora", "aerosol", "aerosoles", "fuego artificial", "fuegos artificiales",
            "petardo", "petardos", "bengala", "bengalas", "radioactivo", "radiactivo",
            "bateria de carro", "batería de carro", "bateria acido", "batería ácido", "acido", "ácido",
        ],
        "db_terms": [
            "productos químicos líquidos", "armas, explosivos, inflamables, gases y químicos",
            "fuegos artificiales", "pólvora", "polvora", "bengalas", "material radioactivo",
            "batería de carros de ácido",
        ],
        "examples": ["químicos", "inflamables", "pólvora", "batería de ácido"],
        "user_message": "Químicos, explosivos, inflamables, gases, material radioactivo o baterías de ácido aparecen como prohibidos.",
        "recommendation": "No los envíes por OWC.",
    },
    {
        "id": "documents_values_luxury",
        "label": "Documentos, valores, joyas y obras de arte",
        "level_hint": "prohibited",
        "terms": [
            "pasaporte", "pasaportes", "prorroga", "prórroga", "cedula", "cédula",
            "documento", "documentos", "identificacion", "identificación", "dinero",
            "efectivo", "cheque", "cheques", "valor", "valores", "joya", "joyas",
            "obra de arte", "obras de arte", "arte", "cuadro", "cuadros",
        ],
        "db_terms": [
            "pasaportes", "documentos de identificación", "documentos de identificacion",
            "valores", "efectivo", "cheques de viajero", "joyas", "obras de arte",
        ],
        "examples": ["pasaporte", "cédula", "efectivo", "joyas", "obras de arte"],
        "user_message": "Documentos personales, valores, efectivo, joyas u obras de arte aparecen como prohibidos.",
        "recommendation": "No los envíes por OWC sin confirmación oficial explícita.",
    },
    {
        "id": "drones_cameras",
        "label": "Drones o helicópteros con cámaras",
        "level_hint": "prohibited",
        "terms": [
            "drone", "drones", "helicoptero", "helicóptero", "helicopteros", "helicópteros",
            "camara drone", "cámara drone", "dji", "mavic", "phantom",
        ],
        "db_terms": ["drones", "helicópteros con cámaras", "helicopteros con camaras"],
        "examples": ["drone", "DJI", "helicóptero con cámara"],
        "user_message": "Drones o helicópteros con cámaras aparecen como artículos prohibidos.",
        "recommendation": "No los envíes por OWC.",
    },
    {
        "id": "animals_medical_bio",
        "label": "Animales, muestras médicas, tejidos y cultivos",
        "level_hint": "prohibited",
        "terms": [
            "animal", "animales", "mascota", "mascotas", "vivo", "vivos", "muerto", "muertos",
            "muestra medica", "muestra médica", "muestras medicas", "muestras médicas",
            "tejido", "tejidos", "cultivo", "cultivos", "biologico", "biológico",
        ],
        "db_terms": ["animales vivos o muertos", "muestras médicas", "tejidos", "cultivos"],
        "examples": ["animales", "muestras médicas", "tejidos", "cultivos"],
        "user_message": "Animales, muestras médicas, tejidos o cultivos aparecen como prohibidos.",
        "recommendation": "No los envíes por OWC.",
    },
    {
        "id": "adult_drugs_gambling_smuggling",
        "label": "Contenido adulto, drogas, juegos de azar y contrabando",
        "level_hint": "prohibited",
        "terms": [
            "pornografico", "pornográfico", "porno", "adulto", "estupefaciente", "estupefacientes",
            "psicotropico", "psicotrópico", "psicotropicos", "psicotrópicos", "droga", "drogas",
            "marihuana", "cannabis", "cocaina", "cocaína", "azar", "juego de azar",
            "juegos de azar", "envite", "suerte", "casino", "loteria", "lotería",
            "contrabando",
        ],
        "db_terms": [
            "material pornográfico", "sustancias estupefacientes", "psicotrópicas",
            "juegos de suerte", "envite", "azar", "contrabando",
        ],
        "examples": ["material pornográfico", "sustancias controladas", "juegos de azar"],
        "user_message": "Estos artículos aparecen como prohibidos por OWC.",
        "recommendation": "No los envíes por OWC.",
    },
]
OWC_SMART_ITEM_CATEGORIES = OWC_SMART_RESTRICTION_CATEGORIES


def normalize_owc_search_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", str(value))
    without_accents = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    cleaned = without_accents.lower()
    for char in ["/", "-", "_", ",", ".", "(", ")"]:
        cleaned = cleaned.replace(char, " ")
    return " ".join(cleaned.strip().split())


def owc_singularize_token(token: str) -> str:
    token = normalize_owc_search_text(token)
    if len(token) > 4 and token.endswith("es"):
        return token[:-2]
    if len(token) > 3 and token.endswith("s"):
        return token[:-1]
    return token


def owc_tokens(value: str | None) -> set[str]:
    tokens = {t for t in normalize_owc_search_text(value).split() if len(t) >= 2}
    tokens.update(owc_singularize_token(t) for t in list(tokens))
    return {t for t in tokens if t}


def owc_text_matches(text: str | None, term: str | None) -> bool:
    text_norm = normalize_owc_search_text(text)
    term_norm = normalize_owc_search_text(term)
    if not text_norm or not term_norm:
        return False
    if term_norm in text_norm or text_norm in term_norm:
        return True
    text_tokens = owc_tokens(text_norm)
    term_tokens = owc_tokens(term_norm)
    return bool(term_tokens) and term_tokens.issubset(text_tokens)


def owc_row_haystack(row: dict) -> str:
    return " ".join(normalize_owc_search_text(part) for part in [row.get("item_name"), row.get("restriction_level"), row.get("reason"), row.get("notes")] if part)


def find_owc_query_categories(query: str) -> list[dict]:
    query_norm = normalize_owc_search_text(query)
    query_tokens = owc_tokens(query_norm)
    categories = []
    for category in OWC_SMART_ITEM_CATEGORIES:
        terms = category.get("input_terms", [])
        if any(owc_text_matches(query_norm, term) or owc_text_matches(term, query_norm) for term in terms):
            categories.append(category)
            continue
        category_tokens = set()
        for term in terms:
            category_tokens.update(owc_tokens(term))
        if query_tokens and query_tokens.intersection(category_tokens):
            categories.append(category)
    return categories


def owc_row_matches_category(row: dict, category: dict) -> bool:
    haystack = owc_row_haystack(row)
    return any(owc_text_matches(haystack, term) for term in category.get("db_terms", []))


def owc_direct_match_score(query: str, row: dict) -> int | None:
    query_norm = normalize_owc_search_text(query)
    item_name = normalize_owc_search_text(row.get("item_name"))
    haystack = owc_row_haystack(row)
    if not query_norm:
        return None
    if owc_text_matches(item_name, query_norm):
        return 0
    query_tokens = owc_tokens(query_norm)
    item_tokens = owc_tokens(item_name)
    haystack_tokens = owc_tokens(haystack)
    if query_tokens and query_tokens.issubset(item_tokens):
        return 1
    if owc_text_matches(haystack, query_norm):
        return 2
    if query_tokens and query_tokens.intersection(item_tokens):
        return 3
    if query_tokens and query_tokens.intersection(haystack_tokens):
        return 4
    return None


def owc_display_level(level: str | None) -> str:
    normalized = normalize_owc_search_text(level)
    if normalized in {"prohibited", "prohibido"}:
        return "Prohibido"
    if normalized in {"restricted", "restringido", "special_regime", "regimen especial"}:
        return "Régimen especial"
    return "Revisar"


def owc_action(level: str | None) -> str:
    normalized = normalize_owc_search_text(level)
    if normalized in {"prohibited", "prohibido"}:
        return "block"
    if normalized in {"restricted", "restringido", "special_regime", "regimen especial"}:
        return "warn"
    return "review"


def owc_severity_score(level: str | None) -> int:
    action = owc_action(level)
    if action == "block":
        return 100
    if action == "warn":
        return 60
    return 30


def owc_default_user_message(row: dict) -> str:
    item_name = row.get("item_name") or "Este artículo"
    action = owc_action(row.get("restriction_level"))
    if action == "block":
        return f"{item_name} aparece como prohibido en las reglas actuales de OWC."
    if action == "warn":
        return f"{item_name} aparece como artículo restringido o bajo régimen especial. Puede requerir validación previa antes del envío."
    return f"{item_name} requiere revisión manual."


def owc_default_recommendation(row: dict) -> str:
    action = owc_action(row.get("restriction_level"))
    if action == "block":
        return "No lo envíes por OWC sin confirmación oficial del courier."
    if action == "warn":
        return "Consulta con OWC antes de enviar. Ten factura, descripción del producto y cantidad disponibles."
    return "Verifica manualmente con OWC si tienes dudas."


def build_owc_restricted_item_match(row: dict, query: str, category: dict | None, match_type: str, rank: int) -> dict:
    level = row.get("restriction_level") or ((category or {}).get("level_hint") or "review")
    if level == "mixed":
        level = "restricted"
    return {
        "id": row.get("id"),
        "item_name": row.get("item_name") or (category or {}).get("fallback_item_name") or (category or {}).get("label") or query,
        "restriction_level": level,
        "display_level": owc_display_level(level),
        "action": owc_action(level),
        "matched_input": query,
        "match_type": match_type,
        "confidence": max(30, 100 - (rank * 10)),
        "category_id": category.get("id") if category else None,
        "category_label": category.get("label") if category else None,
        "reason": row.get("reason") or ("Régimen especial" if owc_action(level) == "warn" else "Prohibido por courier" if owc_action(level) == "block" else "Revisión manual"),
        "notes": row.get("notes") or ("Coincidencia por categoría inteligente" if category else None),
        "user_message": category.get("user_message") if category else owc_default_user_message(row),
        "recommendation": category.get("recommendation") if category else owc_default_recommendation(row),
        "examples": category.get("examples", []) if category else [],
        "source_url": OWC_RESTRICTED_ITEMS_SOURCE_URL,
        "courier_id": row.get("courier_id"),
        "severity_score": owc_severity_score(level),
    }


def build_owc_virtual_match(category: dict, query: str) -> dict:
    level = category.get("level_hint", "review")
    if level == "mixed":
        level = "restricted"
    return build_owc_restricted_item_match(
        row={"id": f"virtual:{category['id']}", "item_name": category.get("fallback_item_name") or category.get("label"), "restriction_level": level, "reason": "Coincidencia por categoría inteligente", "notes": "No hubo coincidencia exacta en la tabla; se muestra una advertencia por alias/categoría.", "courier_id": None},
        query=query,
        category=category,
        match_type="virtual_category",
        rank=5,
    )


def smart_search_owc_restricted_items(rows: list[dict], query: str, limit: int = 10) -> tuple[list[dict], list[dict], list[str]]:
    categories = find_owc_query_categories(query)
    expanded_terms = {normalize_owc_search_text(query)}
    for category in categories:
        expanded_terms.update(normalize_owc_search_text(term) for term in category.get("input_terms", []))
        expanded_terms.update(normalize_owc_search_text(term) for term in category.get("db_terms", []))
    ranked: list[tuple[int, int, str, dict]] = []
    seen: set[str] = set()
    for row in rows:
        row_key = str(row.get("id") or row.get("item_name"))
        direct_score = owc_direct_match_score(query, row)
        if direct_score is not None:
            match = build_owc_restricted_item_match(row=row, query=query, category=None, match_type="direct", rank=direct_score)
            ranked.append((direct_score, -owc_severity_score(row.get("restriction_level")), row_key, match))
            seen.add(row_key)
            continue
        for category in categories:
            if row_key in seen or not owc_row_matches_category(row, category):
                continue
            match = build_owc_restricted_item_match(row=row, query=query, category=category, match_type="alias_category", rank=2)
            ranked.append((2, -owc_severity_score(row.get("restriction_level")), row_key, match))
            seen.add(row_key)
    if not ranked and categories:
        for category in categories:
            match = build_owc_virtual_match(category, query)
            ranked.append((5, -owc_severity_score(match.get("restriction_level")), f"virtual:{category['id']}", match))
    ranked.sort(key=lambda item: (item[0], item[1], str(item[3].get("item_name") or "").lower()))
    category_summaries = [{"id": c["id"], "label": c["label"], "level_hint": c["level_hint"], "examples": c.get("examples", []), "user_message": c.get("user_message"), "recommendation": c.get("recommendation")} for c in categories]
    return [item[3] for item in ranked[:limit]], category_summaries, sorted(term for term in expanded_terms if term)


class UpdateShipmentStatusRequest(BaseModel):
    status: Literal[
        "draft",
        "quoted",
        "confirmed",
        "paid",
        "shipped",
        "delivered",
        "cancelled",
    ]
    tracking_internal: str | None = None
    tracking_external: str | None = None
    notes: str | None = None


@app.get("/")
def root():
    return {"message": "API operativa"}


@app.get("/test-db")
def test_db():
    try:
        response = supabase_public.table("couriers").select("*").execute()
        return {
            "ok": True,
            "count": len(response.data),
            "data": response.data,
        }
    except Exception as e:
        return {
            "ok": False,
            "error": str(e),
        }


def generate_shipment_code(prefix: str = "QTE") -> str:
    now = datetime.now().strftime("%Y%m%d%H%M%S")
    short_id = str(uuid4())[:8].upper()
    return f"{prefix}-{now}-{short_id}"


def get_latest_exchange_rate(
    refresh_if_stale: bool = True,
    force_refresh: bool = False,
):
    latest = get_latest_exchange_rate_row()

    should_refresh, stale_reasons = should_refresh_exchange_rate(latest)
    refresh_attempted = False
    refresh_succeeded = False
    refresh_error = None

    if force_refresh or (refresh_if_stale and should_refresh):
        refresh_attempted = True

        try:
            scraped = fetch_bcv_usd_rate()
            latest = save_bcv_exchange_rate(scraped)
            refresh_succeeded = True
            should_refresh, stale_reasons = should_refresh_exchange_rate(latest)
        except Exception as exc:
            logger.exception("No se pudo refrescar BCV automáticamente")
            refresh_error = str(exc)

    if not latest:
        raise HTTPException(
            status_code=503,
            detail="No hay tasa BCV guardada y no se pudo refrescar automáticamente",
        )

    latest = dict(latest)
    latest["_freshness"] = {
        "stale": should_refresh,
        "reasons": stale_reasons,
        "refresh_attempted": refresh_attempted,
        "refresh_succeeded": refresh_succeeded,
        "refresh_error": refresh_error,
        "message": (
            "Tasa BCV actualizada automáticamente"
            if refresh_succeeded
            else "No se pudo refrescar BCV; se usa la última tasa guardada"
            if refresh_attempted and refresh_error
            else "Tasa BCV cargada desde la base de datos"
        ),
    }

    return latest


def get_courier_by_code(courier_code: str):
    result = (
        supabase_public
        .table("couriers")
        .select("*")
        .eq("code", courier_code)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(
            status_code=404,
            detail=f"No existe courier con code='{courier_code}'",
        )

    return result.data[0]


def get_rate_for_courier(courier_id: str, service_type: str):
    query = (
        supabase_public
        .table("courier_rates")
        .select("*")
        .eq("courier_id", courier_id)
        .eq("service_type", service_type)
        .eq("active", True)
    )

    if service_type == "air":
        result = (
            query
            .in_("charge_unit", ["kg", "lb"])
            .limit(1)
            .execute()
        )
    elif service_type == "sea":
        result = (
            query
            .eq("charge_unit", "ft3")
            .limit(1)
            .execute()
        )
    else:
        result = query.limit(1).execute()

    if not result.data:
        raise HTTPException(
            status_code=404,
            detail=f"No se encontró tarifa activa para courier_id={courier_id} y service_type={service_type}",
        )

    return result.data[0]


def upsert_courier_rate(
    courier_id: str,
    service_type: str,
    charge_unit: str,
    currency: str,
    rate_value: float,
    handling_fee: float,
    handling_fee_currency: str = "VES",
    region: str = "region_central",
):
    existing_result = (
        db_client
        .table("courier_rates")
        .select("*")
        .eq("courier_id", courier_id)
        .eq("service_type", service_type)
        .eq("charge_unit", charge_unit)
        .limit(1)
        .execute()
    )

    payload = {
        "courier_id": courier_id,
        "service_type": service_type,
        "charge_unit": charge_unit,
        "currency": currency,
        "rate": rate_value,
        "minimum_charge_units": 1,
        "handling_fee": handling_fee,
        "handling_fee_currency": handling_fee_currency,
        "insurance_percent": 0,
        "packaging_fee": 0,
        "packaging_fee_currency": handling_fee_currency,
        "applies_to_delivery_type": "office",
        "region": region,
        "active": True,
    }

    if existing_result.data:
        row_id = existing_result.data[0]["id"]
        update_result = (
            db_client
            .table("courier_rates")
            .update(payload)
            .eq("id", row_id)
            .execute()
        )
        return update_result.data[0] if update_result.data else payload

    insert_result = (
        db_client
        .table("courier_rates")
        .insert(payload)
        .execute()
    )
    return insert_result.data[0] if insert_result.data else payload


def get_shipment_by_id(shipment_id: str):
    result = (
        supabase_public
        .table("shipments")
        .select("*")
        .eq("id", shipment_id)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="Shipment no encontrado")

    return result.data[0]


def get_shipment_items_by_id(shipment_id: str):
    result = (
        supabase_public
        .table("shipment_items")
        .select("*")
        .eq("shipment_id", shipment_id)
        .order("created_at", desc=False)
        .execute()
    )
    return result.data or []


def check_restricted_items(courier_id: str, items: list):
    if not items:
        return []

    restrictions = (
        supabase_public
        .table("restricted_items")
        .select("*")
        .eq("courier_id", courier_id)
        .eq("active", True)
        .execute()
    )

    restricted_matches = []
    db_items = restrictions.data or []
    input_names = [item.item_name.strip().lower() for item in items]

    for db_row in db_items:
        db_name = (db_row.get("item_name") or "").strip().lower()

        for input_name in input_names:
            if db_name and db_name in input_name:
                restricted_matches.append(
                    RestrictedItemMatch(
                        item_name=db_row["item_name"],
                        restriction_level=db_row["restriction_level"],
                        matched_input=input_name,
                        reason=db_row.get("reason"),
                        notes=db_row.get("notes"),
                    )
                )

    return restricted_matches


def get_courier_business_rules(courier_id: str):
    result = (
        supabase_public
        .table("courier_business_rules")
        .select("*")
        .eq("courier_id", courier_id)
        .eq("active", True)
        .execute()
    )
    return result.data or []


def _find_rule_row(
    rules_rows: list[dict],
    rule_code: str,
    service_type_key: str = "*",
    region_key: str = "*",
):
    preferred = [
        (rule_code, service_type_key, region_key),
        (rule_code, service_type_key, "*"),
        (rule_code, "*", region_key),
        (rule_code, "*", "*"),
    ]

    for code, service_key, region in preferred:
        for row in rules_rows:
            if (
                row["rule_code"] == code
                and row["service_type_key"] == service_key
                and row["region_key"] == region
            ):
                return row

    return None


def get_rule_number(
    rules_rows: list[dict],
    rule_code: str,
    service_type_key: str = "*",
    region_key: str = "*",
    default: float = 0,
):
    row = _find_rule_row(rules_rows, rule_code, service_type_key, region_key)
    if not row or row.get("numeric_value") is None:
        return default
    return float(row["numeric_value"])


def get_rule_bool(
    rules_rows: list[dict],
    rule_code: str,
    service_type_key: str = "*",
    region_key: str = "*",
    default: bool = False,
):
    row = _find_rule_row(rules_rows, rule_code, service_type_key, region_key)
    if not row or row.get("bool_value") is None:
        return default
    return bool(row["bool_value"])


def build_owc_rules(courier_id: str, region: str):
    rows = get_courier_business_rules(courier_id)

    return {
        "air_base_rate_ves": get_rule_number(rows, "air_base_rate_ves", "air", region, 4248),
        "sea_base_rate_ves": get_rule_number(rows, "sea_base_rate_ves", "sea", region, 29931),
        "correspondence_rate_ves": get_rule_number(rows, "correspondence_rate_ves", "correspondence", region, 1834),
        "volumetric_divisor_in3_per_lb": get_rule_number(rows, "volumetric_divisor_in3_per_lb", "*", "*", 166),
        "air_min_lb": get_rule_number(rows, "air_min_lb", "air", region, 1),
        "sea_min_ft3": get_rule_number(rows, "sea_min_ft3", "sea", region, 1),
        "handling_fee_ves": get_rule_number(rows, "handling_fee_ves", "*", "*", 917),
        "handling_fee_usd_documented": get_rule_number(rows, "handling_fee_usd_documented", "*", "*", 1),
        "repack_fee_amount": get_rule_number(rows, "repack_fee_amount", "*", "*", 5),
        "repack_fee_currency": (
            _find_rule_row(rows, "repack_fee_amount", "*", "*") or {}
        ).get("currency_code", "USD"),
        "repack_min_air_lb": get_rule_number(rows, "repack_min_air_lb", "air", "*", 5),
        "repack_min_sea_ft3": get_rule_number(rows, "repack_min_sea_ft3", "sea", "*", 3),
        "repack_storage_exempt": get_rule_bool(rows, "repack_storage_exempt", "*", "*", True),
        "insurance_percent": get_rule_number(rows, "insurance_percent", "*", "*", 0.05),
        "general_hold_free_business_days": int(get_rule_number(rows, "general_hold_free_business_days", "*", "*", 3)),
        "storage_fee_ves_per_day_ft3": get_rule_number(rows, "storage_fee_ves_per_day_ft3", "*", "*", 0),
        "purchase_by_order_threshold_usd": get_rule_number(rows, "purchase_by_order_threshold_usd", "*", "*", 100),
        "purchase_by_order_lt_threshold_percent": get_rule_number(rows, "purchase_by_order_lt_threshold_percent", "*", "*", 0.20),
        "purchase_by_order_gte_threshold_percent": get_rule_number(rows, "purchase_by_order_gte_threshold_percent", "*", "*", 0.15),
        "provisional_customs_percent": get_rule_number(rows, "provisional_customs_percent", "*", "*", 0.16),
        "provisional_customs_qty_threshold": int(get_rule_number(rows, "provisional_customs_qty_threshold", "*", "*", 4)),
        "provisional_customs_value_threshold_usd": get_rule_number(rows, "provisional_customs_value_threshold_usd", "*", "*", 200),
        "storage_charge_min_ft3": get_rule_number(rows, "storage_charge_min_ft3", "*", "*", 1),
    }


def build_zoom_rules(courier_id: str, region: str):
    rows = get_courier_business_rules(courier_id)

    return {
        "air_rate_usd_per_kg": get_rule_number(
            rows, "air_rate_usd_per_kg", "air", region, 32.40
        ),
        "billable_weight_step_kg": get_rule_number(
            rows, "billable_weight_step_kg", "air", region, 0.5
        ),
        "protection_percent": get_rule_number(
            rows, "protection_percent", "air", "*", 0.01
        ),
        "protection_min_usd": get_rule_number(
            rows, "protection_min_usd", "air", "*", 1.20
        ),
        "consolidation_fee_usd": get_rule_number(
            rows, "consolidation_fee_usd", "air", "*", 6.00
        ),
        "air_rate_usd_per_half_kg": get_rule_number(
            rows, "air_rate_usd_per_half_kg", "air", region, 6
        ),
        "sea_rate_usd_per_ft3": get_rule_number(
            rows, "sea_rate_usd_per_ft3", "sea", region, 29
        ),
        "handling_fee_usd": get_rule_number(
            rows, "handling_fee_usd", "*", "*", 0
        ),
        "packaging_fee_usd": get_rule_number(
            rows, "packaging_fee_usd", "*", "*", 0
        ),
        "insurance_air_min_usd": get_rule_number(
            rows, "insurance_air_min_usd", "air", "*", 1
        ),
        "insurance_air_percent": get_rule_number(
            rows, "insurance_air_percent", "air", "*", 0.01
        ),
        "insurance_sea_min_usd": get_rule_number(
            rows, "insurance_sea_min_usd", "sea", "*", 5
        ),
        "insurance_sea_percent": get_rule_number(
            rows, "insurance_sea_percent", "sea", "*", 0.05
        ),
    }


def build_quote_result(payload):
    courier = get_courier_by_code(payload.courier_code)
    exchange = get_latest_exchange_rate()
    restricted_matches = check_restricted_items(courier["id"], payload.items)
    region = getattr(payload, "region", "region_central")
    payload_dict = payload.model_dump()

    if payload.courier_code == "owc":
        owc_rules = build_owc_rules(courier["id"], region)

        quote = calculate_owc_quote(
            rules=owc_rules,
            exchange_rate=float(exchange["rate"]),
            payload=payload_dict,
        )

        rate = {
            "source": "courier_business_rules",
            "service_type": payload.service_type,
            "region": region,
            "air_base_rate_ves": owc_rules["air_base_rate_ves"],
            "sea_base_rate_ves": owc_rules["sea_base_rate_ves"],
            "correspondence_rate_ves": owc_rules["correspondence_rate_ves"],
            "handling_fee_ves": owc_rules["handling_fee_ves"],
            "handling_fee_currency": "VES",
            "packaging_fee_currency": "VES",
            "insurance_percent": owc_rules["insurance_percent"],
        }

        return courier, rate, exchange, restricted_matches, quote

    if payload.courier_code == "zoom":
        zoom_rules = build_zoom_rules(courier["id"], region)

        quote = calculate_zoom_quote(
            rules=zoom_rules,
            exchange_rate=float(exchange["rate"]),
            payload=payload_dict,
        )

        rate = {
            "source": "courier_business_rules",
            "service_type": payload.service_type,
            "region": region,
            "air_rate_usd_per_kg": zoom_rules["air_rate_usd_per_kg"],
            "billable_weight_step_kg": zoom_rules["billable_weight_step_kg"],
            "protection_percent": zoom_rules["protection_percent"],
            "protection_min_usd": zoom_rules["protection_min_usd"],
            "consolidation_fee_usd": zoom_rules["consolidation_fee_usd"],
            "air_rate_usd_per_half_kg": zoom_rules["air_rate_usd_per_half_kg"],
            "sea_rate_usd_per_ft3": zoom_rules["sea_rate_usd_per_ft3"],
            "handling_fee_usd": zoom_rules["handling_fee_usd"],
            "handling_fee_currency": "USD",
            "packaging_fee_usd": zoom_rules["packaging_fee_usd"],
            "packaging_fee_currency": "USD",
            "insurance_air_min_usd": zoom_rules["insurance_air_min_usd"],
            "insurance_air_percent": zoom_rules["insurance_air_percent"],
            "insurance_sea_min_usd": zoom_rules["insurance_sea_min_usd"],
            "insurance_sea_percent": zoom_rules["insurance_sea_percent"],
        }

        return courier, rate, exchange, restricted_matches, quote

    rate = get_rate_for_courier(courier["id"], payload.service_type)

    quote = calculate_quote(
        rate_row=rate,
        exchange_rate=float(exchange["rate"]),
        declared_value_usd=payload.declared_value_usd,
        total_weight_kg=payload.total_weight_kg,
        total_weight_lb=payload.total_weight_lb,
        total_volume_ft3=payload.total_volume_ft3,
        total_same_item_qty=payload.total_same_item_qty,
    )

    return courier, rate, exchange, restricted_matches, quote


@app.get("/courier-rules/owc")
def get_owc_rules(
    region: str = "region_central",
    refresh_if_stale: bool = False,
    force: bool = False,
):
    try:
        courier = get_courier_by_code("owc")
        rows = get_courier_business_rules(courier["id"])
        freshness = analyze_owc_rules_freshness(rows, region)

        refresh_attempted = False
        refresh_succeeded = False
        refresh_error = None
        refresh_result = None

        if force or (refresh_if_stale and freshness["stale"]):
            refresh_attempted = True

            try:
                refresh_result = refresh_owc_business_rules(db_client, region=region)
                refresh_succeeded = owc_refresh_updated_any_rules(refresh_result)

                rows = get_courier_business_rules(courier["id"])
                freshness = analyze_owc_rules_freshness(rows, region)
            except Exception as exc:
                logger.exception("No se pudo refrescar OWC automáticamente region=%s", region)
                refresh_error = str(exc)

        rules = build_owc_rules(courier["id"], region)

        return {
            "courier": courier["name"],
            "courier_code": courier["code"],
            "region": region,
            "rules": rules,
            "freshness": {
                "stale": bool(freshness["stale"]),
                "reasons": freshness["reasons"],
                "oldest_updated_at": freshness["oldest_updated_at"],
                "refresh_attempted": refresh_attempted,
                "refresh_succeeded": refresh_succeeded,
                "refresh_error": refresh_error,
                "message": (
                    "Tarifario OWC actualizado automáticamente"
                    if refresh_succeeded
                    else "No se pudo refrescar OWC; se usan tarifas guardadas"
                    if refresh_attempted and refresh_error
                    else "Tarifario OWC cargado desde la base de datos"
                ),
                "refresh_result": refresh_result,
            },
        }
    except Exception as e:
        logger.exception("Error obteniendo reglas OWC region=%s", region)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/couriers/owc/preview-rates")
def preview_owc_rates(region: str = "region_central"):
    try:
        return scrape_owc_public_rates(region=region)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/couriers/owc/restricted-items/categories")
def list_owc_restricted_item_categories():
    return {
        "courier_code": "owc",
        "source_url": OWC_RESTRICTED_ITEMS_SOURCE_URL,
        "categories": [
            {
                "id": category["id"],
                "label": category["label"],
                "level_hint": category["level_hint"],
                "examples": category.get("examples", []),
                "user_message": category.get("user_message"),
                "recommendation": category.get("recommendation"),
            }
            for category in OWC_SMART_ITEM_CATEGORIES
        ],
    }


@app.get("/couriers/owc/restricted-items")
def search_owc_restricted_items(
    q: str = Query("", min_length=0, max_length=120),
    limit: int = Query(10, ge=1, le=25),
):
    try:
        query = q.strip()
        normalized_query = normalize_owc_search_text(query)
        if not normalized_query:
            return {
                "query": q,
                "normalized_query": "",
                "courier_code": "owc",
                "matches": [],
                "matched_categories": [],
                "expanded_terms": [],
                "count": 0,
                "status": "empty",
                "message": "Escribe un artículo para verificar si tiene restricciones OWC.",
                "source_url": OWC_RESTRICTED_ITEMS_SOURCE_URL,
            }

        courier = get_courier_by_code("owc")
        result = (
            (db_client or supabase_public)
            .table("restricted_items")
            .select("*")
            .eq("courier_id", courier["id"])
            .eq("active", True)
            .execute()
        )
        rows = result.data or []
        matches, matched_categories, expanded_terms = smart_search_owc_restricted_items(rows=rows, query=query, limit=limit)

        has_prohibited = any(normalize_owc_search_text(m.get("restriction_level")) in {"prohibited", "prohibido"} for m in matches)
        has_restricted = any(normalize_owc_search_text(m.get("restriction_level")) in {"restricted", "restringido", "special_regime", "regimen especial"} for m in matches)

        if has_prohibited:
            status = "prohibited"
            message = "Se encontraron artículos prohibidos relacionados con tu búsqueda. No envíes este artículo sin confirmación oficial de OWC."
        elif has_restricted:
            status = "restricted"
            message = "Se encontraron artículos restringidos o bajo régimen especial. Puede requerir validación previa antes del envío."
        elif matched_categories:
            status = "review"
            message = "La búsqueda coincide con una categoría sensible, pero no se encontró una regla exacta. Revisa la recomendación y confirma con OWC si tienes dudas."
        else:
            status = "not_found"
            message = "No se encontraron restricciones en la base actual. Esto no garantiza que el artículo esté permitido; verifica manualmente si tienes dudas."

        return {
            "query": query,
            "normalized_query": normalized_query,
            "courier_code": "owc",
            "courier": courier.get("name"),
            "matches": matches,
            "matched_categories": matched_categories,
            "expanded_terms": expanded_terms,
            "count": len(matches),
            "status": status,
            "message": message,
            "source_url": OWC_RESTRICTED_ITEMS_SOURCE_URL,
        }

    except Exception as e:
        logger.exception("Error buscando artículos restringidos OWC q=%s", q)
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/couriers/owc/refresh-rates")
def refresh_owc_rates(region: str = "region_central"):
    try:
        return refresh_owc_business_rules(db_client, region=region)
    except Exception as e:
        logger.exception("Error refrescando tarifas OWC region=%s", region)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exchange-rate/latest")
def exchange_rate_latest(
    refresh_if_stale: bool = True,
    force: bool = False,
):
    try:
        exchange = get_latest_exchange_rate(
            refresh_if_stale=refresh_if_stale,
            force_refresh=force,
        )
        freshness = exchange.get("_freshness", {})

        return {
            "source": exchange["source"],
            "currency_from": exchange["currency_from"],
            "currency_to": exchange["currency_to"],
            "rate": float(exchange["rate"]),
            "rate_date": exchange["rate_date"],
            "fetched_at": get_exchange_fetched_at(exchange),
            "stale": bool(freshness.get("stale", False)),
            "freshness": freshness,
            "message": freshness.get("message"),
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        logger.exception("Error obteniendo tasa BCV")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/exchange-rate/refresh-bcv")
def refresh_bcv_exchange_rate():
    try:
        scraped = fetch_bcv_usd_rate()
        saved_row = save_bcv_exchange_rate(scraped)

        return {
            "message": "Tasa BCV actualizada correctamente",
            "scraped": scraped,
            "saved": saved_row,
        }

    except Exception as e:
        logger.exception("Error refrescando tasa BCV")
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/shipments")
def list_shipments(
    limit: int = Query(20, ge=1, le=100),
    courier_code: str | None = None,
    status: str | None = None,
):
    try:
        query = (
            supabase_public
            .table("shipments")
            .select("""
                id,
                code,
                courier_id,
                service_type,
                delivery_type,
                customer_name,
                customer_phone,
                customer_email,
                declared_value_usd,
                package_count,
                total_weight_kg,
                total_weight_lb,
                total_volume_ft3,
                total_usd,
                total_ves,
                exchange_rate_used,
                status,
                created_at,
                updated_at
            """)
            .order("created_at", desc=True)
            .limit(limit)
        )

        if status:
            query = query.eq("status", status)

        shipments_result = query.execute()
        shipments = shipments_result.data or []

        if courier_code:
            courier = get_courier_by_code(courier_code)
            shipments = [s for s in shipments if s["courier_id"] == courier["id"]]

        courier_rows = (
            supabase_public
            .table("couriers")
            .select("id, code, name")
            .execute()
        ).data or []

        courier_map = {c["id"]: c for c in courier_rows}

        enriched = []
        for shipment in shipments:
            courier_info = courier_map.get(shipment["courier_id"], {})
            enriched.append({
                **shipment,
                "courier_code": courier_info.get("code"),
                "courier_name": courier_info.get("name"),
            })

        return {
            "count": len(enriched),
            "data": enriched,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/shipments/{shipment_id}")
def get_shipment_detail(shipment_id: str):
    try:
        shipment = get_shipment_by_id(shipment_id)

        courier_result = (
            supabase_public
            .table("couriers")
            .select("id, code, name")
            .eq("id", shipment["courier_id"])
            .limit(1)
            .execute()
        )

        courier_data = courier_result.data[0] if courier_result.data else None
        items = get_shipment_items_by_id(shipment_id)

        return {
            "shipment": {
                **shipment,
                "courier_code": courier_data["code"] if courier_data else None,
                "courier_name": courier_data["name"] if courier_data else None,
            },
            "items": items,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/shipments/{shipment_id}/items")
def get_shipment_items(shipment_id: str):
    try:
        _ = get_shipment_by_id(shipment_id)
        items = get_shipment_items_by_id(shipment_id)

        return {
            "shipment_id": shipment_id,
            "count": len(items),
            "data": items,
        }

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.patch("/shipments/{shipment_id}/status")
def update_shipment_status(shipment_id: str, payload: UpdateShipmentStatusRequest):
    try:
        existing = get_shipment_by_id(shipment_id)

        update_data = {
            "status": payload.status,
        }

        if payload.tracking_internal is not None:
            update_data["tracking_internal"] = payload.tracking_internal

        if payload.tracking_external is not None:
            update_data["tracking_external"] = payload.tracking_external

        if payload.notes is not None:
            previous_notes = existing.get("notes") or ""
            if previous_notes.strip():
                update_data["notes"] = f"{previous_notes}\n{payload.notes}"
            else:
                update_data["notes"] = payload.notes

        update_result = (
            db_client
            .table("shipments")
            .update(update_data)
            .eq("id", shipment_id)
            .execute()
        )

        if not update_result.data:
            raise HTTPException(status_code=500, detail="No se pudo actualizar el shipment")

        return {
            "message": "Shipment actualizado correctamente",
            "shipment": update_result.data[0],
        }

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quote/test/zoom")
def quote_test_zoom():
    try:
        courier = get_courier_by_code("zoom")
        exchange = get_latest_exchange_rate()
        region = "region_central"
        rules = build_zoom_rules(courier["id"], region)

        payload = {
            "service_type": "air",
            "declared_value_usd": 201,
            "total_weight_kg": 1,
            "total_weight_lb": 2.20462,
            "total_volume_ft3": 0,
            "length_in": 0,
            "width_in": 0,
            "height_in": 0,
            "tracking_count": 1,
            "enable_handling_fee": False,
            "enable_repack_fee": False,
            "hold_mode": "none",
            "hold_days": 0,
            "use_insurance": True,
            "use_purchase_by_order": False,
            "apply_provisional_customs": False,
            "items": [],
        }

        result = calculate_zoom_quote(
            rules=rules,
            exchange_rate=float(exchange["rate"]),
            payload=payload,
        )

        return {
            "courier": courier["name"],
            "rules_used": rules,
            "quote": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quote/test/owc-air")
def quote_test_owc_air():
    try:
        courier = get_courier_by_code("owc")
        rate = get_rate_for_courier(courier["id"], "air")
        exchange = get_latest_exchange_rate()

        result = calculate_quote(
            rate_row=rate,
            exchange_rate=float(exchange["rate"]),
            declared_value_usd=201,
            total_weight_kg=1,
            total_weight_lb=1,
            total_volume_ft3=0,
            total_same_item_qty=2,
        )

        return {
            "courier": courier["name"],
            "rate_used": rate,
            "quote": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/quote/test/owc-sea")
def quote_test_owc_sea():
    try:
        courier = get_courier_by_code("owc")
        rate = get_rate_for_courier(courier["id"], "sea")
        exchange = get_latest_exchange_rate()

        result = calculate_quote(
            rate_row=rate,
            exchange_rate=float(exchange["rate"]),
            declared_value_usd=201,
            total_weight_kg=0,
            total_weight_lb=0,
            total_volume_ft3=1,
            total_same_item_qty=2,
        )

        return {
            "courier": courier["name"],
            "rate_used": rate,
            "quote": result,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/quote/calculate", response_model=QuoteCalculateResponse)
def quote_calculate(payload: QuoteCalculateRequest):
    try:
        courier, rate, exchange, restricted_matches, quote = build_quote_result(payload)

        return QuoteCalculateResponse(
            courier=courier["name"],
            courier_code=courier["code"],
            service_type=payload.service_type,
            exchange_rate_used=float(exchange["rate"]),
            restricted_matches=restricted_matches,
            quote=quote,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/quote/calculate-and-save", response_model=QuoteSaveResponse)
def quote_calculate_and_save(payload: QuoteSaveRequest):
    try:
        courier, rate, exchange, restricted_matches, quote = build_quote_result(payload)

        shipment_code = generate_shipment_code()

        handling_fee_currency = rate.get("handling_fee_currency", "VES")
        packaging_fee_currency = rate.get("packaging_fee_currency", "VES")

        shipment_payload = {
            "code": shipment_code,
            "courier_id": courier["id"],
            "service_type": payload.service_type,
            "delivery_type": payload.delivery_type,
            "customer_name": payload.customer_name,
            "customer_phone": payload.customer_phone,
            "customer_email": payload.customer_email,
            "declared_value_usd": payload.declared_value_usd,
            "package_count": len(payload.items) if payload.items else 1,
            "total_weight_kg": payload.total_weight_kg,
            "total_weight_lb": payload.total_weight_lb,
            "total_volume_ft3": payload.total_volume_ft3,
            "uses_minimum_charge": quote["uses_minimum_charge"],
            "insurance_amount_usd": quote["breakdown"]["insurance_usd"],
            "handling_amount": (
                quote["breakdown"]["handling_ves"]
                if handling_fee_currency == "VES"
                else quote["breakdown"]["handling_usd"]
            ),
            "handling_currency": handling_fee_currency,
            "packaging_amount": (
                quote["breakdown"]["packaging_ves"]
                if packaging_fee_currency == "VES"
                else quote["breakdown"]["packaging_usd"]
            ),
            "packaging_currency": packaging_fee_currency,
            "customs_tax_usd": quote["breakdown"]["customs_tax_usd"],
            "subtotal_usd": quote["total_usd"],
            "subtotal_ves": quote["total_ves"],
            "exchange_rate_used": float(exchange["rate"]),
            "total_usd": quote["total_usd"],
            "total_ves": quote["total_ves"],
            "status": "quoted",
            "calculation_breakdown": quote,
            "notes": payload.notes,
        }

        shipment_insert = (
            db_client
            .table("shipments")
            .insert(shipment_payload)
            .execute()
        )

        if not shipment_insert.data:
            raise HTTPException(status_code=500, detail="No se pudo guardar shipment")

        shipment = shipment_insert.data[0]
        shipment_id = shipment["id"]

        items_to_insert = []
        for item in payload.items:
            normalized_name = item.item_name.strip().lower()

            matched_restriction = next(
                (
                    r for r in restricted_matches
                    if r.matched_input == normalized_name
                ),
                None,
            )

            items_to_insert.append({
                "shipment_id": shipment_id,
                "item_name": item.item_name,
                "category": item.category,
                "quantity": item.quantity,
                "unit_price_usd": item.unit_price_usd,
                "total_price_usd": item.unit_price_usd * item.quantity,
                "weight_kg": item.weight_kg,
                "weight_lb": item.weight_lb,
                "length_in": item.length_in,
                "width_in": item.width_in,
                "height_in": item.height_in,
                "volume_ft3": item.volume_ft3,
                "is_restricted": matched_restriction is not None,
                "restriction_note": (
                    f"{matched_restriction.restriction_level}: {matched_restriction.item_name}"
                    if matched_restriction
                    else None
                ),
            })

        if items_to_insert:
            db_client.table("shipment_items").insert(items_to_insert).execute()

        return QuoteSaveResponse(
            shipment_id=shipment_id,
            shipment_code=shipment_code,
            courier=courier["name"],
            courier_code=courier["code"],
            service_type=payload.service_type,
            exchange_rate_used=float(exchange["rate"]),
            restricted_matches=restricted_matches,
            quote=quote,
        )

    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
