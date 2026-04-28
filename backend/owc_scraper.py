import re
import logging
from html import unescape
from decimal import Decimal, ROUND_HALF_UP
from typing import Any
from urllib.request import Request, urlopen
from urllib.error import URLError, HTTPError

OWC_TARIFFS_URL = "https://onewaycargo.net/tarifas"
OWC_TARIFFS_SOURCE_URL = "https://onewaycargo.net/tarifas#calculadora"
logger = logging.getLogger(__name__)

REGION_TAB_TEXT = {
    "region_central": "Región central",
    "resto_pais": "Resto del país",
}

REGION_COUNTER_IDS = {
    "region_central": {
        "air": "aereoCentral",
        "sea": "maritimoRegularCentral",
        "correspondence": "correspondenciaCentral",
    },
    "resto_pais": {
        "air": "aereoRestoDelPais",
        "sea": "maritimoRegularRestoDelPais",
        "correspondence": "correspondenciaRestoDelPais",
    },
}

REGION_MINIMUMS = {
    "region_central": {
        "air_min_lb": Decimal("1"),
        "sea_min_ft3": Decimal("1"),
    },
    "resto_pais": {
        "air_min_lb": Decimal("6"),
        "sea_min_ft3": Decimal("2"),
    },
}


def _round_2(value: Decimal) -> float:
    return float(value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def _clean_spaces(text: str) -> str:
    return re.sub(r"\s+", " ", text).strip()


def _normalize_number_string(raw: str) -> str:
    """
    Convierte formatos como:
    4.253
    29.967
    1.837
    4,253
    29,967
    483,33790000
    a una forma decimal estable.
    """
    raw = raw.strip()
    raw = raw.replace("Bs", "").replace("$", "").replace("USD", "").strip()

    if "." in raw and "," in raw:
        if raw.rfind(",") > raw.rfind("."):
            raw = raw.replace(".", "").replace(",", ".")
        else:
            raw = raw.replace(",", "")
        return raw

    if "," in raw:
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) in (1, 2, 3, 4, 5, 6, 7, 8):
            if len(parts[1]) > 3:
                return raw.replace(".", "").replace(",", ".")
            return raw.replace(",", "")
        return raw.replace(",", "")

    if "." in raw:
        parts = raw.split(".")
        if len(parts) == 2 and len(parts[1]) > 3:
            return raw
        return raw.replace(".", "")

    return raw


def _to_decimal(raw: str) -> Decimal:
    normalized = _normalize_number_string(raw)
    return Decimal(normalized)


def _extract_first_decimal(pattern: str, text: str) -> Decimal | None:
    match = re.search(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    if not match:
        return None
    return _to_decimal(match.group(1))


def _extract_all_decimals(pattern: str, text: str) -> list[Decimal]:
    matches = re.findall(pattern, text, flags=re.IGNORECASE | re.DOTALL)
    return [_to_decimal(m) for m in matches]


def _fetch_tariffs_html() -> str:
    try:
        request = Request(
            OWC_TARIFFS_SOURCE_URL,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/124.0 Safari/537.36"
                )
            },
        )
        with urlopen(request, timeout=30) as response:
            charset = response.headers.get_content_charset() or "utf-8"
            return response.read().decode(charset, errors="replace")
    except (HTTPError, URLError, TimeoutError) as exc:
        raise RuntimeError(f"No se pudo descargar la página de tarifas OWC: {exc}") from exc


def _html_to_text(html: str) -> str:
    text = re.sub(r"<script\b[^>]*>.*?</script>", " ", html, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<style\b[^>]*>.*?</style>", " ", text, flags=re.IGNORECASE | re.DOTALL)
    text = re.sub(r"<[^>]+>", " ", text)
    return _clean_spaces(unescape(text))


def _extract_counter_decimal(html: str, element_id: str) -> Decimal | None:
    patterns = [
        rf"counter\(\s*['\"]#{re.escape(element_id)}['\"]\s*,\s*([\d\.,]+)\s*\)",
        rf"counter\(\s*['\"]{re.escape(element_id)}['\"]\s*,\s*([\d\.,]+)\s*\)",
    ]
    values: list[Decimal] = []

    for pattern in patterns:
        values.extend(_extract_all_decimals(pattern, html))

    positive_values = [value for value in values if value > 0]
    if positive_values:
        return max(positive_values)
    if values:
        return values[-1]
    return None


def _extract_handling_fee(text: str) -> Decimal | None:
    handling_matches = _extract_all_decimals(
        r"Handling Fee de\s+([\d\.,]+)\s*Bs",
        text,
    )
    return handling_matches[0] if handling_matches else None


def scrape_owc_public_rates(region: str = "region_central") -> dict[str, Any]:
    """
    Extrae tarifas públicas de OWC desde los valores counter(...) embebidos
    en la página oficial.
    """
    if region not in REGION_COUNTER_IDS:
        raise ValueError(f"Región OWC inválida: {region}")

    html = _fetch_tariffs_html()
    page_text = _html_to_text(html)
    counter_ids = REGION_COUNTER_IDS[region]

    air_rate = _extract_counter_decimal(html, counter_ids["air"])
    sea_rate = _extract_counter_decimal(html, counter_ids["sea"])
    correspondence_rate = _extract_counter_decimal(html, counter_ids["correspondence"])
    handling_fee = _extract_handling_fee(page_text)
    air_min_lb = REGION_MINIMUMS[region]["air_min_lb"]
    sea_min_ft3 = REGION_MINIMUMS[region]["sea_min_ft3"]

    public_rates_available = all(
        value is not None and value > 0
        for value in [air_rate, sea_rate, correspondence_rate, handling_fee]
    )

    if not public_rates_available:
        logger.warning(
            "OWC scraping incompleto para %s: air=%s sea=%s correspondence=%s handling=%s",
            region,
            air_rate,
            sea_rate,
            correspondence_rate,
            handling_fee,
        )

    return {
        "courier_code": "owc",
        "url": OWC_TARIFFS_SOURCE_URL,
        "region": region,
        "air_rate_ves_lb": _round_2(air_rate) if air_rate is not None else None,
        "sea_rate_ves_ft3": _round_2(sea_rate) if sea_rate is not None else None,
        "correspondence_rate_ves": _round_2(correspondence_rate) if correspondence_rate is not None else None,
        "handling_fee_ves": _round_2(handling_fee) if handling_fee is not None else None,
        "air_min_lb": _round_2(air_min_lb) if air_min_lb is not None else None,
        "sea_min_ft3": _round_2(sea_min_ft3) if sea_min_ft3 is not None else None,
        "public_rates_available": public_rates_available,
        "raw_text_excerpt": page_text[:2500],
        "message": (
            "Tarifas públicas OWC extraídas correctamente"
            if public_rates_available
            else "No se pudieron extraer todas las tarifas públicas OWC"
        ),
        "engine": "owc_counter_script_v1",
    }


def refresh_owc_business_rules(
    supabase_client,
    region: str = "region_central",
) -> dict[str, Any]:
    """
    Hace scraping y luego actualiza courier_business_rules.
    """
    if supabase_client is None:
        raise RuntimeError("Cliente Supabase no disponible para actualizar tarifas OWC")

    try:
        scraped = scrape_owc_public_rates(region=region)
    except Exception as exc:
        logger.exception("Fallo scraping OWC para %s. Se conservan tarifas existentes.", region)
        return {
            "message": "No se actualizaron tarifas OWC; se conservaron las tarifas existentes",
            "region": region,
            "error": str(exc),
            "scraped": {
                "courier_code": "owc",
                "url": OWC_TARIFFS_SOURCE_URL,
                "region": region,
                "public_rates_available": False,
            },
            "saved": {},
        }

    if not scraped["public_rates_available"]:
        logger.warning(
            "OWC refresh sin cambios para %s porque el scraping no produjo tarifas completas",
            region,
        )
        return {
            "message": "Proceso de actualización OWC completado sin cambios; se conservaron tarifas existentes",
            "scraped": scraped,
            "saved": {},
        }

    courier_row = (
        supabase_client
        .table("courier_business_rules")
        .select("courier_id")
        .eq("rule_code", "air_base_rate_ves")
        .limit(1)
        .execute()
    )

    if not courier_row.data:
        couriers_result = (
            supabase_client
            .table("couriers")
            .select("id, code, name")
            .eq("code", "owc")
            .single()
            .execute()
        )

        if not couriers_result.data:
            raise RuntimeError("No se encontró el courier OWC en la tabla couriers")

        courier_id = couriers_result.data["id"]
    else:
        courier_id = courier_row.data[0]["courier_id"]

    saved: dict[str, Any] = {}

    def _update_rule(
        rule_code: str,
        numeric_value: float,
        service_type_key: str | None = None,
        region_key: str | None = None,
        currency_code: str = "VES",
    ) -> dict[str, Any]:
        query = (
            supabase_client
            .table("courier_business_rules")
            .update({
                "numeric_value": numeric_value,
                "currency_code": currency_code,
            })
            .eq("courier_id", courier_id)
            .eq("rule_code", rule_code)
        )

        if service_type_key is not None:
            query = query.eq("service_type_key", service_type_key)

        if region_key is not None:
            query = query.eq("region_key", region_key)

        response = query.execute()
        return {
            "updated_count": len(response.data) if response.data else 0,
            "data": response.data or [],
        }

    saved["air_base_rate_ves"] = _update_rule(
        rule_code="air_base_rate_ves",
        numeric_value=scraped["air_rate_ves_lb"],
        service_type_key="air",
        region_key=region,
    )

    saved["sea_base_rate_ves"] = _update_rule(
        rule_code="sea_base_rate_ves",
        numeric_value=scraped["sea_rate_ves_ft3"],
        service_type_key="sea",
        region_key=region,
    )

    saved["correspondence_rate_ves"] = _update_rule(
        rule_code="correspondence_rate_ves",
        numeric_value=scraped["correspondence_rate_ves"],
        service_type_key="correspondence",
        region_key=region,
    )

    handling_response = (
        supabase_client
        .table("courier_business_rules")
        .update({
            "numeric_value": scraped["handling_fee_ves"],
            "currency_code": "VES",
        })
        .eq("courier_id", courier_id)
        .eq("rule_code", "handling_fee_ves")
        .execute()
    )
    saved["handling_fee_ves"] = {
        "updated_count": len(handling_response.data) if handling_response.data else 0,
        "data": handling_response.data or [],
    }

    if scraped["air_min_lb"] is not None:
        saved["air_min_lb"] = _update_rule(
            rule_code="air_min_lb",
            numeric_value=scraped["air_min_lb"],
            service_type_key="air",
            region_key=region,
            currency_code="UNIT",
        )

    if scraped["sea_min_ft3"] is not None:
        saved["sea_min_ft3"] = _update_rule(
            rule_code="sea_min_ft3",
            numeric_value=scraped["sea_min_ft3"],
            service_type_key="sea",
            region_key=region,
            currency_code="UNIT",
        )

    return {
        "message": "Proceso de actualización OWC completado",
        "scraped": scraped,
        "saved": saved,
    }
