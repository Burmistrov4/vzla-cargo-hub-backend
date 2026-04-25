import re
from decimal import Decimal, ROUND_HALF_UP
from typing import Any

from playwright.sync_api import TimeoutError as PlaywrightTimeoutError
from playwright.sync_api import sync_playwright


OWC_TARIFFS_URL = "https://onewaycargo.net/tarifas"

REGION_TAB_TEXT = {
    "region_central": "Región central",
    "resto_pais": "Resto del país",
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

    # Si tiene ambos, inferimos separador decimal por el último
    if "." in raw and "," in raw:
        if raw.rfind(",") > raw.rfind("."):
            # 1.234,56
            raw = raw.replace(".", "").replace(",", ".")
        else:
            # 1,234.56
            raw = raw.replace(",", "")
        return raw

    # Solo coma
    if "," in raw:
        parts = raw.split(",")
        if len(parts) == 2 and len(parts[1]) in (1, 2, 3, 4, 5, 6, 7, 8):
            # Puede ser decimal o millares; si la parte decimal es muy larga lo tratamos como decimal
            # Para tarifas públicas OWC normalmente 4.253 / 29.967 se ven con punto, pero por robustez:
            if len(parts[1]) > 3:
                return raw.replace(".", "").replace(",", ".")
            # 4,253 probablemente significa 4253, no 4.253
            return raw.replace(",", "")
        return raw.replace(",", "")

    # Solo punto
    if "." in raw:
        parts = raw.split(".")
        if len(parts) == 2 and len(parts[1]) > 3:
            # caso raro tipo 483.33790000
            return raw
        # 4.253 => 4253
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


def _extract_region_text_from_rendered_page(region: str) -> str:
    """
    Usa Playwright porque el HTML inicial de OWC llega con 0 Bs.
    Necesitamos el texto ya renderizado en navegador.
    """
    tab_text = REGION_TAB_TEXT.get(region, "Región central")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={"width": 1440, "height": 2200})

        try:
            page.goto(OWC_TARIFFS_URL, wait_until="networkidle", timeout=30000)

            # Esperamos que la página de tarifas cargue algo útil
            page.wait_for_timeout(1500)

            # Intentamos seleccionar la región correcta
            try:
                page.locator(f"text={tab_text}").first.click(timeout=4000)
                page.wait_for_timeout(1200)
            except Exception:
                # si no logra hacer click, seguimos con lo visible
                pass

            body_text = page.locator("body").inner_text()
            return _clean_spaces(body_text)

        except PlaywrightTimeoutError as e:
            raise RuntimeError(f"Timeout cargando OWC: {e}") from e
        finally:
            browser.close()


def scrape_owc_public_rates(region: str = "region_central") -> dict[str, Any]:
    """
    Extrae tarifas públicas visibles de OWC desde la página renderizada.
    """
    rendered_text = _extract_region_text_from_rendered_page(region)

    # Patrones principales
    air_rate = _extract_first_decimal(
        r"A[ÉE]REO\s+Desde\s+([\d\.,]+)\s*Bs\s+por libra",
        rendered_text,
    )
    sea_rate = _extract_first_decimal(
        r"MAR[ÍI]TIMO\s+Desde\s+([\d\.,]+)\s*Bs\s+por pie c[úu]bico",
        rendered_text,
    )
    correspondence_rate = _extract_first_decimal(
        r"CORRESPONDENCIA\s+Tarifa [úu]nica\s+([\d\.,]+)\s*Bs\s+por env[íi]o",
        rendered_text,
    )

    handling_matches = _extract_all_decimals(
        r"Handling Fee de\s+([\d\.,]+)\s*Bs",
        rendered_text,
    )
    handling_fee = handling_matches[0] if handling_matches else None

    # Mínimos regionales opcionales
    air_min_lb = _extract_first_decimal(
        r"A[ÉE]REO.*?M[íi]nimo requerido por env[íi]o de\s+([\d\.,]+)\s*libras",
        rendered_text,
    )
    sea_min_ft3 = _extract_first_decimal(
        r"MAR[ÍI]TIMO.*?M[íi]nimo requerido por env[íi]o de\s+([\d\.,]+)\s*pie",
        rendered_text,
    )

    public_rates_available = all(
        value is not None
        for value in [air_rate, sea_rate, correspondence_rate, handling_fee]
    )

    result = {
        "courier_code": "owc",
        "url": OWC_TARIFFS_URL,
        "region": region,
        "air_rate_ves_lb": _round_2(air_rate) if air_rate is not None else None,
        "sea_rate_ves_ft3": _round_2(sea_rate) if sea_rate is not None else None,
        "correspondence_rate_ves": _round_2(correspondence_rate) if correspondence_rate is not None else None,
        "handling_fee_ves": _round_2(handling_fee) if handling_fee is not None else None,
        "air_min_lb": _round_2(air_min_lb) if air_min_lb is not None else None,
        "sea_min_ft3": _round_2(sea_min_ft3) if sea_min_ft3 is not None else None,
        "public_rates_available": public_rates_available,
        "raw_text_excerpt": rendered_text[:2500],
        "message": (
            "Tarifas públicas OWC extraídas correctamente"
            if public_rates_available
            else "No se pudieron extraer todas las tarifas públicas OWC"
        ),
        "engine": "playwright_rendered_page",
    }

    return result


def refresh_owc_business_rules(
    supabase_client,
    region: str = "region_central",
) -> dict[str, Any]:
    """
    Hace scraping y luego actualiza courier_business_rules.
    """
    scraped = scrape_owc_public_rates(region=region)

    if not scraped["public_rates_available"]:
        return {
            "message": "Proceso de actualización OWC completado sin cambios",
            "scraped": scraped,
            "saved": {},
        }

    courier_row = (
        supabase_client
        .table("couriers")
        .select("id, code, name")
        .eq("code", "owc")
        .single()
        .execute()
    )

    if not courier_row.data:
        raise RuntimeError("No se encontró el courier OWC en la tabla couriers")

    courier_id = courier_row.data["id"]

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

    # handling fee normalmente es transversal
    saved["handling_fee_ves"] = (
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
        "updated_count": len(saved["handling_fee_ves"].data) if saved["handling_fee_ves"].data else 0,
        "data": saved["handling_fee_ves"].data or [],
    }

    # mínimos regionales si existen en la tabla
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