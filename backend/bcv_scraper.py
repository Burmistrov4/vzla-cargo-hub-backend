import os
import re
import urllib3

from decimal import Decimal
from datetime import date
from typing import Dict, Any

import certifi
import requests
from bs4 import BeautifulSoup


BCV_URL = "https://www.bcv.org.ve/"

SPANISH_MONTHS = {
    "enero": 1,
    "febrero": 2,
    "marzo": 3,
    "abril": 4,
    "mayo": 5,
    "junio": 6,
    "julio": 7,
    "agosto": 8,
    "septiembre": 9,
    "setiembre": 9,
    "octubre": 10,
    "noviembre": 11,
    "diciembre": 12,
}


def normalize_decimal_str(value: str) -> Decimal:
    cleaned = value.strip().replace(".", "").replace(",", ".")
    return Decimal(cleaned)


def parse_bcv_date(text: str) -> date | None:
    pattern = r"Fecha\s+Valor:\s*[A-Za-zÁÉÍÓÚáéíóúñÑ]+,\s*(\d{1,2})\s+([A-Za-zÁÉÍÓÚáéíóúñÑ]+)\s+(\d{4})"
    match = re.search(pattern, text, re.IGNORECASE)

    if not match:
        return None

    day = int(match.group(1))
    month_name = match.group(2).strip().lower()
    year = int(match.group(3))

    month = SPANISH_MONTHS.get(month_name)
    if not month:
        return None

    return date(year, month, day)


def extract_bcv_data_from_html(html: str) -> Dict[str, Any]:
    soup = BeautifulSoup(html, "html.parser")
    page_text = soup.get_text(" ", strip=True)

    patterns = [
        r"\bUSD\b\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]+)",
        r"\$\s*USD\s*([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]+)",
        r"\bUSD\b[^0-9]{0,20}([0-9]{1,3}(?:\.[0-9]{3})*,[0-9]+)",
    ]

    rate_value = None
    raw_match = None

    for pattern in patterns:
        match = re.search(pattern, page_text, re.IGNORECASE)
        if match:
            raw_match = match.group(1)
            rate_value = normalize_decimal_str(raw_match)
            break

    if rate_value is None:
        raise ValueError("No se pudo extraer la tasa USD del BCV")

    rate_date = parse_bcv_date(page_text) or date.today()

    return {
        "source": "BCV",
        "currency_from": "USD",
        "currency_to": "VES",
        "rate": float(rate_value),
        "rate_date": rate_date.isoformat(),
        "raw_match": raw_match,
        "url": BCV_URL,
    }


def fetch_bcv_usd_rate() -> Dict[str, Any]:
    headers = {
        "User-Agent": "Mozilla/5.0",
        "Accept-Language": "es-ES,es;q=0.9,en;q=0.8",
    }

    force_insecure = os.getenv("BCV_INSECURE_SSL", "false").lower() == "true"

    # Intento 1: validación SSL normal con certifi
    if not force_insecure:
        try:
            response = requests.get(
                BCV_URL,
                headers=headers,
                timeout=20,
                verify=certifi.where(),
            )
            response.raise_for_status()
            data = extract_bcv_data_from_html(response.text)
            data["ssl_mode"] = "secure"
            return data
        except requests.exceptions.SSLError:
            pass

    # Intento 2: fallback local sin verificación SSL
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

    response = requests.get(
        BCV_URL,
        headers=headers,
        timeout=20,
        verify=False,
    )
    response.raise_for_status()

    data = extract_bcv_data_from_html(response.text)
    data["ssl_mode"] = "insecure_fallback"
    return data