from datetime import datetime
import logging
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


def get_latest_exchange_rate():
    result = (
        supabase_public
        .table("exchange_rates")
        .select("*")
        .order("rate_date", desc=True)
        .limit(1)
        .execute()
    )

    if not result.data:
        raise HTTPException(status_code=404, detail="No hay tasa de cambio registrada")

    return result.data[0]


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
def get_owc_rules(region: str = "region_central"):
    try:
        courier = get_courier_by_code("owc")
        rules = build_owc_rules(courier["id"], region)

        return {
            "courier": courier["name"],
            "courier_code": courier["code"],
            "region": region,
            "rules": rules,
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/couriers/owc/preview-rates")
def preview_owc_rates(region: str = "region_central"):
    try:
        return scrape_owc_public_rates(region=region)
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/couriers/owc/refresh-rates")
def refresh_owc_rates(region: str = "region_central"):
    try:
        return refresh_owc_business_rules(db_client, region=region)
    except Exception as e:
        logger.exception("Error refrescando tarifas OWC region=%s", region)
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/exchange-rate/latest")
def exchange_rate_latest():
    try:
        exchange = get_latest_exchange_rate()
        return {
            "source": exchange["source"],
            "currency_from": exchange["currency_from"],
            "currency_to": exchange["currency_to"],
            "rate": float(exchange["rate"]),
            "rate_date": exchange["rate_date"],
            "fetched_at": exchange["fetched_at"],
        }
    except HTTPException:
        raise
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/exchange-rate/refresh-bcv")
def refresh_bcv_exchange_rate():
    try:
        scraped = fetch_bcv_usd_rate()

        payload = {
            "source": scraped["source"],
            "currency_from": scraped["currency_from"],
            "currency_to": scraped["currency_to"],
            "rate": scraped["rate"],
            "rate_date": scraped["rate_date"],
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

        saved_row = result.data[0] if result.data else payload

        return {
            "message": "Tasa BCV actualizada correctamente",
            "scraped": scraped,
            "saved": saved_row,
        }

    except Exception as e:
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
