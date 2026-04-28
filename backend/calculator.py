from decimal import Decimal, ROUND_HALF_UP, ROUND_CEILING

ZERO = Decimal("0")
ONE = Decimal("1")
LB_PER_KG = Decimal("2.2046226218")
IN3_PER_FT3 = Decimal("1728")


def d(value) -> Decimal:
    if isinstance(value, Decimal):
        return value
    if value is None:
        return ZERO
    return Decimal(str(value))


def round_usd(value: Decimal | int | float) -> float:
    return float(d(value).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))


def round_ves(value: Decimal | int | float) -> int:
    return int(d(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def round_int_half_up(value: Decimal | int | float) -> int:
    return int(d(value).quantize(Decimal("1"), rounding=ROUND_HALF_UP))


def round_up_to_step(value: Decimal, step: Decimal) -> Decimal:
    if value <= 0 or step <= 0:
        return ZERO
    steps = (value / step).to_integral_value(rounding=ROUND_CEILING)
    return steps * step


def derive_weight_lb(
    total_weight_lb: Decimal = ZERO,
    total_weight_kg: Decimal = ZERO,
    items: list | None = None,
) -> Decimal:
    if total_weight_lb > 0:
        return total_weight_lb

    if total_weight_kg > 0:
        return total_weight_kg * LB_PER_KG

    items = items or []
    total_items_weight_lb = ZERO
    total_items_weight_kg = ZERO

    for item in items:
        if isinstance(item, dict):
            qty = d(item.get("quantity", 1))
            weight_lb = d(item.get("weight_lb", 0))
            weight_kg = d(item.get("weight_kg", 0))
        else:
            qty = d(getattr(item, "quantity", 1))
            weight_lb = d(getattr(item, "weight_lb", 0))
            weight_kg = d(getattr(item, "weight_kg", 0))

        total_items_weight_lb += weight_lb * qty
        total_items_weight_kg += weight_kg * qty

    if total_items_weight_lb > 0:
        return total_items_weight_lb

    if total_items_weight_kg > 0:
        return total_items_weight_kg * LB_PER_KG

    return ZERO


def derive_package_dimensions(
    length_in: Decimal = ZERO,
    width_in: Decimal = ZERO,
    height_in: Decimal = ZERO,
    items: list | None = None,
) -> tuple[Decimal, Decimal, Decimal]:
    if length_in > 0 and width_in > 0 and height_in > 0:
        return length_in, width_in, height_in

    items = items or []
    if len(items) == 1:
        item = items[0]
        if isinstance(item, dict):
            item_length = d(item.get("length_in", 0))
            item_width = d(item.get("width_in", 0))
            item_height = d(item.get("height_in", 0))
        else:
            item_length = d(getattr(item, "length_in", 0))
            item_width = d(getattr(item, "width_in", 0))
            item_height = d(getattr(item, "height_in", 0))

        if item_length > 0 and item_width > 0 and item_height > 0:
            return item_length, item_width, item_height

    return length_in, width_in, height_in


def derive_volume_ft3(
    total_volume_ft3: Decimal = ZERO,
    length_in: Decimal = ZERO,
    width_in: Decimal = ZERO,
    height_in: Decimal = ZERO,
    items: list | None = None,
) -> Decimal:
    if total_volume_ft3 > 0:
        return total_volume_ft3

    if length_in > 0 and width_in > 0 and height_in > 0:
        return (length_in * width_in * height_in) / IN3_PER_FT3

    items = items or []
    total_items_volume_ft3 = ZERO

    for item in items:
        if isinstance(item, dict):
            qty = d(item.get("quantity", 1))
            volume_ft3 = d(item.get("volume_ft3", 0))
            item_length = d(item.get("length_in", 0))
            item_width = d(item.get("width_in", 0))
            item_height = d(item.get("height_in", 0))
        else:
            qty = d(getattr(item, "quantity", 1))
            volume_ft3 = d(getattr(item, "volume_ft3", 0))
            item_length = d(getattr(item, "length_in", 0))
            item_width = d(getattr(item, "width_in", 0))
            item_height = d(getattr(item, "height_in", 0))

        if volume_ft3 > 0:
            total_items_volume_ft3 += volume_ft3 * qty
        elif item_length > 0 and item_width > 0 and item_height > 0:
            total_items_volume_ft3 += ((item_length * item_width * item_height) / IN3_PER_FT3) * qty

    return total_items_volume_ft3


def calculate_quote(
    rate_row: dict,
    exchange_rate: float,
    declared_value_usd: float = 0,
    total_weight_kg: float = 0,
    total_weight_lb: float = 0,
    total_volume_ft3: float = 0,
    total_same_item_qty: int = 1,
):
    """
    Motor genérico basado en courier_rates.
    Se mantiene por compatibilidad con couriers simples.
    """

    bcv = d(exchange_rate)
    service_type = rate_row.get("service_type", "air")
    charge_unit = rate_row.get("charge_unit", "lb")
    currency = (rate_row.get("currency") or "USD").upper()

    rate_value = d(rate_row.get("rate", 0))
    minimum_charge_units = d(rate_row.get("minimum_charge_units", 1))

    handling_fee = d(rate_row.get("handling_fee", 0))
    handling_fee_currency = (rate_row.get("handling_fee_currency") or "VES").upper()

    packaging_fee = d(rate_row.get("packaging_fee", 0))
    packaging_fee_currency = (rate_row.get("packaging_fee_currency") or "VES").upper()

    insurance_percent = d(rate_row.get("insurance_percent", 0))
    declared_value_usd_dec = d(declared_value_usd)

    total_weight_lb_dec = d(total_weight_lb)
    total_weight_kg_dec = d(total_weight_kg)
    total_volume_ft3_dec = d(total_volume_ft3)

    if charge_unit == "kg":
        exact_units = (
            total_weight_kg_dec
            if total_weight_kg_dec > 0
            else (total_weight_lb_dec / LB_PER_KG if total_weight_lb_dec > 0 else ZERO)
        )
    elif charge_unit == "lb":
        exact_units = (
            total_weight_lb_dec
            if total_weight_lb_dec > 0
            else (total_weight_kg_dec * LB_PER_KG if total_weight_kg_dec > 0 else ZERO)
        )
    elif charge_unit == "ft3":
        exact_units = total_volume_ft3_dec
    else:
        exact_units = ONE

    uses_minimum_charge = False
    billable_units = exact_units

    if billable_units <= 0:
        billable_units = minimum_charge_units
        uses_minimum_charge = True
    elif minimum_charge_units > 0 and billable_units < minimum_charge_units:
        billable_units = minimum_charge_units
        uses_minimum_charge = True

    freight_amount = billable_units * rate_value

    if currency == "USD":
        freight_usd = freight_amount
        freight_ves = freight_usd * bcv
    else:
        freight_ves = freight_amount
        freight_usd = (freight_ves / bcv) if bcv > 0 else ZERO

    if handling_fee_currency == "USD":
        handling_usd = handling_fee
        handling_ves = handling_fee * bcv
    else:
        handling_ves = handling_fee
        handling_usd = (handling_ves / bcv) if bcv > 0 else ZERO

    if packaging_fee_currency == "USD":
        packaging_usd = packaging_fee
        packaging_ves = packaging_fee * bcv
    else:
        packaging_ves = packaging_fee
        packaging_usd = (packaging_ves / bcv) if bcv > 0 else ZERO

    insurance_usd = declared_value_usd_dec * insurance_percent if declared_value_usd_dec > 0 else ZERO
    insurance_ves = insurance_usd * bcv

    customs_tax_usd = ZERO
    customs_tax_ves = ZERO
    repack_usd = ZERO
    repack_ves = ZERO
    storage_usd = ZERO
    storage_ves = ZERO
    purchase_service_usd = ZERO
    purchase_service_ves = ZERO
    compactation_fee_usd = ZERO
    compactation_fee_ves = ZERO

    total_usd = (
        freight_usd
        + handling_usd
        + packaging_usd
        + insurance_usd
        + customs_tax_usd
        + repack_usd
        + storage_usd
        + purchase_service_usd
        + compactation_fee_usd
    )

    total_ves = total_usd * bcv if bcv > 0 else ZERO

    return {
        "engine": "generic_rate_row_v1",
        "service_type": service_type,
        "charge_unit": charge_unit,
        "chargeable_units_exact": float(exact_units),
        "chargeable_units_display": round_int_half_up(billable_units),
        "uses_minimum_charge": uses_minimum_charge,
        "exchange_rate_used": float(bcv),
        "raw_metrics": {
            "real_weight_lb": round_usd(
                total_weight_lb_dec if total_weight_lb_dec > 0 else total_weight_kg_dec * LB_PER_KG
            ),
            "volumetric_weight_lb": 0,
            "raw_volume_ft3": round_usd(total_volume_ft3_dec),
            "display_volume_ft3": round_int_half_up(total_volume_ft3_dec),
            "storage_chargeable_ft3": 0,
            "length_in_used": 0,
            "width_in_used": 0,
            "height_in_used": 0,
        },
        "flags": {
            "enable_handling_fee": handling_fee > 0,
            "enable_repack_fee": False,
            "compactation_requested": False,
            "hold_mode": "none",
            "hold_days": 0,
            "storage_days_charged": 0,
            "use_insurance": insurance_percent > 0 and declared_value_usd_dec > 0,
            "use_purchase_by_order": False,
            "apply_provisional_customs": False,
            "repack_applies": False,
            "repack_storage_exempt": False,
            "generic_total_same_item_qty": int(total_same_item_qty),
        },
        "public_calculator_reference": {},
        "breakdown": {
            "freight_usd": round_usd(freight_usd),
            "freight_ves": round_ves(freight_ves),
            "insurance_usd": round_usd(insurance_usd),
            "insurance_ves": round_ves(insurance_ves),
            "customs_tax_usd": round_usd(customs_tax_usd),
            "customs_tax_ves": round_ves(customs_tax_ves),
            "handling_usd": round_usd(handling_usd),
            "handling_ves": round_ves(handling_ves),
            "packaging_usd": round_usd(packaging_usd),
            "packaging_ves": round_ves(packaging_ves),
            "repack_usd": round_usd(repack_usd),
            "repack_ves": round_ves(repack_ves),
            "storage_usd": round_usd(storage_usd),
            "storage_ves": round_ves(storage_ves),
            "purchase_service_usd": round_usd(purchase_service_usd),
            "purchase_service_ves": round_ves(purchase_service_ves),
            "compactation_fee_usd": round_usd(compactation_fee_usd),
            "compactation_fee_ves": round_ves(compactation_fee_ves),
        },
        "total_usd": round_usd(total_usd),
        "total_ves": round_ves(total_ves),
    }


def calculate_zoom_quote(
    rules: dict,
    exchange_rate: float,
    payload: dict,
):
    """
    Motor observado para Zoom Casillero Internacional.

    Alcance v1:
    - Casillero Internacional;
    - Estados Unidos -> Venezuela;
    - Aereo;
    - Mercancia;
    - Entrega en oficina.
    """

    bcv = d(exchange_rate)
    items = payload.get("items") or []

    service_type = payload["service_type"]
    declared_value_usd = d(payload.get("declared_value_usd", 0))
    delivery_type = payload.get("delivery_type", "office")
    zoom_service = payload.get("zoom_service", "international_locker")
    origin_country = payload.get("origin_country", "US")
    destination_country = payload.get("destination_country", "VE")
    shipment_kind = payload.get("shipment_kind", "merchandise")
    consolidated = bool(payload.get("consolidated", False))
    consolidated_package_count = int(payload.get("consolidated_package_count", 1) or 1)
    use_protection = bool(payload.get("use_protection", True))

    total_weight_lb_input = d(payload.get("total_weight_lb", 0))
    total_weight_kg_input = d(payload.get("total_weight_kg", 0))
    total_volume_ft3_input = d(payload.get("total_volume_ft3", 0))

    length_in_input = d(payload.get("length_in", 0))
    width_in_input = d(payload.get("width_in", 0))
    height_in_input = d(payload.get("height_in", 0))

    if zoom_service != "international_locker":
        raise ValueError("Zoom solo soporta Casillero Internacional en esta version")
    if origin_country != "US" or destination_country != "VE":
        raise ValueError("Zoom Casillero v1 solo soporta Estados Unidos -> Venezuela")
    if service_type != "air":
        raise ValueError("Zoom Casillero v1 solo soporta servicio aereo")
    if delivery_type != "office":
        raise ValueError("Zoom Casillero v1 solo soporta entrega en oficina")
    if shipment_kind != "merchandise":
        raise ValueError("Zoom Casillero v1 solo soporta mercancia")
    if consolidated and consolidated_package_count < 2:
        raise ValueError("Si consolidated=true, consolidated_package_count debe ser al menos 2")
    if consolidated and consolidated_package_count != 2:
        raise ValueError("Zoom Casillero v1 solo tiene validado consolidado con 2 encomiendas")

    air_rate_usd_per_kg = d(rules.get("air_rate_usd_per_kg", "32.40"))
    protection_percent = d(rules.get("protection_percent", "0.01"))
    protection_min_usd = d(rules.get("protection_min_usd", "1.20"))
    consolidation_fee_usd = d(rules.get("consolidation_fee_usd", "6.00"))
    billable_weight_step_kg = d(rules.get("billable_weight_step_kg", "0.5"))

    total_weight_lb = derive_weight_lb(
        total_weight_lb=total_weight_lb_input,
        total_weight_kg=total_weight_kg_input,
        items=items,
    )

    total_weight_kg = (
        total_weight_kg_input
        if total_weight_kg_input > 0
        else (total_weight_lb / LB_PER_KG if total_weight_lb > 0 else ZERO)
    )

    if total_weight_kg <= 0:
        raise ValueError("Zoom requiere peso fisico mayor a 0 kg")

    length_in, width_in, height_in = derive_package_dimensions(
        length_in=length_in_input,
        width_in=width_in_input,
        height_in=height_in_input,
        items=items,
    )

    raw_volume_ft3 = derive_volume_ft3(
        total_volume_ft3=total_volume_ft3_input,
        length_in=length_in,
        width_in=width_in,
        height_in=height_in,
        items=items,
    )

    raw_volumetric_lb = ZERO
    billable_weight_kg = round_up_to_step(total_weight_kg, billable_weight_step_kg)
    chargeable_exact = billable_weight_kg * LB_PER_KG
    chargeable_display = round_usd(billable_weight_kg)
    storage_chargeable_ft3 = ZERO

    freight_usd = billable_weight_kg * air_rate_usd_per_kg
    freight_ves = freight_usd * bcv if bcv > 0 else ZERO

    protection_usd = ZERO
    if use_protection and declared_value_usd > 0:
        calculated_protection = declared_value_usd * protection_percent
        protection_usd = (
            protection_min_usd
            if calculated_protection < protection_min_usd
            else calculated_protection
        )
    protection_ves = protection_usd * bcv

    consolidation_usd = consolidation_fee_usd if consolidated else ZERO
    consolidation_ves = consolidation_usd * bcv

    total_usd = freight_usd + protection_usd + consolidation_usd
    total_ves = total_usd * bcv if bcv > 0 else ZERO

    return {
        "engine": "zoom_locker_air_office_v1",
        "observed_formula_version": "zoom_locker_air_office_v1",
        "service_type": service_type,
        "charge_unit": "kg",
        "chargeable_units_exact": float(chargeable_exact),
        "chargeable_units_display": chargeable_display,
        "uses_minimum_charge": False,
        "exchange_rate_used": float(bcv),
        "raw_metrics": {
            "real_weight_lb": round_usd(total_weight_lb),
            "volumetric_weight_lb": round_usd(raw_volumetric_lb),
            "raw_volume_ft3": round_usd(raw_volume_ft3),
            "display_volume_ft3": round_usd(raw_volume_ft3),
            "storage_chargeable_ft3": round_usd(storage_chargeable_ft3),
            "length_in_used": round_usd(length_in),
            "width_in_used": round_usd(width_in),
            "height_in_used": round_usd(height_in),
            "physical_weight_kg": round_usd(total_weight_kg),
            "billable_weight_kg": round_usd(billable_weight_kg),
        },
        "flags": {
            "zoom_mode": True,
            "zoom_service": zoom_service,
            "origin_country": origin_country,
            "destination_country": destination_country,
            "shipment_kind": shipment_kind,
            "delivery_type": delivery_type,
            "consolidated": consolidated,
            "consolidated_package_count": consolidated_package_count,
            "use_protection": use_protection,
            "zoom_billing_basis": "kg_rounded_up_to_0.5",
            "air_basis": "zoom_observed_kg_rate",
            "rate_source": "zoom_observed_cases",
        },
        "public_calculator_reference": {
            "air_rate_usd_per_kg": round_usd(air_rate_usd_per_kg),
            "billable_weight_step_kg": round_usd(billable_weight_step_kg),
            "protection_percent": round_usd(protection_percent),
            "protection_min_usd": round_usd(protection_min_usd),
            "consolidation_fee_usd": round_usd(consolidation_fee_usd),
            "exchange_rate": float(bcv),
        },
        "breakdown": {
            "freight_usd": round_usd(freight_usd),
            "freight_ves": round_usd(freight_ves),
            "protection_usd": round_usd(protection_usd),
            "protection_ves": round_usd(protection_ves),
            "consolidation_usd": round_usd(consolidation_usd),
            "consolidation_ves": round_usd(consolidation_ves),
            "exchange_rate": float(bcv),
            "insurance_usd": round_usd(protection_usd),
            "insurance_ves": round_usd(protection_ves),
            "customs_tax_usd": 0,
            "customs_tax_ves": 0,
            "handling_usd": 0,
            "handling_ves": 0,
            "packaging_usd": 0,
            "packaging_ves": 0,
            "repack_usd": 0,
            "repack_ves": 0,
            "storage_usd": 0,
            "storage_ves": 0,
            "purchase_service_usd": 0,
            "purchase_service_ves": 0,
            "compactation_fee_usd": 0,
            "compactation_fee_ves": 0,
        },
        "total_usd": round_usd(total_usd),
        "total_ves": round_usd(total_ves),
    }


def calculate_owc_quote(
    rules: dict,
    exchange_rate: float,
    payload: dict,
):
    bcv = d(exchange_rate)
    items = payload.get("items") or []

    service_type = payload["service_type"]
    declared_value_usd = d(payload.get("declared_value_usd", 0))

    total_weight_lb_input = d(payload.get("total_weight_lb", 0))
    total_weight_kg_input = d(payload.get("total_weight_kg", 0))
    total_volume_ft3_input = d(payload.get("total_volume_ft3", 0))

    length_in_input = d(payload.get("length_in", 0))
    width_in_input = d(payload.get("width_in", 0))
    height_in_input = d(payload.get("height_in", 0))

    total_same_item_qty = int(payload.get("total_same_item_qty", 1) or 1)
    tracking_count = int(payload.get("tracking_count", 1) or 1)

    enable_handling_fee = bool(payload.get("enable_handling_fee", True))
    enable_repack_fee = bool(payload.get("enable_repack_fee", False))
    compactation_requested = bool(payload.get("compactation_requested", False))

    hold_mode = payload.get("hold_mode", "none")
    hold_days = int(payload.get("hold_days", 0) or 0)

    use_insurance = bool(payload.get("use_insurance", False))
    use_purchase_by_order = bool(payload.get("use_purchase_by_order", False))
    apply_provisional_customs = bool(payload.get("apply_provisional_customs", False))

    storage_fee_ves_per_day_ft3_override = payload.get("storage_fee_ves_per_day_ft3_override")
    storage_fee_ves_per_day_ft3 = (
        d(storage_fee_ves_per_day_ft3_override)
        if storage_fee_ves_per_day_ft3_override is not None
        else d(rules.get("storage_fee_ves_per_day_ft3", 0))
    )

    air_base_rate_ves = d(rules.get("air_base_rate_ves", 0))
    sea_base_rate_ves = d(rules.get("sea_base_rate_ves", 0))
    correspondence_rate_ves = d(rules.get("correspondence_rate_ves", 0))

    volumetric_divisor_in3_per_lb = d(rules.get("volumetric_divisor_in3_per_lb", 166))
    air_min_lb = d(rules.get("air_min_lb", 1))
    sea_min_ft3 = d(rules.get("sea_min_ft3", 1))

    handling_fee_ves_rule = d(rules.get("handling_fee_ves", 0))

    repack_fee_amount = d(rules.get("repack_fee_amount", 0))
    repack_fee_currency = (rules.get("repack_fee_currency") or "USD").upper()
    repack_min_air_lb = d(rules.get("repack_min_air_lb", 5))
    repack_min_sea_ft3 = d(rules.get("repack_min_sea_ft3", 3))
    repack_storage_exempt = bool(rules.get("repack_storage_exempt", True))

    insurance_percent = d(rules.get("insurance_percent", 0.05))
    general_hold_free_business_days = int(rules.get("general_hold_free_business_days", 3))

    purchase_by_order_threshold_usd = d(rules.get("purchase_by_order_threshold_usd", 100))
    purchase_by_order_lt_threshold_percent = d(rules.get("purchase_by_order_lt_threshold_percent", 0.20))
    purchase_by_order_gte_threshold_percent = d(rules.get("purchase_by_order_gte_threshold_percent", 0.15))

    provisional_customs_percent = d(rules.get("provisional_customs_percent", 0.16))
    provisional_customs_qty_threshold = int(rules.get("provisional_customs_qty_threshold", 4))
    provisional_customs_value_threshold_usd = d(rules.get("provisional_customs_value_threshold_usd", 200))

    storage_charge_min_ft3 = d(rules.get("storage_charge_min_ft3", 1))

    total_weight_lb = derive_weight_lb(
        total_weight_lb=total_weight_lb_input,
        total_weight_kg=total_weight_kg_input,
        items=items,
    )

    length_in, width_in, height_in = derive_package_dimensions(
        length_in=length_in_input,
        width_in=width_in_input,
        height_in=height_in_input,
        items=items,
    )

    raw_volume_ft3 = derive_volume_ft3(
        total_volume_ft3=total_volume_ft3_input,
        length_in=length_in,
        width_in=width_in,
        height_in=height_in,
        items=items,
    )

    raw_volumetric_lb = ZERO
    if length_in > 0 and width_in > 0 and height_in > 0 and volumetric_divisor_in3_per_lb > 0:
        raw_volumetric_lb = (length_in * width_in * height_in) / volumetric_divisor_in3_per_lb

    display_volume_ft3 = round_int_half_up(raw_volume_ft3)

    charge_unit = "lb"
    chargeable_exact = ZERO
    chargeable_display = 0
    freight_ves = ZERO
    uses_minimum_charge = False
    air_basis = "n/a"

    if service_type == "air":
        dominant_exact = total_weight_lb
        dominant_mode = "real_exact"

        if raw_volumetric_lb > total_weight_lb:
            dominant_exact = raw_volumetric_lb
            dominant_mode = "volumetric_display"

        if dominant_exact < air_min_lb:
            uses_minimum_charge = True
            air_basis = "minimum_charge"
            charge_unit = "lb"
            chargeable_exact = dominant_exact
            chargeable_display = round_int_half_up(air_min_lb)
            freight_ves = air_base_rate_ves * air_min_lb
        elif dominant_mode == "volumetric_display":
            air_basis = "volumetric_display"
            charge_unit = "lb"
            chargeable_exact = dominant_exact
            chargeable_display = round_int_half_up(dominant_exact)
            freight_ves = air_base_rate_ves * d(chargeable_display)
        else:
            air_basis = "real_exact"
            charge_unit = "lb"
            chargeable_exact = dominant_exact
            chargeable_display = round_int_half_up(dominant_exact)
            freight_ves = air_base_rate_ves * dominant_exact

    elif service_type == "sea":
        charge_unit = "ft3"
        chargeable_exact = raw_volume_ft3
        chargeable_display = display_volume_ft3

        billable_ft3 = raw_volume_ft3
        if billable_ft3 <= 0 or billable_ft3 < sea_min_ft3:
            billable_ft3 = sea_min_ft3
            uses_minimum_charge = True

        freight_ves = sea_base_rate_ves * billable_ft3

    elif service_type == "correspondence":
        charge_unit = "unit"
        chargeable_exact = ONE
        chargeable_display = 1
        freight_ves = correspondence_rate_ves
    else:
        raise ValueError("service_type inválido para OWC")

    freight_usd = (freight_ves / bcv) if bcv > 0 else ZERO

    handling_ves = ZERO
    if enable_handling_fee and handling_fee_ves_rule > 0:
        handling_ves = handling_fee_ves_rule * d(tracking_count)
    handling_usd = (handling_ves / bcv) if bcv > 0 else ZERO

    repack_applies = False
    if enable_repack_fee:
        if service_type == "air":
            repack_applies = chargeable_exact >= repack_min_air_lb
        elif service_type == "sea":
            repack_applies = max(raw_volume_ft3, sea_min_ft3) >= repack_min_sea_ft3

    repack_usd = ZERO
    repack_ves = ZERO
    if repack_applies and repack_fee_amount > 0:
        if repack_fee_currency == "USD":
            repack_usd = repack_fee_amount
            repack_ves = repack_usd * bcv
        else:
            repack_ves = repack_fee_amount
            repack_usd = (repack_ves / bcv) if bcv > 0 else ZERO

    insurance_usd = ZERO
    if use_insurance and declared_value_usd > 0:
        insurance_usd = declared_value_usd * insurance_percent
    insurance_ves = insurance_usd * bcv

    customs_tax_usd = ZERO
    if apply_provisional_customs:
        customs_applies = (
            declared_value_usd > provisional_customs_value_threshold_usd
            or total_same_item_qty >= provisional_customs_qty_threshold
        )
        if customs_applies:
            customs_tax_usd = declared_value_usd * provisional_customs_percent
    customs_tax_ves = customs_tax_usd * bcv

    purchase_service_usd = ZERO
    if use_purchase_by_order and declared_value_usd > 0:
        if declared_value_usd < purchase_by_order_threshold_usd:
            purchase_service_usd = declared_value_usd * purchase_by_order_lt_threshold_percent
        else:
            purchase_service_usd = declared_value_usd * purchase_by_order_gte_threshold_percent
    purchase_service_ves = purchase_service_usd * bcv

    packaging_usd = ZERO
    packaging_ves = ZERO

    storage_days_charged = 0
    if hold_mode in {"general", "repack"} and hold_days > general_hold_free_business_days:
        storage_days_charged = hold_days - general_hold_free_business_days

    storage_chargeable_ft3 = ZERO
    if storage_days_charged > 0:
        if hold_mode == "repack" and repack_applies and repack_storage_exempt:
            storage_days_charged = 0
        else:
            storage_chargeable_ft3 = (
                raw_volume_ft3
                if raw_volume_ft3 >= storage_charge_min_ft3
                else storage_charge_min_ft3
            )

    storage_ves = storage_chargeable_ft3 * storage_fee_ves_per_day_ft3 * d(storage_days_charged)
    storage_usd = (storage_ves / bcv) if bcv > 0 else ZERO

    compactation_fee_usd = ZERO
    compactation_fee_ves = ZERO
    if compactation_requested:
        # Placeholder: por ahora se mantiene en cero hasta definir regla comercial.
        compactation_fee_usd = ZERO
        compactation_fee_ves = ZERO

    total_usd_exact = (
        freight_usd
        + insurance_usd
        + customs_tax_usd
        + handling_usd
        + packaging_usd
        + repack_usd
        + storage_usd
        + purchase_service_usd
        + compactation_fee_usd
    )

    total_usd = round_usd(total_usd_exact)
    total_ves = round_ves(d(total_usd) * bcv) if bcv > 0 else 0

    sea_freight_reference_ves = sea_base_rate_ves * (
        raw_volume_ft3 if raw_volume_ft3 >= sea_min_ft3 else sea_min_ft3
    )

    air_freight_reference_ves = ZERO
    if total_weight_lb < air_min_lb and raw_volumetric_lb < air_min_lb:
        air_freight_reference_ves = air_base_rate_ves * air_min_lb
    elif raw_volumetric_lb > total_weight_lb:
        air_freight_reference_ves = air_base_rate_ves * d(round_int_half_up(raw_volumetric_lb))
    else:
        air_freight_reference_ves = air_base_rate_ves * total_weight_lb

    return {
        "engine": "owc_rules_v3",
        "service_type": service_type,
        "charge_unit": charge_unit,
        "chargeable_units_exact": float(chargeable_exact),
        "chargeable_units_display": chargeable_display,
        "uses_minimum_charge": uses_minimum_charge,
        "exchange_rate_used": float(bcv),
        "raw_metrics": {
            "real_weight_lb": round_usd(total_weight_lb),
            "volumetric_weight_lb": round_usd(raw_volumetric_lb),
            "raw_volume_ft3": round_usd(raw_volume_ft3),
            "display_volume_ft3": display_volume_ft3,
            "storage_chargeable_ft3": round_usd(storage_chargeable_ft3),
            "length_in_used": round_usd(length_in),
            "width_in_used": round_usd(width_in),
            "height_in_used": round_usd(height_in),
        },
        "flags": {
            "enable_handling_fee": enable_handling_fee,
            "enable_repack_fee": enable_repack_fee,
            "compactation_requested": compactation_requested,
            "hold_mode": hold_mode,
            "hold_days": hold_days,
            "storage_days_charged": storage_days_charged,
            "use_insurance": use_insurance,
            "use_purchase_by_order": use_purchase_by_order,
            "apply_provisional_customs": apply_provisional_customs,
            "repack_applies": repack_applies,
            "repack_storage_exempt": repack_storage_exempt,
            "air_basis": air_basis,
        },
        "public_calculator_reference": {
            "air_final_weight_display_lb": round_int_half_up(
                raw_volumetric_lb if raw_volumetric_lb > total_weight_lb else total_weight_lb
            ),
            "sea_volume_display_ft3": display_volume_ft3,
            "air_freight_ves_without_fees": round_ves(air_freight_reference_ves),
            "sea_freight_ves_without_fees": round_ves(sea_freight_reference_ves),
            "correspondence_rate_ves": round_ves(correspondence_rate_ves),
        },
        "breakdown": {
            "freight_usd": round_usd(freight_usd),
            "freight_ves": round_ves(freight_ves),
            "insurance_usd": round_usd(insurance_usd),
            "insurance_ves": round_ves(insurance_ves),
            "customs_tax_usd": round_usd(customs_tax_usd),
            "customs_tax_ves": round_ves(customs_tax_ves),
            "handling_usd": round_usd(handling_usd),
            "handling_ves": round_ves(handling_ves),
            "packaging_usd": round_usd(packaging_usd),
            "packaging_ves": round_ves(packaging_ves),
            "repack_usd": round_usd(repack_usd),
            "repack_ves": round_ves(repack_ves),
            "storage_usd": round_usd(storage_usd),
            "storage_ves": round_ves(storage_ves),
            "purchase_service_usd": round_usd(purchase_service_usd),
            "purchase_service_ves": round_ves(purchase_service_ves),
            "compactation_fee_usd": round_usd(compactation_fee_usd),
            "compactation_fee_ves": round_ves(compactation_fee_ves),
        },
        "total_usd": total_usd,
        "total_ves": total_ves,
    }


def calculate_zoom_quote_legacy(
    rules: dict,
    exchange_rate: float,
    payload: dict,
):
    """
    Primera versión conservadora de Zoom.

    Objetivos:
    - no romper el frontend;
    - devolver la misma estructura que OWC;
    - modelar primero AÉREO con base en reglas públicas observadas;
    - dejar MARÍTIMO funcional en forma simple para seguir iterando.

    Notas:
    - Aéreo: tarifa publicada por cada 0.5 kg.
    - Marítimo: tarifa publicada por ft3.
    - Seguro Zoom se modela como:
        * Aéreo: mínimo $1 si valor < 100, si no 1%.
        * Marítimo: 5%, mínimo $5.
    - Consolidación todavía NO se está cobrando aquí porque tu payload actual
      no trae un campo explícito tipo zoom_consolidate / consolidated_packages.
    """

    bcv = d(exchange_rate)
    items = payload.get("items") or []

    service_type = payload["service_type"]
    declared_value_usd = d(payload.get("declared_value_usd", 0))

    total_weight_lb_input = d(payload.get("total_weight_lb", 0))
    total_weight_kg_input = d(payload.get("total_weight_kg", 0))
    total_volume_ft3_input = d(payload.get("total_volume_ft3", 0))

    length_in_input = d(payload.get("length_in", 0))
    width_in_input = d(payload.get("width_in", 0))
    height_in_input = d(payload.get("height_in", 0))

    tracking_count = int(payload.get("tracking_count", 1) or 1)
    use_insurance = bool(payload.get("use_insurance", False))
    enable_handling_fee = bool(payload.get("enable_handling_fee", False))
    enable_repack_fee = bool(payload.get("enable_repack_fee", False))
    hold_mode = payload.get("hold_mode", "none")
    hold_days = int(payload.get("hold_days", 0) or 0)

    air_rate_usd_per_half_kg = d(rules.get("air_rate_usd_per_half_kg", 6))
    sea_rate_usd_per_ft3 = d(rules.get("sea_rate_usd_per_ft3", 29))
    handling_fee_usd = d(rules.get("handling_fee_usd", 0))
    packaging_fee_usd = d(rules.get("packaging_fee_usd", 0))

    insurance_air_min_usd = d(rules.get("insurance_air_min_usd", 1))
    insurance_air_percent = d(rules.get("insurance_air_percent", "0.01"))

    insurance_sea_min_usd = d(rules.get("insurance_sea_min_usd", 5))
    insurance_sea_percent = d(rules.get("insurance_sea_percent", "0.05"))

    total_weight_lb = derive_weight_lb(
        total_weight_lb=total_weight_lb_input,
        total_weight_kg=total_weight_kg_input,
        items=items,
    )

    total_weight_kg = (
        total_weight_kg_input
        if total_weight_kg_input > 0
        else (total_weight_lb / LB_PER_KG if total_weight_lb > 0 else ZERO)
    )

    length_in, width_in, height_in = derive_package_dimensions(
        length_in=length_in_input,
        width_in=width_in_input,
        height_in=height_in_input,
        items=items,
    )

    raw_volume_ft3 = derive_volume_ft3(
        total_volume_ft3=total_volume_ft3_input,
        length_in=length_in,
        width_in=width_in,
        height_in=height_in,
        items=items,
    )

    raw_volumetric_lb = ZERO

    charge_unit = "lb"
    chargeable_exact = ZERO
    chargeable_display = 0
    freight_usd = ZERO
    uses_minimum_charge = False

    zoom_billable_weight_kg = ZERO
    zoom_billable_volume_ft3 = ZERO

    if service_type == "air":
        zoom_billable_weight_kg = round_up_to_step(total_weight_kg, Decimal("0.5"))

        freight_usd = (
            (zoom_billable_weight_kg / Decimal("0.5")) * air_rate_usd_per_half_kg
            if zoom_billable_weight_kg > 0
            else ZERO
        )

        charge_unit = "lb"
        chargeable_exact = zoom_billable_weight_kg * LB_PER_KG
        chargeable_display = round_int_half_up(chargeable_exact)

    elif service_type == "sea":
        zoom_billable_volume_ft3 = raw_volume_ft3 if raw_volume_ft3 > 0 else ZERO
        freight_usd = zoom_billable_volume_ft3 * sea_rate_usd_per_ft3

        charge_unit = "ft3"
        chargeable_exact = zoom_billable_volume_ft3
        chargeable_display = round_int_half_up(zoom_billable_volume_ft3)

    else:
        raise ValueError("service_type inválido para Zoom")

    freight_ves = freight_usd * bcv if bcv > 0 else ZERO

    handling_usd = ZERO
    if enable_handling_fee and handling_fee_usd > 0:
        handling_usd = handling_fee_usd * d(tracking_count)
    handling_ves = handling_usd * bcv

    packaging_usd = packaging_fee_usd
    packaging_ves = packaging_usd * bcv

    insurance_usd = ZERO
    if use_insurance and declared_value_usd > 0:
        if service_type == "air":
            if declared_value_usd < Decimal("100"):
                insurance_usd = insurance_air_min_usd
            else:
                insurance_usd = declared_value_usd * insurance_air_percent
        elif service_type == "sea":
            calculated_sea_insurance = declared_value_usd * insurance_sea_percent
            insurance_usd = (
                insurance_sea_min_usd
                if calculated_sea_insurance < insurance_sea_min_usd
                else calculated_sea_insurance
            )
    insurance_ves = insurance_usd * bcv

    customs_tax_usd = ZERO
    customs_tax_ves = ZERO

    repack_usd = ZERO
    repack_ves = ZERO
    repack_applies = False

    storage_usd = ZERO
    storage_ves = ZERO
    storage_days_charged = 0
    storage_chargeable_ft3 = ZERO

    purchase_service_usd = ZERO
    purchase_service_ves = ZERO

    compactation_fee_usd = ZERO
    compactation_fee_ves = ZERO

    total_usd = (
        freight_usd
        + insurance_usd
        + handling_usd
        + packaging_usd
        + repack_usd
        + storage_usd
        + purchase_service_usd
        + compactation_fee_usd
        + customs_tax_usd
    )

    total_ves = (
        freight_ves
        + insurance_ves
        + handling_ves
        + packaging_ves
        + repack_ves
        + storage_ves
        + purchase_service_ves
        + compactation_fee_ves
        + customs_tax_ves
    )

    return {
        "engine": "zoom_rules_v1",
        "service_type": service_type,
        "charge_unit": charge_unit,
        "chargeable_units_exact": float(chargeable_exact),
        "chargeable_units_display": chargeable_display,
        "uses_minimum_charge": uses_minimum_charge,
        "exchange_rate_used": float(bcv),
        "raw_metrics": {
            "real_weight_lb": round_usd(total_weight_lb),
            "volumetric_weight_lb": round_usd(raw_volumetric_lb),
            "raw_volume_ft3": round_usd(raw_volume_ft3),
            "display_volume_ft3": round_usd(raw_volume_ft3),
            "storage_chargeable_ft3": round_usd(storage_chargeable_ft3),
            "length_in_used": round_usd(length_in),
            "width_in_used": round_usd(width_in),
            "height_in_used": round_usd(height_in),
        },
        "flags": {
            "zoom_mode": True,
            "zoom_billing_basis": "half_kg" if service_type == "air" else "ft3",
            "zoom_billable_weight_kg": round_usd(zoom_billable_weight_kg),
            "zoom_billable_volume_ft3": round_usd(zoom_billable_volume_ft3),
            "enable_handling_fee": enable_handling_fee,
            "enable_repack_fee": enable_repack_fee,
            "hold_mode": hold_mode,
            "hold_days": hold_days,
            "storage_days_charged": storage_days_charged,
            "use_insurance": use_insurance,
            "repack_applies": repack_applies,
            "air_basis": "zoom_half_kg" if service_type == "air" else "n/a",
        },
        "public_calculator_reference": {
            "air_rate_usd_per_half_kg": round_usd(air_rate_usd_per_half_kg),
            "sea_rate_usd_per_ft3": round_usd(sea_rate_usd_per_ft3),
            "zoom_billable_weight_kg": round_usd(zoom_billable_weight_kg),
            "zoom_billable_volume_ft3": round_usd(zoom_billable_volume_ft3),
        },
        "breakdown": {
            "freight_usd": round_usd(freight_usd),
            "freight_ves": round_ves(freight_ves),
            "insurance_usd": round_usd(insurance_usd),
            "insurance_ves": round_ves(insurance_ves),
            "customs_tax_usd": round_usd(customs_tax_usd),
            "customs_tax_ves": round_ves(customs_tax_ves),
            "handling_usd": round_usd(handling_usd),
            "handling_ves": round_ves(handling_ves),
            "packaging_usd": round_usd(packaging_usd),
            "packaging_ves": round_ves(packaging_ves),
            "repack_usd": round_usd(repack_usd),
            "repack_ves": round_ves(repack_ves),
            "storage_usd": round_usd(storage_usd),
            "storage_ves": round_ves(storage_ves),
            "purchase_service_usd": round_usd(purchase_service_usd),
            "purchase_service_ves": round_ves(purchase_service_ves),
            "compactation_fee_usd": round_usd(compactation_fee_usd),
            "compactation_fee_ves": round_ves(compactation_fee_ves),
        },
        "total_usd": round_usd(total_usd),
        "total_ves": round_ves(total_ves),
    }
