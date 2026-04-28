from backend.calculator import calculate_zoom_quote


EXCHANGE_RATE = 485.2251

RULES = {
    "air_rate_usd_per_kg": 32.40,
    "billable_weight_step_kg": 0.5,
    "protection_percent": 0.01,
    "protection_min_usd": 1.20,
    "consolidation_fee_usd": 6.00,
}

CASES = [
    {
        "name": "1 kg / USD 1000 / no consolidado",
        "weight_kg": 1,
        "declared_value_usd": 1000,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 42.40,
    },
    {
        "name": "1 kg / USD 1000 / consolidado 2",
        "weight_kg": 1,
        "declared_value_usd": 1000,
        "consolidated": True,
        "count": 2,
        "expected_total_usd": 48.40,
    },
    {
        "name": "5 kg / USD 1000 / no consolidado",
        "weight_kg": 5,
        "declared_value_usd": 1000,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 172.00,
    },
    {
        "name": "5 kg / USD 1000 / consolidado 2",
        "weight_kg": 5,
        "declared_value_usd": 1000,
        "consolidated": True,
        "count": 2,
        "expected_total_usd": 178.00,
    },
    {
        "name": "10 kg / USD 1000 / no consolidado",
        "weight_kg": 10,
        "declared_value_usd": 1000,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 334.00,
    },
    {
        "name": "10 kg / USD 1000 / consolidado 2",
        "weight_kg": 10,
        "declared_value_usd": 1000,
        "consolidated": True,
        "count": 2,
        "expected_total_usd": 340.00,
    },
    {
        "name": "10 kg / USD 200 / no consolidado",
        "weight_kg": 10,
        "declared_value_usd": 200,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 326.00,
    },
    {
        "name": "10 kg / USD 201 / no consolidado",
        "weight_kg": 10,
        "declared_value_usd": 201,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 326.01,
    },
    {
        "name": "2.26 kg / USD 50 / no consolidado",
        "weight_kg": 2.26,
        "declared_value_usd": 50,
        "consolidated": False,
        "count": 1,
        "expected_total_usd": 82.20,
    },
]


def build_payload(case):
    return {
        "courier_code": "zoom",
        "service_type": "air",
        "delivery_type": "office",
        "region": "region_central",
        "declared_value_usd": case["declared_value_usd"],
        "total_weight_kg": case["weight_kg"],
        "total_weight_lb": 0,
        "total_volume_ft3": 0,
        "length_in": 0,
        "width_in": 0,
        "height_in": 0,
        "tracking_count": 1,
        "items": [],
        "zoom_service": "international_locker",
        "origin_country": "US",
        "destination_country": "VE",
        "shipment_kind": "merchandise",
        "consolidated": case["consolidated"],
        "consolidated_package_count": case["count"],
        "use_protection": True,
    }


def main():
    print("Zoom probe - observed formula zoom_locker_air_office_v1")
    print("rate_usd_per_kg=32.40, step=0.5kg, protection=max(value*1%, 1.20)")
    print()

    failed = 0

    for case in CASES:
        result = calculate_zoom_quote(RULES, EXCHANGE_RATE, build_payload(case))
        actual = float(result["total_usd"])
        expected = float(case["expected_total_usd"])
        diff = round(actual - expected, 2)
        ok = abs(diff) <= 0.01
        failed += 0 if ok else 1

        breakdown = result["breakdown"]
        metrics = result["raw_metrics"]
        print(
            f"{'OK' if ok else 'FAIL'} | {case['name']} | "
            f"billable={metrics['billable_weight_kg']} kg | "
            f"freight={breakdown['freight_usd']:.2f} | "
            f"protection={breakdown['protection_usd']:.2f} | "
            f"consolidation={breakdown['consolidation_usd']:.2f} | "
            f"total={actual:.2f} | expected={expected:.2f} | diff={diff:+.2f}"
        )

    if failed:
        raise SystemExit(f"{failed} Zoom cases failed")


if __name__ == "__main__":
    main()
