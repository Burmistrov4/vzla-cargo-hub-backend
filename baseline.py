import json
from backend.main import build_quote_result, QuoteCalculateRequest
from backend.models import QuoteItem

scenarios = [
    {
        "name": "OWC Aereo 1 item (5 kg)",
        "payload": QuoteCalculateRequest(
            courier_code="owc",
            service_type="air",
            declared_value_usd=100.0,
            enable_repack_fee=False,
            items=[
                QuoteItem(item_name="test",quantity=1, weight_kg=5.0, length_in=10, width_in=10, height_in=10)
            ]
        )
    },
    {
        "name": "OWC Aereo Repack 2 items (15 kg)",
        "payload": QuoteCalculateRequest(
            courier_code="owc",
            service_type="air",
            declared_value_usd=500.0,
            enable_repack_fee=True,
            items=[
                QuoteItem(item_name="test",quantity=2, weight_kg=7.5, length_in=15, width_in=15, height_in=15)
            ]
        )
    },
    {
        "name": "OWC Maritimo (3 items grandes)",
        "payload": QuoteCalculateRequest(
            courier_code="owc",
            service_type="sea",
            declared_value_usd=300.0,
            enable_repack_fee=False,
            items=[
                QuoteItem(item_name="test",quantity=3, weight_kg=10.0, length_in=20, width_in=20, height_in=20)
            ]
        )
    },
    {
        "name": "Zoom Aereo 1 item",
        "payload": QuoteCalculateRequest(
            courier_code="zoom",
            service_type="air",
            declared_value_usd=150.0,
            zoom_service="international_locker",
            items=[
                QuoteItem(item_name="test",quantity=1, weight_kg=1.3, length_in=5, width_in=5, height_in=5)
            ]
        )
    },
    {
        "name": "OWC Aereo (Muchas cajas consolidadas)",
        "payload": QuoteCalculateRequest(
            courier_code="owc",
            service_type="air",
            declared_value_usd=1000.0,
            enable_repack_fee=True,
            items=[
                QuoteItem(item_name="caja", quantity=10, weight_kg=2.0, length_in=5, width_in=5, height_in=5)
            ]
        )
    },
    {
        "name": "Zoom Aereo (Pesado)",
        "payload": QuoteCalculateRequest(
            courier_code="zoom",
            service_type="air",
            declared_value_usd=200.0,
            zoom_service="international_locker",
            items=[
                QuoteItem(item_name="pesado", quantity=1, weight_kg=15.0, length_in=10, width_in=10, height_in=10)
            ]
        )
    }
]

def run():
    results = []
    for sc in scenarios:
        try:
            courier, rate, exchange, restricted_matches, quote = build_quote_result(sc["payload"])
            results.append({
                "scenario": sc["name"],
                "quote": quote,
                "exchange_rate": exchange["rate"]
            })
        except Exception as e:
            results.append({
                "scenario": sc["name"],
                "error": str(e)
            })

    with open("baseline_quotes.json", "w") as f:
        json.dump(results, f, indent=2)
    print("Baseline saved to baseline_quotes.json")

if __name__ == "__main__":
    run()
