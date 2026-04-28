from typing import Literal
from pydantic import BaseModel, Field, ConfigDict


ServiceType = Literal["air", "sea", "correspondence"]
CourierCode = Literal["zoom", "owc"]
DeliveryType = Literal["office", "delivery", "home"]
RegionType = Literal["region_central", "resto_pais"]
HoldModeType = Literal["none", "general", "repack"]
ZoomServiceType = Literal["international_locker"]
ZoomCountryType = Literal["US", "VE"]
ZoomShipmentKindType = Literal["merchandise", "document"]


class QuoteItem(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    item_name: str = Field(..., min_length=1, max_length=200)
    category: str | None = None
    quantity: int = Field(default=1, ge=1)
    unit_price_usd: float = Field(default=0, ge=0)

    weight_kg: float = Field(default=0, ge=0)
    weight_lb: float = Field(default=0, ge=0)

    length_in: float = Field(default=0, ge=0)
    width_in: float = Field(default=0, ge=0)
    height_in: float = Field(default=0, ge=0)

    volume_ft3: float = Field(default=0, ge=0)


class QuoteCalculateRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    courier_code: CourierCode
    service_type: ServiceType

    delivery_type: DeliveryType = "office"
    region: RegionType = "region_central"

    declared_value_usd: float = Field(default=0, ge=0)

    total_weight_kg: float = Field(default=0, ge=0)
    total_weight_lb: float = Field(default=0, ge=0)
    total_volume_ft3: float = Field(default=0, ge=0)

    # Dimensiones globales del paquete/caja principal
    length_in: float = Field(default=0, ge=0)
    width_in: float = Field(default=0, ge=0)
    height_in: float = Field(default=0, ge=0)

    total_same_item_qty: int = Field(default=1, ge=1)
    tracking_count: int = Field(default=1, ge=1)

    enable_handling_fee: bool = True
    enable_repack_fee: bool = False
    compactation_requested: bool = False

    hold_mode: HoldModeType = "none"
    hold_days: int = Field(default=0, ge=0)

    use_insurance: bool = False
    use_purchase_by_order: bool = False
    apply_provisional_customs: bool = True

    storage_fee_ves_per_day_ft3_override: float | None = Field(default=None, ge=0)

    zoom_service: ZoomServiceType = "international_locker"
    origin_country: ZoomCountryType = "US"
    destination_country: ZoomCountryType = "VE"
    shipment_kind: ZoomShipmentKindType = "merchandise"
    consolidated: bool = False
    consolidated_package_count: int = Field(default=1, ge=1)
    use_protection: bool = True

    items: list[QuoteItem] = Field(default_factory=list)


class QuoteSaveRequest(QuoteCalculateRequest):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    customer_name: str | None = None
    customer_phone: str | None = None
    customer_email: str | None = None
    notes: str | None = None


class RestrictedItemMatch(BaseModel):
    model_config = ConfigDict(extra="forbid", str_strip_whitespace=True)

    item_name: str
    restriction_level: str
    matched_input: str
    reason: str | None = None
    notes: str | None = None


class QuoteCalculateResponse(BaseModel):
    courier: str
    courier_code: str
    service_type: str
    exchange_rate_used: float
    restricted_matches: list[RestrictedItemMatch]
    quote: dict


class QuoteSaveResponse(BaseModel):
    shipment_id: str
    shipment_code: str
    courier: str
    courier_code: str
    service_type: str
    exchange_rate_used: float
    restricted_matches: list[RestrictedItemMatch]
    quote: dict
