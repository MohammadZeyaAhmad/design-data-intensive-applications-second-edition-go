
¬
 inventory/legacy/inventory.protoatlas.inventory.v1"ˆ
ReserveRequest'
idempotency_key (	RidempotencyKey
sku (	Rsku
quantity (Rquantity
ttl_seconds (R
ttlSeconds"'
GetReservationRequest
id (	Rid"Ç
Reservation
id (	Rid
	tenant_id (	RtenantId
sku (	Rsku
quantity (Rquantity
status (	Rstatus
version (Rversion+
expires_at_unix_ms (RexpiresAtUnixMs"£
Stock
	tenant_id (	RtenantId
sku (	Rsku
on_hand (RonHand
reserved (Rreserved
	allocated (R	allocated
version (Rversion"†
ReservationReplyA
reservation (2.atlas.inventory.v1.ReservationRreservation/
stock (2.atlas.inventory.v1.StockRstock2¾
	InventoryS
Reserve".atlas.inventory.v1.ReserveRequest$.atlas.inventory.v1.ReservationReply\
GetReservation).atlas.inventory.v1.GetReservationRequest.atlas.inventory.v1.Reservationbproto3