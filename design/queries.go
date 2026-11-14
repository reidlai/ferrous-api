package design

import (
	. "goa.design/goa/v3/dsl"
)

// OrderItemType defines an individual line item.
var OrderItemType = Type("OrderItem", func() {
	Description("Individual line item within an order")
	Attribute("item_id", String, "Line item unique identifier", func() {
		Example("item-001")
	})
	Attribute("product_id", String, "Product SKU or identifier", func() {
		Example("prod-4412")
	})
	Attribute("price_cents", Int64, "Price in cents per unit", func() {
		Example(1999)
	})
	Attribute("quantity", Int, "Quantity ordered", func() {
		Minimum(1)
		Example(2)
	})
	Required("item_id", "product_id", "price_cents", "quantity")
})

// OrderDetailsType defines the full analytical order view.
var OrderDetailsType = Type("OrderDetails", func() {
	Description("Detailed order state queried from lakehouse")
	Attribute("order_id", String, "Unique order identifier", func() {
		Example("ord-98234-x")
	})
	Attribute("customer_id", String, "Customer identifier", func() {
		Example("cust-1029")
	})
	Attribute("status", String, "Current order lifecycle status", func() {
		Example("processing")
	})
	Attribute("total_amount_cents", Int64, "Total order amount in cents", func() {
		Example(3998)
	})
	Attribute("currency", String, "Currency ISO code", func() {
		Example("USD")
	})
	Attribute("items", ArrayOf(OrderItemType), "List of items in order")
	Attribute("created_at", String, "RFC 3339 creation timestamp", func() {
		Format(FormatDateTime)
	})
	Attribute("updated_at", String, "RFC 3339 last update timestamp", func() {
		Format(FormatDateTime)
	})
	Required("order_id", "customer_id", "status", "total_amount_cents", "currency", "items", "created_at", "updated_at")
})

// OrderSummaryType defines the order summary record for listings.
var OrderSummaryType = Type("OrderSummary", func() {
	Description("Summary order record for lakehouse list queries")
	Attribute("order_id", String, "Unique order identifier", func() {
		Example("ord-98234-x")
	})
	Attribute("customer_id", String, "Customer identifier", func() {
		Example("cust-1029")
	})
	Attribute("status", String, "Current order lifecycle status", func() {
		Example("delivered")
	})
	Attribute("total_amount_cents", Int64, "Total order amount in cents", func() {
		Example(5420)
	})
	Attribute("currency", String, "Currency ISO code", func() {
		Example("USD")
	})
	Attribute("created_at", String, "RFC 3339 creation timestamp", func() {
		Format(FormatDateTime)
	})
	Required("order_id", "customer_id", "status", "total_amount_cents", "currency", "created_at")
})

// OrderListResponseType defines the paginated list response.
var OrderListResponseType = Type("OrderListResponse", func() {
	Description("Paginated list of orders from lakehouse query")
	Attribute("orders", ArrayOf(OrderSummaryType), "Page of order records")
	Attribute("total_count", Int64, "Total matching records in lakehouse dataset", func() {
		Example(1450)
	})
	Attribute("limit", Int64, "Page limit requested", func() {
		Example(50)
	})
	Attribute("offset", Int64, "Page offset requested", func() {
		Example(0)
	})
	Required("orders", "total_count", "limit", "offset")
})

// GetOrderPayload defines parameters for fetching an order.
var GetOrderPayload = Type("GetOrderPayload", func() {
	Attribute("order_id", String, "Order identifier", func() {
		Example("ord-98234-x")
	})
	Required("order_id")
})

// ListOrdersPayload defines query parameters for order listing.
var ListOrdersPayload = Type("ListOrdersPayload", func() {
	Attribute("limit", Int64, "Maximum records to return", func() {
		Default(50)
		Minimum(1)
		Maximum(1000)
	})
	Attribute("offset", Int64, "Record offset for pagination", func() {
		Default(0)
		Minimum(0)
	})
	Attribute("customer_id", String, "Optional filter by customer ID")
	Attribute("status", String, "Optional filter by order status")
})

// Service queries handles analytical and order queries.
var _ = Service("queries", func() {
	Description("In-process vectorized lakehouse SQL queries over Delta Lake tables")
	HTTP(func() {
		Path("/api/v1/orders")
	})

	Error("bad_request", ProblemDetails, "Bad Request - Invalid query parameters")
	Error("unauthorized", ProblemDetails, "Unauthorized - Missing or invalid JWT token")
	Error("not_found", ProblemDetails, "Not Found - Order record does not exist")
	Error("internal_error", ProblemDetails, "Internal Server Error - Lakehouse scan execution failure")

	Method("get_order", func() {
		Description("Fetch single order details by ID from Delta Lake")
		Security(JWTAuth, func() {
			Scope("api:read")
		})
		Payload(GetOrderPayload)
		Result(OrderDetailsType)
		HTTP(func() {
			GET("/{order_id}")
			Response(StatusOK)
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("not_found", StatusNotFound)
			Response("internal_error", StatusInternalServerError)
		})
	})

	Method("list_orders", func() {
		Description("Query paginated orders from Delta Lake table")
		Security(JWTAuth, func() {
			Scope("api:read")
		})
		Payload(ListOrdersPayload)
		Result(OrderListResponseType)
		HTTP(func() {
			GET("")
			Param("limit")
			Param("offset")
			Param("customer_id")
			Param("status")
			Response(StatusOK)
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("internal_error", StatusInternalServerError)
		})
	})
})
