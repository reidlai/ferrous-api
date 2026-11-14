package design

import (
	. "goa.design/goa/v3/dsl"
)

// IssueCreditRequest payload for issuing a courtesy credit concession.
var IssueCreditPayload = Type("IssueCreditRequest", func() {
	Description("Payload for issuing a courtesy credit concession on an order")
	Attribute("order_id", String, "Target order identifier", func() {
		Example("ord-98234-x")
	})
	Attribute("amount_cents", Int64, "Credit amount in cents (must be > 0)", func() {
		Minimum(1)
		Example(2500)
	})
	Attribute("reason", String, "Concession justification or customer dispute reason", func() {
		MinLength(3)
		MaxLength(500)
		Example("Late delivery concession approved by agent")
	})
	Required("order_id", "amount_cents", "reason")
})

// CancelOrderRequest payload for canceling an order.
var CancelOrderPayload = Type("CancelOrderRequest", func() {
	Description("Payload for canceling an existing order")
	Attribute("order_id", String, "Target order identifier", func() {
		Example("ord-98234-x")
	})
	Attribute("reason", String, "Order cancellation justification", func() {
		MinLength(3)
		MaxLength(500)
		Example("Customer requested cancellation prior to fulfillment")
	})
	Required("order_id", "reason")
})

// Service orders handles command mutations.
var _ = Service("orders", func() {
	Description("Order mutation commands dispatched to Apache Iggy WAL intake pipeline")
	HTTP(func() {
		Path("/api/v1/commands/orders")
	})

	Error("bad_request", ProblemDetails, "Bad Request - Malformed payload or missing required fields")
	Error("unauthorized", ProblemDetails, "Unauthorized - Missing or invalid JWT token")
	Error("forbidden", ProblemDetails, "Forbidden - Insufficient permissions for command dispatch")
	Error("unprocessable_entity", ProblemDetails, "Unprocessable Entity - Semantic validation failure")
	Error("internal_error", ProblemDetails, "Internal Server Error - Command dispatch or WAL fsync failure")

	Method("issue_credit", func() {
		Description("Dispatch IssueCourtesyCredit command to intake WAL")
		Security(JWTAuth, func() {
			Scope("api:write")
		})
		Payload(IssueCreditPayload)
		Result(CommandAcceptedResponse)
		HTTP(func() {
			POST("/issue-credit")
			Response(StatusAccepted)
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("forbidden", StatusForbidden)
			Response("unprocessable_entity", StatusUnprocessableEntity)
			Response("internal_error", StatusInternalServerError)
		})
	})

	Method("cancel_order", func() {
		Description("Dispatch CancelOrder command to intake WAL")
		Security(JWTAuth, func() {
			Scope("api:write")
		})
		Payload(CancelOrderPayload)
		Result(CommandAcceptedResponse)
		HTTP(func() {
			POST("/cancel")
			Response(StatusAccepted)
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("forbidden", StatusForbidden)
			Response("unprocessable_entity", StatusUnprocessableEntity)
			Response("internal_error", StatusInternalServerError)
		})
	})
})
