package design

import (
	. "goa.design/goa/v3/dsl"
)

// JWTAuth defines the Kanidm JWT Bearer security scheme.
var JWTAuth = JWTSecurity("jwt_bearer", func() {
	Description("Kanidm JWT Bearer token authentication")
	Scope("api:read", "Read access to analytical queries and telemetry streams")
	Scope("api:write", "Write access to mutation commands")
	Scope("api:admin", "Administrative operations")
})

// InvalidParam represents a single validation failure on a field or parameter.
var InvalidParam = Type("InvalidParam", func() {
	Description("Validation error detail for a specific request parameter or field")
	Attribute("name", String, "Field or parameter name that failed validation", func() {
		Example("amount_cents")
	})
	Attribute("reason", String, "Explanation of why validation failed", func() {
		Example("must be greater than 0")
	})
	Required("name", "reason")
})

// ProblemDetails represents an RFC 7807 conformant error envelope.
var ProblemDetails = Type("ProblemDetails", func() {
	Description("RFC 7807 conformant Problem Details error envelope")
	Attribute("type", String, "URI reference identifying problem type", func() {
		Default("about:blank")
		Example("urn:ferrouscx:error:validation")
	})
	Attribute("title", String, "Short human-readable summary of problem type", func() {
		Example("Invalid Request Payload")
	})
	Attribute("status", Int, "HTTP status code", func() {
		Example(400)
	})
	Attribute("detail", String, "Human-readable explanation specific to this occurrence", func() {
		Example("Field amount_cents must be greater than zero")
	})
	Attribute("instance", String, "URI reference identifying specific occurrence of the problem", func() {
		Example("/api/v1/commands/orders/issue-credit")
	})
	Attribute("code", String, "Application-specific machine-readable error code", func() {
		Example("INVALID_ARGUMENT")
	})
	Attribute("invalid_params", ArrayOf(InvalidParam), "Detailed parameter validation failures")
	Required("title", "status")
})

// CommandAcceptedResponse represents standard asynchronous command acknowledgment.
var CommandAcceptedResponse = Type("CommandAcceptedResponse", func() {
	Description("Standard response for asynchronous command acceptance")
	Attribute("command_id", String, "Monotonically ordered UUIDv7 identifying the dispatched command", func() {
		Format(FormatUUID)
		Example("0193108c-9824-7da9-8f0a-15603d772274")
	})
	Attribute("status", String, "Command intake status", func() {
		Example("accepted")
	})
	Attribute("timestamp", String, "RFC 3339 UTC timestamp when command was ingested", func() {
		Format(FormatDateTime)
		Example("2026-11-09T08:30:00Z")
	})
	Required("command_id", "status", "timestamp")
})

// API declaration for ferrous-api.
var _ = API("ferrous-api", func() {
	Title("FerrousCX Contract-First Axum API Service")
	Description("Contract-First HTTP API Service and SSE real-time streaming hub for the FerrousCX Platform.")
	Version("1.0.0")
	Server("ferrous-api", func() {
		Description("FerrousCX API Service HTTP Server")
		Services("orders", "queries", "streams")
		Host("localhost", func() {
			Description("Local development server")
			URI("http://localhost:8080")
		})
	})
})
