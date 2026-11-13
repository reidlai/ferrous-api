package design

import (
	. "goa.design/goa/v3/dsl"
)

// DomainEventStreamItemType defines a domain event streamed over SSE.
var DomainEventStreamItemType = Type("DomainEventStreamItem", func() {
	Description("Domain event notification broadcast over SSE")
	Attribute("event_id", String, "Unique event UUIDv7", func() {
		Format(FormatUUID)
		Example("0193108c-9824-7da9-8f0a-15603d772274")
	})
	Attribute("event_type", String, "Event discriminator name", func() {
		Example("CourtesyCreditIssued")
	})
	Attribute("aggregate_id", String, "Associated order or entity ID", func() {
		Example("ord-98234-x")
	})
	Attribute("payload", Any, "Arbitrary JSON event payload")
	Attribute("timestamp", String, "RFC 3339 timestamp", func() {
		Format(FormatDateTime)
	})
	Required("event_id", "event_type", "aggregate_id", "payload", "timestamp")
})

// BenchmarkTelemetryItemType defines real-time benchmark streaming metrics.
var BenchmarkTelemetryItemType = Type("BenchmarkTelemetryItem", func() {
	Description("Real-time agent benchmark telemetry measurement")
	Attribute("run_id", String, "Benchmark execution run ID", func() {
		Example("bench-run-842")
	})
	Attribute("step", Int64, "Execution step index", func() {
		Example(42)
	})
	Attribute("ttft_micros", Int64, "Time to first token in microseconds", func() {
		Example(12450)
	})
	Attribute("itl_micros", Int64, "Inter-token latency in microseconds", func() {
		Example(3200)
	})
	Attribute("itl_jitter_micros", Int64, "Inter-token latency jitter in microseconds", func() {
		Example(180)
	})
	Attribute("token_delta", Int64, "Token count increment", func() {
		Example(5)
	})
	Attribute("tps", Float64, "Tokens per second throughput", func() {
		Example(312.5)
	})
	Attribute("model_id", String, "Evaluated LLM model identifier", func() {
		Example("llama-3.1-8b-instruct")
	})
	Attribute("provider", String, "Inference runtime provider", func() {
		Example("vllm")
	})
	Attribute("timestamp", String, "RFC 3339 timestamp", func() {
		Format(FormatDateTime)
	})
	Required("run_id", "step", "ttft_micros", "itl_micros", "token_delta", "timestamp")
})

// PipelineTelemetryItemType defines streaming pipeline health metrics.
var PipelineTelemetryItemType = Type("PipelineTelemetryItem", func() {
	Description("Streaming pipeline health and lag telemetry")
	Attribute("iggy_commit_lag", Int64, "Iggy broker commit log lag count", func() {
		Example(12)
	})
	Attribute("iggy_fsync_latency_us", Int64, "Iggy WAL fsync commit latency in microseconds", func() {
		Example(85)
	})
	Attribute("arroyo_watermark_lag_ms", Int64, "Arroyo streaming watermark lag in milliseconds", func() {
		Example(145)
	})
	Attribute("delta_commit_count", Int64, "Delta Lake 2PC commit sequence count", func() {
		Example(8920)
	})
	Attribute("olist_replay_rate_eps", Float64, "Read-only olist/orders_stream replay rate in events per second", func() {
		Example(1250.0)
	})
	Attribute("olist_high_watermark", Int64, "Read-only olist/orders_stream topic high watermark offset", func() {
		Example(98420)
	})
	Attribute("olist_consumer_lag", Int64, "Read-only olist/orders_stream consumer group lag", func() {
		Example(12)
	})
	Attribute("iggy_status", String, "Iggy telemetry source availability status (available or unavailable)", func() {
		Example("available")
	})
	Attribute("arroyo_status", String, "Arroyo telemetry source availability status (available or unavailable)", func() {
		Example("available")
	})
	Attribute("delta_status", String, "Delta Lake commit log source availability status (available or unavailable)", func() {
		Example("available")
	})
	Attribute("timestamp", String, "RFC 3339 timestamp", func() {
		Format(FormatDateTime)
	})
	Required("iggy_commit_lag", "arroyo_watermark_lag_ms", "delta_commit_count", "timestamp")
})

// EventStreamPayload defines query filters for the general event stream.
var EventStreamPayload = Type("EventStreamPayload", func() {
	Attribute("order_id", String, "Optional filter by order ID")
	Attribute("event_types", String, "Optional comma-separated event types")
})

// BenchmarkStreamPayload defines parameters for benchmark telemetry.
var BenchmarkStreamPayload = Type("BenchmarkStreamPayload", func() {
	Attribute("run_id", String, "Benchmark run identifier")
	Required("run_id")
})

// Service streams handles SSE real-time streaming endpoints.
var _ = Service("streams", func() {
	Description("Server-Sent Events (SSE) real-time streaming endpoints")
	HTTP(func() {
		Path("/api/v1")
	})

	Error("bad_request", ProblemDetails, "Bad Request - Invalid stream parameters")
	Error("unauthorized", ProblemDetails, "Unauthorized - Missing or invalid JWT token")
	Error("not_found", ProblemDetails, "Not Found - Run ID not found")
	Error("internal_error", ProblemDetails, "Internal Server Error - Stream initialization failure")

	Method("event_stream", func() {
		Description("Stream domain events and AI agent thoughts over native SSE")
		Security(JWTAuth, func() {
			Scope("api:read")
		})
		Payload(EventStreamPayload)
		Result(DomainEventStreamItemType)
		HTTP(func() {
			GET("/events/stream")
			Param("order_id")
			Param("event_types")
			Response(StatusOK, func() {
				ContentType("text/event-stream")
			})
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("internal_error", StatusInternalServerError)
		})
	})

	Method("benchmark_stream", func() {
		Description("Stream real-time TTFT and token performance metrics over SSE")
		Security(JWTAuth, func() {
			Scope("api:read")
		})
		Payload(BenchmarkStreamPayload)
		Result(BenchmarkTelemetryItemType)
		HTTP(func() {
			GET("/benchmarks/{run_id}/stream")
			Response(StatusOK, func() {
				ContentType("text/event-stream")
			})
			Response("bad_request", StatusBadRequest)
			Response("unauthorized", StatusUnauthorized)
			Response("not_found", StatusNotFound)
			Response("internal_error", StatusInternalServerError)
		})
	})

	Method("pipeline_telemetry_stream", func() {
		Description("Stream streaming pipeline watermark lag and commit counts over SSE")
		Security(JWTAuth, func() {
			Scope("api:read")
		})
		Result(PipelineTelemetryItemType)
		HTTP(func() {
			GET("/telemetry/pipeline/stream")
			Response(StatusOK, func() {
				ContentType("text/event-stream")
			})
			Response("unauthorized", StatusUnauthorized)
			Response("internal_error", StatusInternalServerError)
		})
	})
})
