# Provenance and workflow controls

The API records actor, correlation and cause separately from domain payloads. Checkout and shipment workers resume persisted metadata after restart. Local operator controls can pause, resume or request another attempt without resetting business progress or generating replacement payment/allocation IDs.

## Request and message metadata

Authenticated requests receive a fresh server-generated `X-Request-ID`. An optional `X-Correlation-ID` must be one UUID; otherwise it defaults to that request ID. Both IDs are returned as response headers. `X-Actor-ID`, `X-Causation-ID` and caller-provided `X-Request-ID` are rejected. Tenant credentials select the actor; metadata never selects a tenant or grants operator access.

```json
{
  "version": 1,
  "actor": "worker:checkout",
  "correlation_id": "11111111-1111-4111-8111-111111111111",
  "causation_id": "22222222-2222-4222-8222-222222222222",
  "origin_request_id": "33333333-3333-4333-8333-333333333333"
}
```

| Field | Meaning |
|---|---|
| `actor` | Executor identity: `tenant-key:<tenant>`, `worker:<service>`, `operator:local`, or `system:internal` |
| `correlation_id` | Diagnostic grouping for a request and its asynchronous descendants; callers can reuse it across related commands |
| `causation_id` | Immediate command, source event or stable operation ID selected by the adapter |
| `origin_request_id` | The accepted API request or operator invocation that initiated this branch, retained when workers take over |
| `version` | Metadata schema version; `0` explicitly means legacy/unknown provenance |

These are diagnostic references rather than a global event order, a cross-store foreign key, or proof of exactly-once execution. A correlation UUID is not an idempotency key. Metadata is excluded from business request hashes: retrying a command under another correlation/request ID still returns its original saved outcome and leaves the accepted fact's provenance unchanged.

Catalog prices/quotes, Inventory reservations/outbox facts, Ordering events, payment intents/journal, provider intents and Fulfillment history persist metadata in separate columns. Ordering history, checkout views, payment journal entries and shipment views/history expose their relevant metadata. Domain reducers and existing snapshot checksums do not incorporate it. Previously recorded facts retain `{"version":0}`; migration does not invent historical actors or request IDs. New internal work without a request gets an explicit internal/service actor and operation correlation, with no invented originating HTTP request.

An Ordering handoff's cause is its source event UUID. A worker records its own actor while retaining correlation and originating request. Fulfillment stores original request metadata and, separately, the accepted dispatch command metadata. Its worker resumes the dispatch branch. A first checkout cancellation records its own request provenance; repeated cancellation requests preserve that first record. Manual commands can start a new correlation group unless the client supplies the existing group ID.

Actor labels describe the current development identities. They do not identify an individual human. Credentials, credential hashes and free-form personal descriptions are absent from these envelopes. Production OIDC identities, durable distributed tracing and retention policies remain separate work.

## Local operator CLI

Run these commands from the repository root with the privileged local database binding in ignored `.env`. They are local operator functions; public tenant HTTP authentication does not create their in-process capability. The composition root explicitly binds one tenant scope. Production database roles and remote operator authentication are not implemented by this local capability.

```sh
go run ./cmd/admin workflow-status -kind checkout -tenant acme -id CHECKOUT_UUID
go run ./cmd/admin workflow-status -kind shipment -tenant acme -id SHIPMENT_UUID

go run ./cmd/admin workflow-control -kind checkout -tenant acme -id CHECKOUT_UUID \
  -action pause -reason investigation -key INCIDENT_PAUSE_KEY -expected-control-version 0

go run ./cmd/admin workflow-control -kind checkout -tenant acme -id CHECKOUT_UUID \
  -action resume -reason dependency_restored -key INCIDENT_RESUME_KEY -expected-control-version 1

go run ./cmd/admin workflow-control -kind shipment -tenant acme -id SHIPMENT_UUID \
  -action retry -reason operator_retry -key INCIDENT_RETRY_KEY -expected-control-version 2

go run ./cmd/admin workflow-history -kind checkout -tenant acme -id CHECKOUT_UUID \
  -after-version 0 -limit 50
```

Substitute IDs, keys and current control versions from `workflow-status`. `-correlation-id UUID` optionally links the operator action to an incident or workflow diagnostic group. The CLI fixes its audit actor to `operator:local`; arbitrary actor strings cannot grant access. Reasons are controlled codes: `investigation`, `dependency_restored`, and `operator_retry`.

| Action | Preconditions and effect |
|---|---|
| `pause` | Nonterminal, currently unpaused, and no live lease; prevents new claims |
| `resume` | Nonterminal, paused, and no live lease; makes its existing progress eligible now |
| `retry` | Nonterminal, unpaused, and no live lease; removes scheduling delay without clearing errors/counts or changing progress; shipments must already be dispatching |

Controls require `expected-control-version`, independently of the business/shipment version. Every accepted action advances that control version and lease token, writes an immutable before/after audit record, and saves its response in one local transaction. Operator races have one winner at a version. Audit insertion failure rolls back the scheduling change and request identity. History uses a resource-local version cursor, maximum page size 100. Audit UUIDs and timestamps are not global commit-order cursors.

Repeat the same action/key/payload for an uncertain CLI result. It returns the original audit UUID and response even after later progress or terminal completion. Changed input with the same key conflicts. New controls on terminal jobs fail. Responses are saved acknowledgments; `workflow-status` reports current scheduling state.

## Pause and recovery boundaries

A live lease makes a control fail with `workflow has an active lease`. Wait for the bounded attempt or lease expiry, then retry the same request. Controls recheck database time after acquiring the row lock. They do not steal an active claim. When accepted, a new token fences expired workers' checkpoints.

Pausing prevents new claims. An external call made by an expired worker can still finish afterward. The integration scenario deliberately completes such a call while paused and rejects its old checkpoint. Resume reuses the same stable operation ID and reconciles the physical/financial result. Pause is not a cancellation or a promise that an already-running external effect disappeared.

A paused checkout still accepts a cancellation request, but compensation waits for resume. Inventory hold deadlines continue independently while checkout is paused; resumption may discover expiry and compensate, including refunding a capture if allocation can no longer commit. A paused shipment still accepts versioned warehouse commands, including dispatch acceptance; its worker waits for resume. A retry leaves the workflow stage, completed-line count, cancellation flag, immutable facts, financial identity, allocation IDs and diagnostic retry counts intact. These controls do not implement arbitrary checkpoint edits, forced outcomes, refunds, returns, or projection quarantine repair.

Reproduce with `make up`, then `make verify-operations`. The local scenario stops the provider, pauses a retrying checkout, restores the provider, restarts API/worker, replays the control, resumes, checks one capture and its lineage, and controls two warehouse shipments. It writes ignored `artifacts/operations.json`. See the [lease walkthrough](chapters/ch09/workflow-control.md) and [recovery runbook](runbooks/workflow-control.md).
