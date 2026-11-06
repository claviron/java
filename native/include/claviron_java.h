#ifndef ORBIT_JAVA_H
#define ORBIT_JAVA_H
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Protocol constants, not operational defaults. ABI v3 supports 64-bit targets. */
#define OJ_ABI_VERSION UINT32_C(3)
#define OJ_CAP_FOUNDATION UINT64_C(1)
#define OJ_CAP_APPLICATION UINT64_C(2)
#define OJ_CAP_STREAMING UINT64_C(4)
#define OJ_CAP_LIFECYCLE UINT64_C(8)
#define OJ_CAP_INVOKE UINT64_C(16)
typedef uint64_t oj_handle;
enum oj_status {
    OJ_OK = 0, OJ_INVALID_ARGUMENT = 1, OJ_INVALID_HANDLE = 2,
    OJ_WRONG_KIND = 3, OJ_NO_CAPACITY = 4, OJ_BUSY = 5,
    OJ_CLOSED = 6, OJ_CANCELLED = 7, OJ_PANIC = 8,
    OJ_INCOMPATIBLE = 9, OJ_END = 10, OJ_ALREADY_COMPLETED = 11, OJ_TRANSPORT = 12
};

typedef struct {
    uint32_t struct_size, abi_version;
    uint64_t capabilities;
    uint32_t options_size, options_align;
    uint32_t completion_size, completion_align;
    uint32_t buffer_view_size, buffer_view_align;
} oj_abi_info;

typedef struct {
    uint32_t struct_size, abi_version;
    uint64_t max_operations, max_buffer_bytes;
} oj_options;

typedef struct {
    oj_handle operation;
    int32_t status;
    uint32_t reserved;
    oj_handle buffer;
    oj_handle resource;
} oj_completion;

typedef struct {
    const uint8_t *data;
    uint64_t length;
} oj_buffer_view_record;

/* All output pointers must be writable, correctly aligned, and disjoint from
 * input and concurrent access. Inputs are borrowed only during the call. A
 * nonzero status leaves outputs unchanged. No Rust unwind crosses this ABI.
 * Status names have static diagnostic meaning; no error allocation or errno.
 * Query ABI/version compatibility before any other call. resource is an
 * application-owned handle; buffer may be zero on no-payload completion. */
int32_t oj_abi_query(oj_abi_info *out, uint32_t out_size);
int32_t oj_engine_open(const oj_options *options, oj_handle *out);

/* Shutdown requires no attached applications. It rejects new producers,
 * cancels pending operations, and wakes wait.
 * Already queued results remain valid. It is idempotent on a live engine. */
int32_t oj_engine_shutdown(oj_handle engine);
/* Release requires shutdown, no waiter, operations or leases. Busy retains the
 * handle. Released, stale and wrong-kind handles are rejected. */
int32_t oj_engine_release(oj_handle engine);

/* One blocking waiter per engine. No timeout. All admitted operations have one
 * reserved terminal slot. END means shutdown with pending/queued work drained.
 * Delivery transfers any buffer lease; operation release does not release it. */
int32_t oj_completion_wait(oj_handle engine, oj_completion *out);
int32_t oj_operation_cancel(oj_handle operation);
/* Before delivery: Busy. After delivery: retire the operation's queue credit. */
int32_t oj_operation_release(oj_handle operation);

/* Borrowed immutable view. Keep its lease alive and exclude release throughout
 * access. Closing scoped Java views must precede the matching native release.
 * Empty buffers are valid, separately owned leases. */
int32_t oj_buffer_view(oj_handle buffer, oj_buffer_view_record *out);
int32_t oj_buffer_release(oj_handle buffer);

/* Same-host application transport. Inputs are borrowed only during the call.
 * Each process takes a distinct java lane. All capacities are explicit. */
typedef struct { const uint8_t *data; uint64_t length; } oj_bytes;
typedef struct { oj_bytes name, value; } oj_header;
typedef struct {
    uint32_t struct_size, abi_version;
    uint64_t incarnation, max_exchanges, event_capacity, control_capacity;
    oj_bytes fleet, name;
} oj_application_options;
typedef struct {
    uint32_t size, options_size, options_align, request_size, request_align;
    uint32_t event_size, event_align, header_size;
    uint32_t control_size, control_align, state_size, state_align;
} oj_application_layout;
typedef struct {
    uint32_t struct_size, http_version;
    oj_bytes method, uri;
    uint64_t header_count, generation;
} oj_request_record;
typedef struct { oj_bytes id, topic, json; } oj_event_record;

typedef struct { uint64_t generation, running; } oj_application_state_record;
enum oj_lifecycle_action { OJ_START = 1, OJ_STOP = 2, OJ_RESTART = 3 };
enum oj_control_kind { OJ_LIFECYCLE = 1, OJ_COMMAND = 2, OJ_SHUTDOWN = 3 };
typedef struct {
    uint32_t kind, action;
    uint64_t generation, running;
    oj_bytes name, payload, edge;
} oj_control_record;

/* Application state is distinct from membership/incarnation. stop cancels the
 * generation but retains the lane; start after stop/restart creates a fresh
 * generation. The action is shared with sibling instances of this application. */
int32_t oj_application_state(oj_handle application, oj_application_state_record *out);
int32_t oj_application_action(oj_handle application, uint32_t action);
/* Completes at retirement (immediately for an old/stopped generation), or close.
 * This observer does not prevent graceful drain of accepted work. */
int32_t oj_generation_watch(oj_handle application, uint64_t expected, oj_handle *operation);
int32_t oj_request_current(oj_handle exchange, uint32_t *out);
/* Control resources are transferred by next. Release after the callback finishes;
 * the copied snapshot may outlive its native delivery scope. */
int32_t oj_control_next(oj_handle application, oj_handle *operation);
int32_t oj_control_info(oj_handle control, oj_control_record *out);
int32_t oj_control_release(oj_handle control);

int32_t oj_application_abi(oj_application_layout *out, uint32_t size);
int32_t oj_application_join(oj_handle engine, const oj_application_options *options, oj_handle *out);
/* next completes with resource=exchange, or END with resource=0 at withdrawal. */
int32_t oj_application_next(oj_handle application, oj_handle *operation);
/* Stop admission; accepted exchanges and delivered event scopes retain the
 * application until their handlers finish and the resources are released. */
int32_t oj_application_withdraw(oj_handle application);
int32_t oj_application_closed(oj_handle application, oj_handle *operation);
/* Requires completed drain; joins the native Runnable host. */
int32_t oj_application_release(oj_handle application);
/* Request views are borrowed until exchange release. No text conversion of
 * ordered headers; capacity is a count of oj_header records. */
int32_t oj_request_info(oj_handle exchange, oj_request_record *out);
int32_t oj_request_headers(oj_handle exchange, oj_header *out, uint64_t capacity);
int32_t oj_request_watch(oj_handle exchange, oj_handle *operation);
int32_t oj_request_respond(oj_handle exchange, uint32_t status,
                          const oj_header *headers, uint64_t count,
                          oj_bytes body, oj_handle *operation);
/* One pending read per exchange. Capacity is reserved against the engine's byte
 * budget until the returned buffer lease is released. Completion carries only
 * bytes read, or END with no lease at EOF. Cancellation abandons the exchange. */
int32_t oj_request_read(oj_handle exchange, uint64_t capacity, oj_handle *operation);
/* Streaming: commit head once, await each write, then finish. A write completes
 * only when all copied input bytes are accepted by Link. Concurrent output work
 * returns BUSY. A failed/cancelled write resets the exchange, never a clean FIN. */
int32_t oj_response_head(oj_handle exchange, uint32_t status,
                         const oj_header *headers, uint64_t count, oj_handle *operation);
int32_t oj_response_write(oj_handle exchange, oj_bytes body, oj_handle *operation);
int32_t oj_response_finish(oj_handle exchange, oj_handle *operation);
/* Explicitly abandons any unfinished exchange, wakes its I/O and drops ownership. */
int32_t oj_request_release(oj_handle exchange);
/* ApplicationEvent subscription is built in, not the raw service event protocol.
 * An event resource is transferred by next; release it after handling the event.
 * Copied metadata may outlive release; the resource pins callback drain.
 * publish accepts UTF-8 JSON for EventValue conversion, not an HTTP head codec. */
int32_t oj_event_subscribe(oj_handle application, oj_bytes topic, uint32_t enabled);
int32_t oj_event_next(oj_handle application, oj_handle *operation);
int32_t oj_event_publish(oj_handle application, oj_bytes topic, oj_bytes json);
int32_t oj_event_info(oj_handle event, oj_event_record *out);
int32_t oj_event_release(oj_handle event);

/* Invokes use their own fleet/connection, never HTTP exchange records. The
 * application owns generation cancellation and joins accepted invoke work.
 * The selected fleet must differ from the application's HTTP/event fleet and
 * use compatible Claviron Link geometry. No timer/retry is installed. */
typedef struct {
    uint32_t struct_size, abi_version;
    uint64_t pending_calls, accepted_calls, operation_bytes, request_bytes, response_bytes;
    oj_bytes fleet, service;
} oj_invoke_options;
typedef struct { oj_bytes target, service; } oj_invoke_route;
typedef struct {
    uint32_t size, options_size, options_align, request_size, request_align;
    uint32_t result_size, result_align, route_size;
} oj_invoke_layout;
typedef struct { oj_bytes operation, payload; uint64_t generation; } oj_invocation_record;
typedef struct { uint32_t code, reserved; oj_bytes payload; } oj_invoke_result_record;
enum oj_invoke_outcome {
    OJ_INVOKE_SUCCESS=0, OJ_INVOKE_UNKNOWN_TARGET=1, OJ_INVOKE_AMBIGUOUS_TARGET=2,
    OJ_INVOKE_UNAVAILABLE=3, OJ_INVOKE_BUSY=4, OJ_INVOKE_CLOSED=5,
    OJ_INVOKE_OUTCOME_UNKNOWN=6, OJ_INVOKE_PAYLOAD_TOO_LARGE=7, OJ_INVOKE_PROTOCOL=8,
    OJ_INVOKE_TRANSPORT=9, OJ_INVOKE_REMOTE_BUSY=10, OJ_INVOKE_REMOTE_CLOSING=11,
    OJ_INVOKE_UNSUPPORTED=12, OJ_INVOKE_RESPONSE_TOO_LARGE=13,
    OJ_INVOKE_REJECTED=14, OJ_INVOKE_INVALID_CONFIG=15
};
enum oj_invoke_reply { OJ_REPLY_SUCCESS=0, OJ_REPLY_BUSY=1, OJ_REPLY_CLOSING=2,
    OJ_REPLY_UNSUPPORTED=3, OJ_REPLY_TOO_LARGE=4, OJ_REPLY_REJECTED=5 };
int32_t oj_invoke_abi(oj_invoke_layout *out, uint32_t size);
int32_t oj_invoke_open(oj_handle application, const oj_invoke_options *options,
                       const oj_invoke_route *routes, uint64_t count, oj_handle *out);
int32_t oj_invoke_next(oj_handle binding, oj_handle *operation);
/* Call completes with a result resource. Cancellation withdraws local interest,
 * never proves rollback of accepted remote work. Unknown outcomes are not replayable. */
int32_t oj_invoke_call(oj_handle binding, oj_bytes target, oj_bytes operation,
                       oj_bytes payload, oj_handle *out_operation);
int32_t oj_invoke_withdraw(oj_handle binding);
int32_t oj_invoke_closed(oj_handle binding, oj_handle *operation);
int32_t oj_invoke_release(oj_handle binding);
int32_t oj_invocation_info(oj_handle invocation, oj_invocation_record *out);
int32_t oj_invocation_current(oj_handle invocation, uint32_t *out);
int32_t oj_invocation_watch(oj_handle invocation, oj_handle *operation);
/* Reply copies bytes before return; accepted reply is not proof of caller delivery. */
int32_t oj_invocation_reply(oj_handle invocation, uint32_t code, oj_bytes payload);
int32_t oj_invocation_release(oj_handle invocation);
int32_t oj_invoke_result_info(oj_handle result, oj_invoke_result_record *out);
int32_t oj_invoke_result_release(oj_handle result);

#ifdef __cplusplus
}
#endif
#endif
