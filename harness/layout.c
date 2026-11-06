#include "claviron_java.h"
#include <stddef.h>

_Static_assert(sizeof(oj_abi_info) == 40, "ABI info size");
_Static_assert(_Alignof(oj_abi_info) == 8, "ABI info alignment");
_Static_assert(offsetof(oj_abi_info, capabilities) == 8, "capability offset");
_Static_assert(offsetof(oj_abi_info, buffer_view_align) == 36, "last field offset");
_Static_assert(sizeof(oj_options) == 24, "options size");
_Static_assert(offsetof(oj_options, max_operations) == 8, "operation budget offset");
_Static_assert(offsetof(oj_options, max_buffer_bytes) == 16, "byte budget offset");
_Static_assert(sizeof(oj_completion) == 32, "completion size");
_Static_assert(offsetof(oj_completion, status) == 8, "status offset");
_Static_assert(offsetof(oj_completion, buffer) == 16, "lease offset");
_Static_assert(sizeof(oj_buffer_view_record) == 16, "buffer size");
_Static_assert(offsetof(oj_buffer_view_record, length) == 8, "length offset");

_Static_assert(sizeof(oj_bytes) == 16, "byte view size");
_Static_assert(sizeof(oj_header) == 32, "header pair size");
_Static_assert(sizeof(oj_application_options) == 72, "application options size");
_Static_assert(offsetof(oj_application_options, fleet) == 40, "fleet view offset");
_Static_assert(sizeof(oj_application_layout) == 48, "application layout query size");
_Static_assert(sizeof(oj_request_record) == 56, "request info size");
_Static_assert(offsetof(oj_request_record, header_count) == 40, "header count offset");
_Static_assert(sizeof(oj_event_record) == 48, "event info size");
_Static_assert(offsetof(oj_completion, resource) == 24, "resource transfer offset");

_Static_assert(sizeof(oj_application_state_record) == 16, "application state size");
_Static_assert(sizeof(oj_control_record) == 72, "control record size");
_Static_assert(offsetof(oj_control_record, name) == 24, "control name offset");
_Static_assert(offsetof(oj_control_record, edge) == 56, "control edge offset");
_Static_assert(offsetof(oj_request_record, generation) == 48, "request generation offset");

_Static_assert(sizeof(oj_invoke_options) == 80, "invoke options size");
_Static_assert(offsetof(oj_invoke_options, fleet) == 48, "invoke fleet offset");
_Static_assert(sizeof(oj_invoke_layout) == 32, "invoke layout query size");
_Static_assert(sizeof(oj_invoke_route) == 32, "invoke route size");
_Static_assert(sizeof(oj_invocation_record) == 40, "invocation metadata size");
_Static_assert(offsetof(oj_invocation_record, generation) == 32, "invoke generation offset");
_Static_assert(sizeof(oj_invoke_result_record) == 24, "invoke result size");

int main(void) { return 0; }
