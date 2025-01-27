-module(kamock_api_versions).
-export([handle_api_versions_request/2]).

-include_lib("kafcod/include/api_key.hrl").
-include_lib("kafcod/include/error_code.hrl").

handle_api_versions_request(#{correlation_id := CorrelationId}, _Env) ->
    % Lie about which API keys and versions we support.

    % The list below isn't used with kafine. It supports a narrow range of broker versions, and it always uses the
    % highest versions that Wireshark understands, so it doesn't bother asking the broker about supported API versions.

    % In order to support testing with kcat (see the README for justification), we need to advertise support for API
    % requests and versions that we don't actually support.
    %
    % Specifically: we need to announce support for at least Produce v2 and Fetch v2. For sanity's sake, however, we'll
    % advertise v0+
    %
    % kcat won't actually use them, so we don't actually need to implement them.

    % The reverse is true with kafire: it only cares about the advertised supported version for Fetch. It will always
    % use fixed versions for all other messages. So kamock_broker_handler supports versions (and API keys) that aren't
    % necessarily included here.

    % It turns out that kafta also ignores API versions; it'll use Metadata v10, for example, even though we don't
    % advertise it.

    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        api_keys => [
            #{api_key => ?PRODUCE, min_version => 0, max_version => 8},
            #{api_key => ?FETCH, min_version => 0, max_version => 11},
            #{api_key => ?LIST_OFFSETS, min_version => 0, max_version => 5},
            #{api_key => ?METADATA, min_version => 0, max_version => 12},
            #{api_key => ?FIND_COORDINATOR, min_version => 0, max_version => 3},
            #{api_key => ?OFFSET_COMMIT, min_version => 0, max_version => 8},
            #{api_key => ?OFFSET_FETCH, min_version => 0, max_version => 4},
            #{api_key => ?JOIN_GROUP, min_version => 0, max_version => 7},
            #{api_key => ?HEARTBEAT, min_version => 0, max_version => 4},
            #{api_key => ?LEAVE_GROUP, min_version => 0, max_version => 4},
            #{api_key => ?SYNC_GROUP, min_version => 0, max_version => 5},
            #{api_key => ?API_VERSIONS, min_version => 0, max_version => 3}
        ]
    }.
