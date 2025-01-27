-module(kamock_find_coordinator).
-export([handle_find_coordinator_request/2]).

-export([
    make_find_coordinator_response/2,
    make_find_coordinator_error/3
]).

-include_lib("kafcod/include/error_code.hrl").

handle_find_coordinator_request(
    _FindCoordinatorRequest = #{correlation_id := CorrelationId},
    Env
) ->
    Coordinator = maps:with([node_id, host, port], Env),
    make_find_coordinator_response(CorrelationId, Coordinator).

make_find_coordinator_response(
    CorrelationId, _Coordinator = #{node_id := NodeId, host := Host, port := Port}
) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE,
        error_message => null,
        node_id => NodeId,
        host => Host,
        port => Port
    }.

make_find_coordinator_error(CorrelationId, ErrorCode, ErrorMessage) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ErrorCode,
        error_message => ErrorMessage,
        node_id => -1,
        host => <<>>,
        port => -1
    }.
