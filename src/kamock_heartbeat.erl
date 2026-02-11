-module(kamock_heartbeat).
-export([handle_heartbeat_request/2]).
-export([
    expect_generation_id/1,
    return_error/1
]).
-export([
    make_heartbeat_response/2,
    make_heartbeat_response/1
]).

-include_lib("kafcod/include/error_code.hrl").

handle_heartbeat_request(_HeartbeatRequest = #{correlation_id := CorrelationId}, _Env) ->
    make_heartbeat_response(CorrelationId).

expect_generation_id(GenerationId) ->
    fun
        (_HeartbeatRequest = #{correlation_id := CorrelationId, generation_id := G}, _Env) when
            G /= GenerationId
        ->
            make_heartbeat_response(CorrelationId, ?REBALANCE_IN_PROGRESS);
        (_HeartbeatRequest = #{correlation_id := CorrelationId}, _End) ->
            make_heartbeat_response(CorrelationId)
    end.

return_error(ErrorCode) ->
    fun
        (_HeartbeatRequest = #{correlation_id := CorrelationId}, _Env) ->
            make_heartbeat_response(CorrelationId, ErrorCode)
    end.

make_heartbeat_response(CorrelationId, ErrorCode) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ErrorCode
    }.

make_heartbeat_response(CorrelationId) ->
    make_heartbeat_response(CorrelationId, ?NONE).
