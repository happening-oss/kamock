-module(kamock_heartbeat).
-export([handle_heartbeat_request/2]).
-export([expect_generation_id/1]).

-include_lib("kafcod/include/error_code.hrl").

handle_heartbeat_request(_HeartbeatRequest = #{correlation_id := CorrelationId}, _Env) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE
    }.

expect_generation_id(GenerationId) ->
    fun
        (_HeartbeatRequest = #{correlation_id := CorrelationId, generation_id := G}, _Env) when
            G /= GenerationId
        ->
            #{
                correlation_id => CorrelationId,
                throttle_time_ms => 0,
                error_code => ?REBALANCE_IN_PROGRESS
            };
        (_HeartbeatRequest = #{correlation_id := CorrelationId}, _End) ->
            #{
                correlation_id => CorrelationId,
                throttle_time_ms => 0,
                error_code => ?NONE
            }
    end.
