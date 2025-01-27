-module(kamock_fetch).
-export([handle_fetch_request/2]).

-include_lib("kafcod/include/error_code.hrl").

handle_fetch_request(
    _FetchRequest = #{
        correlation_id := CorrelationId,
        max_wait_ms := MaxWaitMs,
        session_id := SessionId,
        topics := FetchTopics
    },
    Env = #{}
) ->
    % Sleep for a bit, so that we're not continually spamming; use MaxWaitMs as a hint.
    timer:sleep(MaxWaitMs div 10),

    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        responses => lists:map(
            fun(Topic) ->
                kamock_fetchable_topic:make_fetchable_topic_response(Topic, Env)
            end,
            FetchTopics
        ),
        session_id => SessionId,
        throttle_time_ms => 0
    }.
