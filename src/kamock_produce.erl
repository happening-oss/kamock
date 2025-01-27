-module(kamock_produce).
-export([handle_produce_request/2]).

handle_produce_request(
    _ProduceRequest = #{correlation_id := CorrelationId, topic_data := TopicData}, Env
) ->
    #{
        correlation_id => CorrelationId,
        responses => lists:map(
            fun(TopicProduceData) ->
                kamock_topic_produce_response:make_topic_produce_response(TopicProduceData, Env)
            end,
            TopicData
        ),
        throttle_time_ms => 0
    }.
