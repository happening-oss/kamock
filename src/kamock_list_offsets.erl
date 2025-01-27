-module(kamock_list_offsets).
-export([handle_list_offsets_request/2]).

handle_list_offsets_request(
    _ListOffsetsRequest = #{correlation_id := CorrelationId, topics := Topics},
    Env
) ->
    #{
        correlation_id => CorrelationId,
        topics => lists:map(
            fun(Topic) ->
                make_list_offsets_topic_response(Topic, Env)
            end,
            Topics
        ),
        throttle_time_ms => 0
    }.

make_list_offsets_topic_response(#{name := TopicName, partitions := Partitions}, Env) ->
    #{
        name => TopicName,
        partitions => lists:map(
            fun(Partition) ->
                kamock_list_offsets_partition_response:make_list_offsets_partition_response(
                    TopicName, Partition, Env
                )
            end,
            Partitions
        )
    }.
