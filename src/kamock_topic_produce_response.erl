-module(kamock_topic_produce_response).
-export([make_topic_produce_response/2]).

make_topic_produce_response(
    _TopicProduceData = #{name := Topic, partition_data := PartitionData}, Env
) ->
    #{
        name => Topic,
        partition_responses => lists:map(
            fun(PartitionProduceData) ->
                kamock_partition_produce_response:make_partition_produce_response(
                    Topic, PartitionProduceData, Env
                )
            end,
            PartitionData
        )
    }.
