-module(kamock_offset_fetch_response_topic).
-export([make_offset_fetch_response_topic/2]).

make_offset_fetch_response_topic(
    _Topic = #{name := TopicName, partition_indexes := PartitionIndexes}, Env
) ->
    #{
        name => TopicName,
        partitions => lists:map(
            fun(PartitionIndex) ->
                kamock_offset_fetch_response_partition:make_offset_fetch_response_partition(
                    TopicName, PartitionIndex, Env
                )
            end,
            PartitionIndexes
        )
    }.
