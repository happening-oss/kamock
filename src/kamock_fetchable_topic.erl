-module(kamock_fetchable_topic).
-export([make_fetchable_topic_response/2]).

make_fetchable_topic_response(#{topic := Topic, partitions := FetchPartitions}, Env) ->
    #{
        topic => Topic,
        partitions => lists:map(
            fun(FetchPartition) ->
                kamock_partition_data:make_partition_data(Topic, FetchPartition, Env)
            end,
            FetchPartitions
        )
    }.
