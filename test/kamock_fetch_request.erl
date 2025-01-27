-module(kamock_fetch_request).
-export([build_fetch_request/2]).

-include_lib("kafcod/include/isolation_level.hrl").

build_fetch_request(Topic, Partitions) when is_binary(Topic), is_list(Partitions) ->
    Offset = 0,
    PartitionOffsets = lists:foldl(
        fun(Partition, Acc) -> Acc#{Partition => Offset} end, #{}, Partitions
    ),
    build_fetch_request(Topic, PartitionOffsets);
build_fetch_request(Topic, PartitionOffsets) when is_binary(Topic), is_map(PartitionOffsets) ->
    #{
        % We're a client, not a broker.
        replica_id => -1,

        max_wait_ms => 500,
        min_bytes => 0,
        max_bytes => 52_428_800,

        isolation_level => ?READ_COMMITTED,

        % Sessions and forgotten topics are for inter-broker replication.
        session_id => 0,
        session_epoch => -1,
        forgotten_topics_data => [],

        topics => [build_fetch_topic(Topic, PartitionOffsets)],
        rack_id => <<>>
    }.

build_fetch_topic(Topic, PartitionOffsets) ->
    ByPartitionIndex = fun(#{partition := A}, #{partition := B}) -> A =< B end,
    FetchPartitions = lists:sort(
        ByPartitionIndex,
        maps:fold(
            fun(PartitionIndex, Offset, Acc) ->
                [
                    #{
                        partition => PartitionIndex,

                        fetch_offset => Offset,

                        partition_max_bytes => 1_048_576,
                        log_start_offset => -1,

                        current_leader_epoch => -1
                    }
                    | Acc
                ]
            end,
            [],
            PartitionOffsets
        )
    ),
    #{
        topic => Topic,
        partitions => FetchPartitions
    }.
