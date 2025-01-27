-module(kamock_offset_commit).
-export([handle_offset_commit_request/2]).
-export([to_ets/1, insert/5]).

-include_lib("kafcod/include/error_code.hrl").

handle_offset_commit_request(
    _OffsetCommitRequest = #{correlation_id := CorrelationId, topics := Topics}, _Env
) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        topics => [
            #{
                name => Topic,
                partitions => [
                    #{partition_index => PartitionIndex, error_code => ?NONE}
                 || #{partition_index := PartitionIndex} <- Partitions
                ]
            }
         || #{name := Topic, partitions := Partitions} <- Topics
        ]
    }.

to_ets(Table) ->
    fun(_Req = #{correlation_id := CorrelationId, group_id := GroupId, topics := Topics}, _Env) ->
        #{
            correlation_id => CorrelationId,
            throttle_time_ms => 0,
            topics => lists:foldl(
                fun(#{name := Topic, partitions := Partitions}, AccT) ->
                    [
                        #{
                            name => Topic,
                            partitions => lists:foldl(
                                fun(
                                    #{
                                        partition_index := PartitionIndex,
                                        committed_offset := CommittedOffset,
                                        committed_metadata := CommittedMetadata
                                    },
                                    AccP
                                ) ->
                                    ets:insert(Table, {
                                        {GroupId, Topic, PartitionIndex},
                                        {CommittedOffset, CommittedMetadata}
                                    }),

                                    [
                                        #{
                                            partition_index => PartitionIndex,
                                            error_code => ?NONE
                                        }
                                        | AccP
                                    ]
                                end,
                                [],
                                Partitions
                            )
                        }
                        | AccT
                    ]
                end,
                [],
                Topics
            )
        }
    end.

insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset) ->
    insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset, <<>>).

insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset, CommittedMetadata) ->
    ets:insert(Table, {{GroupId, Topic, PartitionIndex}, {CommittedOffset, CommittedMetadata}}).
