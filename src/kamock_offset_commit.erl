-module(kamock_offset_commit).
-export([handle_offset_commit_request/2]).
-export([
    to_ets/1,
    insert/5,
    lookup/4
]).

-include_lib("stdlib/include/ms_transform.hrl").
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
            topics => lists:map(
                fun(Topic) ->
                    topic_to_ets(Table, GroupId, Topic)
                end,
                Topics
            )
        }
    end.

topic_to_ets(Table, GroupId, _Topic = #{name := Topic, partitions := Partitions}) ->
    #{
        name => Topic,
        partitions => lists:map(
            fun(Partition) ->
                partition_to_ets(Table, GroupId, Topic, Partition)
            end,
            Partitions
        )
    }.

partition_to_ets(
    Table,
    GroupId,
    Topic,
    #{
        partition_index := PartitionIndex,
        committed_offset := CommittedOffset,
        committed_metadata := CommittedMetadata
    }
) ->
    ets:insert(Table, {
        {GroupId, Topic, PartitionIndex},
        {CommittedOffset, CommittedMetadata}
    }),

    #{
        partition_index => PartitionIndex,
        error_code => ?NONE
    }.

insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset) ->
    insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset, <<>>).

insert(Table, GroupId, Topic, PartitionIndex, CommittedOffset, CommittedMetadata) ->
    ets:insert(Table, {{GroupId, Topic, PartitionIndex}, {CommittedOffset, CommittedMetadata}}).

lookup(Table, GroupId, Topic, PartitionIndex) ->
    MatchSpec = ets:fun2ms(fun({{G, T, P}, R}) when
        G =:= GroupId, T =:= Topic, P =:= PartitionIndex
    ->
        R
    end),
    [Match] = ets:select(Table, MatchSpec),
    Match.
