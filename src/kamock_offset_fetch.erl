-module(kamock_offset_fetch).
-export([handle_offset_fetch_request/2]).
-export([from_ets/1]).

-include_lib("kafcod/include/error_code.hrl").

handle_offset_fetch_request(
    _OffsetFetchRequest = #{correlation_id := CorrelationId, topics := Topics}, Env
) ->
    % We only advertise support for up to v4. v8 allows for multiple groups, which we don't support yet.
    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,
        topics => lists:map(
            fun(Topic) ->
                kamock_offset_fetch_response_topic:make_offset_fetch_response_topic(Topic, Env)
            end,
            Topics
        )
    }.

from_ets(Table) ->
    fun(_Req = #{correlation_id := CorrelationId, group_id := GroupId, topics := Topics}, _Env) ->
        #{
            correlation_id => CorrelationId,
            error_code => ?NONE,
            throttle_time_ms => 0,
            topics => lists:map(
                fun(Topic) ->
                    topic_from_ets(Table, GroupId, Topic)
                end,
                Topics
            )
        }
    end.

topic_from_ets(Table, GroupId, #{name := TopicName, partition_indexes := PartitionIndexes}) ->
    #{
        name => TopicName,
        partitions => lists:map(
            fun(PartitionIndex) ->
                partition_from_ets(Table, GroupId, TopicName, PartitionIndex)
            end,
            PartitionIndexes
        )
    }.

partition_from_ets(Table, GroupId, TopicName, PartitionIndex) ->
    partition_from_ets_result(
        PartitionIndex, ets:lookup(Table, {GroupId, TopicName, PartitionIndex})
    ).

partition_from_ets_result(PartitionIndex, []) ->
    #{
        partition_index => PartitionIndex,
        committed_offset => -1,
        % 'metadata' is a nullable string, but a real broker returns an empty string for unknown/new commits, so we'll
        % do that.
        metadata => <<>>,
        error_code => ?NONE
    };
partition_from_ets_result(PartitionIndex, [{_, {CommittedOffset, CommittedMetadata}}]) ->
    #{
        partition_index => PartitionIndex,
        committed_offset => CommittedOffset,
        metadata => CommittedMetadata,
        error_code => ?NONE
    }.
