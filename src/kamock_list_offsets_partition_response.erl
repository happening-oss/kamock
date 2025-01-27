-module(kamock_list_offsets_partition_response).
-export([make_list_offsets_partition_response/3]).
-export([
    range/2,
    from_ets/1
]).

-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/timestamp.hrl").

make_list_offsets_partition_response(
    _TopicName,
    _Partition = #{
        partition_index := PartitionIndex,
        current_leader_epoch := LeaderEpoch,
        timestamp := _
    },
    _Env
) ->
    #{
        partition_index => PartitionIndex,
        error_code => ?NONE,
        % Always -1.
        timestamp => -1,
        offset => 0,
        leader_epoch => LeaderEpoch
    }.

range(FirstOffset, LastOffset) when FirstOffset =< LastOffset ->
    fun(
        _TopicName,
        #{
            partition_index := PartitionIndex,
            current_leader_epoch := LeaderEpoch,
            timestamp := Timestamp
        },
        _Env
    ) ->
        Offset =
            case Timestamp of
                ?EARLIEST_TIMESTAMP -> FirstOffset;
                ?LATEST_TIMESTAMP -> LastOffset
            end,
        #{
            partition_index => PartitionIndex,
            error_code => ?NONE,
            % Always -1.
            timestamp => -1,
            offset => Offset,
            leader_epoch => LeaderEpoch
        }
    end;
range(FirstOffset, LastOffset) ->
    error(badarg, [FirstOffset, LastOffset]).

% This is _part_ of how you might keep the latest offset in ETS. You'll need something in (or to mock)
% kmock_partition_data as well.
from_ets(Table) ->
    fun(
        TopicName,
        #{
            partition_index := PartitionIndex,
            current_leader_epoch := LeaderEpoch,
            timestamp := Timestamp
        },
        _Env
    ) ->
        {FirstOffset, LastOffset} = from_ets(Table, TopicName, PartitionIndex),

        Offset =
            case Timestamp of
                ?EARLIEST_TIMESTAMP -> FirstOffset;
                ?LATEST_TIMESTAMP -> LastOffset
            end,
        #{
            partition_index => PartitionIndex,
            error_code => ?NONE,
            % Always -1.
            timestamp => -1,
            offset => Offset,
            leader_epoch => LeaderEpoch
        }
    end.

from_ets(Table, TopicName, PartitionIndex) ->
    case ets:lookup(Table, {TopicName, PartitionIndex}) of
        [] -> {0, 0};
        [{FirstOffset, LastOffset}] -> {FirstOffset, LastOffset}
    end.
