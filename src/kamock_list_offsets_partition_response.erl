-module(kamock_list_offsets_partition_response).
-export([make_list_offsets_partition_response/3]).
-export([
    empty/0,
    range/2
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

empty() ->
    range(0, 0).

%% FirstOffset is the offset of the first message; LastOffset is the offset of the *next* message.
%% For an empty partition, FirstOffset == LastOffset; for a single message, FirstOffset + 1 == LastOffset.
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
