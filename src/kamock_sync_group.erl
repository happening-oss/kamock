-module(kamock_sync_group).
-export([handle_sync_group_request/2]).
-export([
    assign/1,
    assign/2
]).

-include_lib("kafcod/include/error_code.hrl").

handle_sync_group_request(
    _SyncGroupRequest = #{
        correlation_id := CorrelationId,
        member_id := MemberId,
        protocol_type := ProtocolType,
        protocol_name := ProtocolName,
        assignments := Assignments
    },
    _Env
) when Assignments /= [] ->
    {value, #{assignment := Assignment}} = lists:search(
        fun(#{member_id := M}) -> M == MemberId end, Assignments
    ),

    #{
        correlation_id => CorrelationId,
        error_code => ?NONE,
        throttle_time_ms => 0,

        assignment => Assignment,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName
    }.

assign(AssignedPartitions) ->
    UserData = <<>>,
    assign(AssignedPartitions, UserData).

assign(AssignedPartitions, UserData) ->
    fun(
        _SyncGroupRequest = #{
            correlation_id := CorrelationId,
            member_id := _MemberId,
            protocol_type := ProtocolType,
            protocol_name := ProtocolName
        },
        _Env
    ) ->
        Assignment = kafcod_consumer_protocol:encode_assignment(AssignedPartitions, UserData),
        #{
            correlation_id => CorrelationId,
            error_code => ?NONE,
            throttle_time_ms => 0,

            assignment => Assignment,
            protocol_type => ProtocolType,
            protocol_name => ProtocolName
        }
    end.
