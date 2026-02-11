-module(kamock_sync_group).
-export([handle_sync_group_request/2]).
-export([
    make_sync_group_response/4,
    make_sync_group_response/5,
    make_sync_group_error/2
]).
-export([
    assign/1,
    assign/2,

    return_error/1
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
) ->
    % The leader should pass us assignments containing at least itself; followers pass empty assignments.
    % Either way, if the member doesn't appear in the assignments, return <<>>.
    % Verified with a real broker and a slightly broken custom assignor.
    Assignment =
        case lists:search(fun(#{member_id := M}) -> M == MemberId end, Assignments) of
            {value, #{assignment := A}} -> A;
            false -> <<>>
        end,

    make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName).

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
        make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName)
    end.

return_error(ErrorCode) ->
    fun(#{correlation_id := CorrelationId}, _Env) ->
        make_sync_group_error(CorrelationId, ErrorCode)
    end.

make_sync_group_response(
    CorrelationId, ErrorCode, Assignment, ProtocolType, ProtocolName
) ->
    #{
        correlation_id => CorrelationId,
        error_code => ErrorCode,
        throttle_time_ms => 0,

        assignment => Assignment,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName
    }.

make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName) ->
    make_sync_group_response(
        CorrelationId,
        ?NONE,
        Assignment,
        ProtocolType,
        ProtocolName
    ).

make_sync_group_error(CorrelationId, ErrorCode) ->
    make_sync_group_response(
        CorrelationId,
        ErrorCode,
        <<>>,
        null,
        null
    ).
