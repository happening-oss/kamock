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

    wait_for_members/2,

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
    Assignment = find_assignment(MemberId, Assignments),
    make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName).

find_assignment(MemberId, Assignments) ->
    % The leader should pass us assignments containing at least itself; followers pass empty assignments.
    % Either way, if the member doesn't appear in the assignments, return <<>>.
    % Verified with a real broker and a slightly broken custom assignor.
    Pred = fun(#{member_id := M}) -> M == MemberId end,
    case lists:search(Pred, Assignments) of
        {value, #{assignment := A}} -> A;
        false -> <<>>
    end.

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

wait_for_members(LeaderId, FollowerIds) ->
    fun
        (SyncGroupRequest = #{member_id := MemberId, assignments := [_ | _]}, _Env) when
            MemberId =:= LeaderId
        ->
            wait_for_members_as_leader(SyncGroupRequest, LeaderId, FollowerIds);
        (SyncGroupRequest = #{assignments := []}, _Env) ->
            wait_for_members_as_follower(SyncGroupRequest, LeaderId)
    end.

-define(member_key(GroupId, MemberId), {n, l, {GroupId, MemberId}}).

wait_for_members_as_follower(
    _SyncGroupRequest = #{
        correlation_id := CorrelationId,
        group_id := GroupId,
        member_id := MemberId,
        protocol_type := ProtocolType,
        protocol_name := ProtocolName,
        assignments := []
    },
    _LeaderId
) ->
    % The leader is going to wait for all of the members to join, so we need to tell it we're here.
    gproc:ensure_reg(?member_key(GroupId, MemberId)),
    receive
        {assignment, Assignment} ->
            make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName)
    end.

wait_for_members_as_leader(SyncGroupRequest = #{group_id := GroupId}, LeaderId, FollowerIds) ->
    gproc:ensure_reg(?member_key(GroupId, LeaderId)),
    wait_for_members_as_leader(SyncGroupRequest, FollowerIds).

wait_for_members_as_leader(
    SyncGroupRequest = #{
        group_id := GroupId, assignments := Assignments
    },
    [FollowerId | FollowerIds]
) ->
    % Wait for the given follower to join.
    gproc:await(?member_key(GroupId, FollowerId)),
    Assignment = find_assignment(FollowerId, Assignments),
    gproc:send(?member_key(GroupId, FollowerId), {assignment, Assignment}),
    wait_for_members_as_leader(SyncGroupRequest, FollowerIds);
wait_for_members_as_leader(
    _SyncGroupRequest = #{
        correlation_id := CorrelationId,
        member_id := MemberId,
        protocol_type := ProtocolType,
        protocol_name := ProtocolName,
        assignments := Assignments
    },
    _FollowerIds = []
) ->
    % No more followers; we're done.
    Assignment = find_assignment(MemberId, Assignments),
    make_sync_group_response(CorrelationId, Assignment, ProtocolType, ProtocolName).

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
