-module(kamock_leave_group).
-export([handle_leave_group_request/2]).
-export([
    return_error/1,
    return_error/2
]).
-export([
    make_leave_group_response/3,
    make_leave_group_response/2,
    make_response_member/3,
    make_response_member/2
]).

-include_lib("kafcod/include/error_code.hrl").

handle_leave_group_request(
    _LeaveGroupRequest = #{
        correlation_id := CorrelationId,
        members := [#{member_id := MemberId, group_instance_id := GroupInstanceId}]
    },
    _Env
) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE,
        members => [
            #{
                member_id => MemberId,
                group_instance_id => GroupInstanceId,
                error_code => ?NONE
            }
        ]
    };
handle_leave_group_request(
    _LeaveGroupRequest = #{
        correlation_id := CorrelationId,
        group_id := _,
        member_id := _
    },
    _Env
) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE
    }.

return_error(ErrorCode) ->
    fun(
        _LeaveGroupRequest = #{
            correlation_id := CorrelationId
        },
        _Env
    ) ->
        make_leave_group_response(CorrelationId, ErrorCode)
    end.

return_error(ErrorCode, Members) ->
    fun(
        _LeaveGroupRequest = #{
            correlation_id := CorrelationId
        },
        _Env
    ) ->
        make_leave_group_response(CorrelationId, ErrorCode, Members)
    end.

make_leave_group_response(CorrelationId, ErrorCode, Members) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ErrorCode,
        members => Members
    }.

make_leave_group_response(CorrelationId, ErrorCode) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ErrorCode
    }.

make_response_member(MemberId, GroupInstanceId, ErrorCode) ->
    #{
        member_id => MemberId,
        group_instance_id => GroupInstanceId,
        error_code => ErrorCode
    }.

make_response_member(MemberId, ErrorCode) ->
    make_response_member(MemberId, null, ErrorCode).
