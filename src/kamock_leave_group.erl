-module(kamock_leave_group).
-export([handle_leave_group_request/2]).

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
