-module(kamock_leave_group_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun return_error_1/0,
        fun return_error_2/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

return_error_1() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    MemberId = <<"some_member_id">>,

    meck:expect(
        kamock_leave_group, handle_leave_group_request, kamock_leave_group:return_error(12345)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    Request = make_request(GroupId, MemberId),
    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0
        }},
        kafcod_connection:call(
            Connection,
            fun leave_group_request:encode_leave_group_request_1/1,
            Request,
            fun leave_group_response:decode_leave_group_response_1/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

return_error_2() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    MemberId = <<"some_member_id">>,

    meck:expect(
        kamock_leave_group, handle_leave_group_request, kamock_leave_group:return_error(12345, [])
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    Request = make_request(GroupId, [make_member(MemberId)]),
    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0,
            members => []
        }},
        kafcod_connection:call(
            Connection,
            fun leave_group_request:encode_leave_group_request_4/1,
            Request,
            fun leave_group_response:decode_leave_group_response_4/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

make_request(GroupId, Members) when is_list(Members) ->
    #{
        group_id => GroupId,
        members => Members
    };
make_request(GroupId, MemberId) when is_binary(MemberId) ->
    #{
        group_id => GroupId,
        member_id => MemberId
    }.

make_member(MemberId) ->
    #{
        member_id => MemberId,
        group_instance_id => null
    }.
