-module(kamock_sync_group_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun return_error/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

return_error() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,
    MemberId = <<"some_member_id">>,

    meck:expect(
        kamock_sync_group, handle_sync_group_request, kamock_sync_group:return_error(12345)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    Request = make_request(GroupId, MemberId, []),
    ?assertEqual(
        {ok, #{
            error_code => 12345,
            throttle_time_ms => 0,
            assignment => <<>>,
            protocol_type => null,
            protocol_name => null
        }},
        kafcod_connection:call(
            Connection,
            fun sync_group_request:encode_sync_group_request_5/1,
            Request,
            fun sync_group_response:decode_sync_group_response_5/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

make_request(GroupId, MemberId, Assignments) ->
    #{
        group_id => GroupId,
        member_id => MemberId,
        group_instance_id => null,
        generation_id => 0,
        protocol_type => <<"consumer">>,
        protocol_name => <<"range">>,
        assignments => Assignments
    }.
