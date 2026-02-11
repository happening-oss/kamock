-module(kamock_find_coordinator_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/coordinator_type.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(GROUP_ID, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun return/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

return() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    GroupId = ?GROUP_ID,

    Host = <<"universe.com">>,
    Port = 9054,
    NodeId = 42,
    Coordinator = #{
        node_id => NodeId,
        host => Host,
        port => Port
    },
    meck:expect(
        kamock_find_coordinator,
        handle_find_coordinator_request,
        kamock_find_coordinator:return(Coordinator)
    ),

    {ok, Connection} = kafcod_connection:start_link(Broker),

    % We expect error 12345 back
    Request = make_request(GroupId),
    ?assertEqual(
        {ok, #{
            throttle_time_ms => 0,
            error_code => ?NONE,
            error_message => null,
            node_id => NodeId,
            host => Host,
            port => Port
        }},
        kafcod_connection:call(
            Connection,
            fun find_coordinator_request:encode_find_coordinator_request_3/1,
            Request,
            fun find_coordinator_response:decode_find_coordinator_response_3/1
        )
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.

make_request(GroupId) ->
    #{
        key_type => ?COORDINATOR_TYPE_GROUP,
        key => GroupId
    }.
