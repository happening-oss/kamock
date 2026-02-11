-module(kamock_closed_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/api_key.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).

closed_test() ->
    % Returning stop from the handler should close the connection.
    % It should NOT be excessively noisy in the logs.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:new(kamock_broker_handler, [passthrough]),
    meck:expect(kamock_broker_handler, handle_request, [
        {[?API_VERSIONS, '_', '_', '_'], stop},
        {['_', '_', '_', '_'], meck:passthrough()}
    ]),

    ?assertExit(
        _,
        kafcod_connection:call(
            C,
            fun api_versions_request:encode_api_versions_request_1/1,
            #{},
            fun api_versions_response:decode_api_versions_response_1/1
        )
    ),

    kamock_broker:stop(Broker),
    ok.

closed2_test() ->
    % Returning 'stop' from the message handler (i.e. one level deeper) should be the same as above.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(kamock_api_versions, handle_api_versions_request, fun(_Req, _Env) -> stop end),

    ?assertExit(
        _,
        kafcod_connection:call(
            C,
            fun api_versions_request:encode_api_versions_request_1/1,
            #{},
            fun api_versions_response:decode_api_versions_response_1/1
        )
    ),

    kamock_broker:stop(Broker),
    ok.

noreply_test() ->
    % Returning 'stop' from the message handler (i.e. one level deeper) should be the same as above.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(kamock_api_versions, handle_api_versions_request, fun(_Req, _Env) -> noreply end),

    ?assertExit(
        _,
        kafcod_connection:call(
            C,
            fun api_versions_request:encode_api_versions_request_1/1,
            #{},
            fun api_versions_response:decode_api_versions_response_1/1
        )
    ),

    kamock_broker:stop(Broker),
    ok.
