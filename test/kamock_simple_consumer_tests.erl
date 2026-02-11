-module(kamock_simple_consumer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TABLE, binary_to_atom(iolist_to_binary(io_lib:format("~s_~s", [?MODULE, ?FUNCTION_NAME])))).

all_test_() ->
    {setup, fun setup/0, fun cleanup/1, {with, [fun start_stop/1]}}.

setup() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Table = ets:new(?TABLE, [public, ordered_set]),
    meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)),
    meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)),
    meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)),
    Broker.

cleanup(Broker) ->
    kamock_broker:stop(Broker),
    meck:unload(),
    ok.

start_stop(Broker) ->
    {ok, C} = kamock_simple_consumer:start_link(Broker, <<"topic">>, [0], fun(_T, _P, _R) -> ok end),
    MRef = monitor(process, C),
    kamock_simple_consumer:stop(C),
    receive
        {'DOWN', MRef, process, C, _} ->
            ok
    end,
    ok.
