-module(kamock_simple_producer_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TABLE, binary_to_atom(iolist_to_binary(io_lib:format("~s_~s", [?MODULE, ?FUNCTION_NAME])))).

all_test_() ->
    {setup, fun setup/0, fun cleanup/1,
        {with, [
            fun produce_single_message/1,
            fun request_reply/1
        ]}}.

setup() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    Table = ets:new(?TABLE, [public, ordered_set]),
    meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)),
    meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)),
    meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)),
    #{broker => Broker, table => Table}.

cleanup(#{broker := Broker, table := Table}) ->
    kamock_broker:stop(Broker),
    meck:unload(),
    ets:delete(Table),
    ok.

produce_single_message(#{broker := Broker, table := Table}) ->
    Messages = [#{key => <<"key">>, value => <<"value">>, headers => []}],
    ok = kamock_simple_producer:produce(Broker, <<"topic">>, 0, Messages),
    ?assertMatch(
        [
            #{
                offset := 0,
                timestamp := _,
                value := <<"value">>,
                key := <<"key">>,
                headers := []
            }
        ],
        kamock_ets:lookup(Table, <<"topic">>, 0)
    ),
    ok.

request_reply(#{broker := Broker}) ->
    % Test the request-reply example. It simulates application A producing a request. Application B sees the request and
    % replies to it.
    ReplyFun = fun(_T, P, #{value := Value, key := Key, headers := Headers}) ->
        {_, ReplyTopic} = lists:keyfind(<<"reply-topic">>, 1, Headers),
        kamock_simple_producer:produce(Broker, ReplyTopic, P, #{
            key => Key, value => string:uppercase(Value), headers => Headers
        })
    end,
    {ok, B} = kamock_simple_consumer:start_link(Broker, <<"request">>, [0], ReplyFun),

    % Start a consumer for the reply.
    Self = self(),
    {ok, A} = kamock_simple_consumer:start_link(Broker, <<"reply">>, [0], fun(
        _T, _P, ReplyMessage
    ) ->
        Self ! ReplyMessage
    end),

    % Produce a request message.
    Key = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    kamock_simple_producer:produce(Broker, <<"request">>, 0, #{
        key => Key, value => <<"Hello World!">>, headers => [{<<"reply-topic">>, <<"reply">>}]
    }),

    % Wait until we get a reply.
    receive
        #{key := Key, value := <<"HELLO WORLD!">>, headers := _} ->
            ok
    end,

    kamock_simple_consumer:stop(A),
    kamock_simple_consumer:stop(B),
    ok.
