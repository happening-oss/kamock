-module(kamock_produce_tests).
-include_lib("eunit/include/eunit.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

setup() ->
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun produce_single_message/0
    ]}.

produce_single_message() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
    PartitionIndex = 0,
    BatchAttributes = #{compression => none},
    Messages = [#{key => <<"key">>, value => <<"value">>, headers => []}],
    Records = kafcod_message_set:prepare_message_set(BatchAttributes, Messages),

    ProduceRequest = #{
        transactional_id => null,
        acks => -1,
        timeout_ms => 5_000,
        topic_data => [
            #{
                name => Topic,
                partition_data => [
                    #{
                        index => PartitionIndex,
                        records => Records
                    }
                ]
            }
        ]
    },

    {ok, #{
        responses := _
    }} =
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.
