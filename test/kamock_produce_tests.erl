-module(kamock_produce_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

setup() ->
    ok.

cleanup(_) ->
    meck:unload(),
    ok.

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun produce_single_message/0,

        fun not_leader/0
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

    ?assertMatch(
        {ok, #{
            responses := [#{name := _, partition_responses := [#{index := 0, error_code := ?NONE}]}]
        }},
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        )
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

not_leader() ->
    % By default, kamock doesn't care which member of the cluster you produce to. This test demonstrates how to
    % implement partition leaders. There's a corresponding test in kamock_ets_tests.erl, for ETS-backed topics.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % You must produce to the partition leader, so that's the granularity here.
    meck:expect(
        kamock_partition_produce_response,
        make_partition_produce_response,
        [
            % We're not the leader for partition 0.
            {
                ['_', meck:is(fun(#{index := P}) -> P =:= 0 end), '_'],
                kamock_partition_produce_response:return_error(?NOT_LEADER_OR_FOLLOWER)
            },
            % We are the leader for the other partitions.
            {['_', '_', '_'], meck:passthrough()}
        ]
    ),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
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
                        index => 0,
                        records => Records
                    },
                    #{
                        index => 1,
                        records => Records
                    }
                ]
            }
        ]
    },

    ?assertMatch(
        {ok, #{
            responses := [
                #{
                    name := _,
                    partition_responses := [
                        #{index := 0, error_code := ?NOT_LEADER_OR_FOLLOWER},
                        #{index := 1, error_code := ?NONE}
                    ]
                }
            ]
        }},
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        )
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.
