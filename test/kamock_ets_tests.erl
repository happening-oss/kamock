-module(kamock_ets_tests).
-include_lib("eunit/include/eunit.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/isolation_level.hrl").
-include_lib("kafcod/include/timestamp.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).
-define(TABLE, binary_to_atom(iolist_to_binary(io_lib:format("~s_~s", [?MODULE, ?FUNCTION_NAME])))).

%% kamock can run in an ETS-backed mode, so you can actually produce and fetch messages. We test that here.
all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun no_messages/0,
        fun no_messages_multiple_partitions/0,
        fun single_produce/0,
        fun two_produces_interleaved/0,
        fun two_produces_first/0,

        % Separate 'with', otherwise 'foreach' applies to them together, not each.
        {with, [fun produce_multiple/1]},
        {with, [fun insert_then_fetch/1]},
        {with, [fun not_leader/1]}
    ]}.

setup() ->
    Table = ets:new(?TABLE, [public, ordered_set]),
    meck:expect(kamock_produce, handle_produce_request, kamock_ets:produce_to_ets(Table)),
    meck:expect(kamock_fetch, handle_fetch_request, kamock_ets:fetch_from_ets(Table)),
    meck:expect(kamock_list_offsets, handle_list_offsets_request, kamock_ets:offsets_in_ets(Table)),
    Table.

cleanup(Table) ->
    meck:unload(),
    ets:delete(Table),
    ok.

no_messages() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
    PartitionIndex = 0,

    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

no_messages_multiple_partitions() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 0, 1 => 0, 2 => 0, 3 => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := FetchedPartitions}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    ByPartitionIndex = fun(#{partition_index := A}, #{partition_index := B}) -> A =< B end,
    ?assertMatch(
        [
            #{partition_index := 0, records := []},
            #{partition_index := 1, records := []},
            #{partition_index := 2, records := []},
            #{partition_index := 3, records := []}
        ],
        lists:sort(ByPartitionIndex, FetchedPartitions)
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

single_produce() ->
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

    % Fetching from the given topic and partition, at offset zero, should return a message.
    FetchRequest0 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := [_]}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest0,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % Fetching from a different partition should return no message.
    FetchRequest1 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex + 1 => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest1,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % Fetching from an entirely different topic should return no message.
    FetchRequest2 = kamock_fetch_request:build_fetch_request(<<"another-topic">>, #{
        PartitionIndex => 0
    }),
    {ok, #{responses := [#{topic := _, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest2,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % Fetching from the original topic and partition, at offset 1, should return no message.
    FetchRequest3 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 1}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest3,
        fun fetch_response:decode_fetch_response_11/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

two_produces_interleaved() ->
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

    % Fetching from the given topic and partition, at offset zero, should return a message.
    FetchRequest0 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := [_]}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest0,
        fun fetch_response:decode_fetch_response_11/1
    ),

    {ok, #{
        responses := _
    }} =
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        ),

    % Fetching from the original topic and partition, at offset 1, should return another message.
    FetchRequest1 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 1}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := [_]}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest1,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % Fetching from the original topic and partition, at offset 2, should return no message.
    FetchRequest2 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 2}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest2,
        fun fetch_response:decode_fetch_response_11/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

two_produces_first() ->
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

    {ok, #{
        responses := _
    }} =
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        ),

    % Fetching from the given topic and partition, at offset zero, should return both messages.
    FetchRequest0 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := [_, _]}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest0,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % Fetching from the original topic and partition, at offset 2, should return no message.
    FetchRequest2 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 2}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest2,
        fun fetch_response:decode_fetch_response_11/1
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

produce_multiple(Table) ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
    PartitionIndex0 = 0,
    PartitionIndex1 = 1,
    BatchAttributes = #{compression => none},
    Messages = [
        #{key => <<"key">>, value => <<"value">>, headers => []},
        #{key => <<"key">>, value => <<"value">>, headers => []},
        #{key => <<"key">>, value => <<"value">>, headers => []}
    ],
    [Records] = kafcod_message_set:prepare_message_set(BatchAttributes, Messages),

    ProduceRequest = #{
        transactional_id => null,
        acks => -1,
        timeout_ms => 5_000,
        topic_data => [
            #{
                name => Topic,
                partition_data => [
                    #{
                        index => PartitionIndex0,
                        records => [Records, Records, Records]
                    },
                    #{
                        index => PartitionIndex1,
                        records => [Records]
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

    ?assertEqual(9, kamock_ets:high_watermark(Table, Topic, PartitionIndex0)),
    ?assertEqual(3, kamock_ets:high_watermark(Table, Topic, PartitionIndex1)),

    % Fetching from the given topic and partition, at offset zero, should return all of the messages.
    FetchRequest0 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex0 => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := FetchedRecords0}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest0,
        fun fetch_response:decode_fetch_response_11/1
    ),

    ?assertEqual(3, length(FetchedRecords0)),
    lists:foreach(fun(#{records := Rs}) -> ?assertEqual(3, length(Rs)) end, FetchedRecords0),

    % Check to see that 'lookup' works.
    ?assertEqual(9, length(kamock_ets:lookup(Table, Topic, PartitionIndex0))),
    ?assertEqual(3, length(kamock_ets:lookup(Table, Topic, PartitionIndex1))),

    % 'lookup' without a partition returns a map of P => Msgs
    ?assertMatch(
        #{
            PartitionIndex0 := [#{offset := 0}, _, _, _, _, _, _, _, #{offset := 8}],
            PartitionIndex1 := [_, _, _]
        },
        kamock_ets:lookup(Table, Topic)
    ),

    % Fetching from offset 4 should return the (3, 4, 5) and (6, 7, 8) batches.
    FetchRequest4 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex0 => 4}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := FetchedRecords4}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest4,
        fun fetch_response:decode_fetch_response_11/1
    ),

    ?assertEqual(2, length(FetchedRecords4)),

    % Fetching from the original topic and partition, at offset 9, should return no message.
    FetchRequest9 = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex0 => 9}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := []}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest9,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % ListOffsets should return the correct information.
    {ok, #{topics := [#{name := Topic, partitions := PartitionOffsets}]}} = kafcod_connection:call(
        C,
        fun list_offsets_request:encode_list_offsets_request_2/1,
        #{
            topics => [
                #{
                    name => Topic,
                    partitions => [
                        #{partition_index => P, timestamp => ?LATEST_TIMESTAMP}
                     || P <- [0, 1, 2, 3]
                    ]
                }
            ],
            isolation_level => ?READ_COMMITTED,
            replica_id => -1
        },
        fun list_offsets_response:decode_list_offsets_response_2/1
    ),

    ByPartitionIndex = fun(#{partition_index := A}, #{partition_index := B}) -> A =< B end,
    ?assertMatch(
        [
            % the first partition has 9 messages (0-8), so high watermark is 9.
            #{partition_index := 0, offset := 9},
            % the second partition has 3 messages (0, 1, 2), so high watermark is 3.
            #{partition_index := 1, offset := 3},
            % the other partitions have no messages.
            #{partition_index := 2, offset := 0},
            #{partition_index := 3, offset := 0}
        ],
        lists:sort(ByPartitionIndex, PartitionOffsets)
    ),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

insert_then_fetch(Table) ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    {ok, C} = kafcod_connection:start_link(Broker),

    Topic = ?TOPIC_NAME,
    PartitionIndex = 0,

    Message = #{key => <<"key1">>, value => <<"value1">>},
    kamock_ets:insert(Table, Topic, PartitionIndex, Message),

    Messages = [
        #{key => <<"key">>, value => <<"value">>},
        #{key => <<"key">>, value => <<"value">>},
        #{key => <<"key">>, value => <<"value">>}
    ],
    kamock_ets:insert(Table, Topic, PartitionIndex, Messages),

    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{PartitionIndex => 0}),
    {ok, #{responses := [#{topic := Topic, partitions := [#{records := Records}]}]}} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    ?assertEqual(4, count_records(Records)),

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

count_records(RecordBatches) ->
    lists:foldl(fun(#{records := Records}, Acc) -> length(Records) + Acc end, 0, RecordBatches).

not_leader(Table) ->
    % By default, kamock doesn't care which member of the cluster you produce to. This test demonstrates how to
    % implement partition leaders for ETS-backed topics. See kamock_produce_tests.erl for the non-ETS variant.
    {ok, Broker} = kamock_broker:start(?BROKER_REF),

    % Use a customised produce mock, rather than the simple ETS one.
    meck:unload(kamock_produce),

    % You must produce to the partition leader, so that's the granularity here.

    % There are two potential ways we could have implemented this:
    %
    % (1) use the default produce functionality and redirect to ETS at the partition stage;
    % (2) use the ETS produce functionality and mock kamock_ets:partition_to_ets/3 to inject errors.
    %
    % The second isn't currently supported (kamock_ets calls itself, so you can't inject mocks; it needs breaking up to
    % allow that).

    % We use (1) here.
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
            {
                ['_', '_', '_'],
                fun(Topic, PartitionProduceData, _Env) ->
                    kamock_ets:partition_to_ets(Table, Topic, PartitionProduceData)
                end
            }
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
