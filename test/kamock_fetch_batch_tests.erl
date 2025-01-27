-module(kamock_fetch_batch_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).
-define(TOPIC_NAME, iolist_to_binary(io_lib:format("~s___~s", [?MODULE, ?FUNCTION_NAME]))).

% Tests for partition data containing a single batch with multiple messages, as if we'd produced all the messages at
% once.
%
% This is distinct from a single partition response containing multiple batches, which we don't support yet.
all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun fetch_batch_at_zero/0,
        fun fetch_batch_mid_batch/0,
        fun fetch_batch_at_boundary/0,
        fun fetch_batch_at_end/0,

        fun fetch_batch_not_multiple_mid_last_batch/0,
        fun fetch_batch_not_multiple_at_end/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

message_builder(_Topic, Partition, Offset) ->
    Key = iolist_to_binary(io_lib:format("key-~B-~B", [Partition, Offset])),
    Value = iolist_to_binary(io_lib:format("value-~B-~B", [Partition, Offset])),
    #{key => Key, value => Value}.

fetch_batch_at_zero() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 9, 3, fun message_builder/3)
    ),

    % Issue a fetch request with offset zero, we should get a single batch.
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 0}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 9,
        high_watermark := 9,
        records := [
            #{
                base_offset := 0,
                records := Records
            }
        ]
    } = PartitionResponse,
    [#{key := <<"key-0-0">>}, #{key := <<"key-0-1">>}, #{key := <<"key-0-2">>}] = Records,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

fetch_batch_mid_batch() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 9, 3, fun message_builder/3)
    ),

    % Issue a fetch request with an offset that falls within a batch, we should get that batch (but it'll include earlier records as well).
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 4}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 9,
        high_watermark := 9,
        records := [
            #{
                base_offset := 3,
                records := Records
            }
        ]
    } = PartitionResponse,
    [#{key := <<"key-0-3">>}, #{key := <<"key-0-4">>}, #{key := <<"key-0-5">>}] = Records,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

fetch_batch_at_boundary() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 9, 3, fun message_builder/3)
    ),

    % Issue a fetch request with an offset that falls between two batches, we should get the following batch.
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 6}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 9,
        high_watermark := 9,
        records := [
            #{
                base_offset := 6,
                records := Records
            }
        ]
    } = PartitionResponse,
    [#{key := <<"key-0-6">>}, #{key := <<"key-0-7">>}, #{key := <<"key-0-8">>}] = Records,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

fetch_batch_at_end() ->
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 9, 3, fun message_builder/3)
    ),

    % Issue a fetch request with an offset that falls at the end of the topic, we should get an empty response.
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 9}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 9,
        high_watermark := 9,
        records := []
    } = PartitionResponse,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

fetch_batch_not_multiple_mid_last_batch() ->
    % What if the number of messages in the partition isn't a multiple of the batch size, and we ask for something in
    % the last batch?
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, fun message_builder/3)
    ),

    % Issue a fetch request with an offset that falls at the end of the topic, we should get an empty response.
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 13}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 14,
        high_watermark := 14,
        records := [
            #{
                base_offset := 12,
                records := Records
            }
        ]
    } = PartitionResponse,
    [#{key := <<"key-0-12">>}, #{key := <<"key-0-13">>}] = Records,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

fetch_batch_not_multiple_at_end() ->
    % What if the number of messages in the partition isn't a multiple of the batch size, and we ask for something in
    % the last batch?
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, C} = kafcod_connection:start_link(Broker),

    meck:expect(
        kamock_partition_data,
        make_partition_data,
        kamock_partition_data:batches(0, 14, 3, fun message_builder/3)
    ),

    % Issue a fetch request with an offset that falls at the end of the topic, we should get an empty response.
    Topic = ?TOPIC_NAME,
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, #{0 => 14}),

    {ok, #{
        error_code := ?NONE,
        responses := [#{partitions := [PartitionResponse], topic := Topic}]
    }} = kafcod_connection:call(
        C,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    #{
        partition_index := 0,
        error_code := ?NONE,
        log_start_offset := 0,
        last_stable_offset := 14,
        high_watermark := 14,
        records := []
    } = PartitionResponse,

    kafcod_connection:stop(C),
    kamock_broker:stop(Broker),
    ok.

direct_test_() ->
    MessageBuilder = fun(_T, _P, O) -> #{key => <<O>>} end,
    Batches = kamock_partition_data:batches(0, 14, 3, MessageBuilder),
    Flatten = fun
        (#{records := [#{base_offset := BaseOffset, records := Records}]}) ->
            [
                #{offset => BaseOffset + Delta, key => Key}
             || #{offset_delta := Delta, key := Key} <- Records
            ];
        (#{records := []}) ->
            []
    end,
    Test = fun(Offset) ->
        Flatten(
            Batches(
                <<"topic">>, #{partition => 0, fetch_offset => Offset}, #{}
            )
        )
    end,

    [
        % At the beginning, we should get a complete first batch.
        ?_assertEqual(
            [
                #{offset => 0, key => <<0>>},
                #{offset => 1, key => <<1>>},
                #{offset => 2, key => <<2>>}
            ],
            Test(0)
        ),

        % In the last batch, we should get a complete batch.
        ?_assertEqual(
            [
                #{offset => 12, key => <<12>>},
                #{offset => 13, key => <<13>>}
            ],
            Test(12)
        ),

        % In the last batch, we should get a complete batch.
        ?_assertEqual(
            [
                #{offset => 12, key => <<12>>},
                #{offset => 13, key => <<13>>}
            ],
            Test(13)
        ),

        % At the last offset, we should get an empty batch.
        ?_assertEqual(
            [],
            Test(14)
        )
    ].
