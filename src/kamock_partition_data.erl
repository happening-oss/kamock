-module(kamock_partition_data).
-export([make_partition_data/3]).

% Creators
-export([
    empty/0,
    single/1,
    repeat/1,
    range/3,
    batches/4,

    % Use this for more complicated scenarios, such as multiple record batches in a single partition data response.
    bounded/3
]).

% Helpers
-export([
    make_single_message/5,
    make_empty/1,
    make_empty/3,
    make_error/2
]).

-export_type([
    message/0,
    message_builder_fun/0,
    partition_data_fun/0,
    batch_locator_fun/0
]).

-include_lib("kafcod/include/error_code.hrl").
-include("kamock.hrl").

-type headers() :: [{Key :: binary(), Value :: binary() | null}].
-type message() :: #{
    timestamp => non_neg_integer(),
    key => binary() | null,
    value => binary() | null,
    headers => headers()
}.
-type message_builder_fun() :: fun(
    (Topic :: binary(), PartitionIndex :: non_neg_integer(), FetchOffset :: non_neg_integer()) ->
        message()
).

?DOC("""
Given the `FirstOffset` and `LastOffset` values for a particular partition, and the `FetchOffset` from the fetch
request, a batch locator function should return the offset and size of the relevant batch.

See `fixed_size_batch_locator` for the default implementation, which rounds down the requested offset to the start of
the batch and returns a fixed batch size.

Note that your batch locator function should probably be deterministic, and should probably not return overlapping
batches. This is for least-surprise in your tests, not because it breaks anything.
""").
-type batch_locator_fun() :: fun(
    (
        FirstOffset :: non_neg_integer(),
        LastOffset :: non_neg_integer(),
        FetchOffset :: non_neg_integer()
    ) -> {BatchOffset :: non_neg_integer(), BatchSize :: non_neg_integer()}
).

make_partition_data(Topic, FetchPartition, Env) ->
    % By default, pretend that the partition is empty.
    empty_partition(Topic, FetchPartition, Env).

?DOC("""
Return type used by `empty`, `repeat`, `range` and `batches`.
""").
-type partition_data_fun() :: fun(
    (
        Topic :: binary(),
        FetchPartition :: #{partition := non_neg_integer(), fetch_offset := non_neg_integer()},
        Env :: map()
    ) -> fetch_response:partition_data_11()
).

empty_partition(
    _Topic,
    _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
    _Env
) when FetchOffset == 0 ->
    make_empty(PartitionIndex);
empty_partition(
    _Topic,
    _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
    _Env
) when FetchOffset /= 0 ->
    make_error(PartitionIndex, ?OFFSET_OUT_OF_RANGE).

?DOC("""
Pretend to be an empty partition, starting at offset zero.

If you want an empty partition at any other offset, use `range` with `FirstOffset` equal to `LastOffset`.
""").
empty() ->
    fun empty_partition/3.

?DOC("""
Pretend to be a partition with a single message at offset zero.
""").
-spec single(Message :: message() | message_builder_fun()) -> partition_data_fun().

single(MessageOrBuilder) ->
    range(0, 1, MessageOrBuilder).

-spec repeat(Message :: message() | message_builder_fun()) -> partition_data_fun().

?DOC("""
Starting at offset zero, return a fixed message (or call the message builder) for every message.

Messages are returned as singletons.

The partition has infinite length. This is intended to simulate the continuous production of messages as you're
fetching.
""").
repeat(MessageBuilder) when is_function(MessageBuilder, 3) ->
    FirstOffset = 0,
    PartitionDataBuilder = fun(Topic, PartitionIndex, FetchOffset) ->
        % There's always one more message.
        LastOffset = FetchOffset + 1,
        Message = MessageBuilder(Topic, PartitionIndex, FetchOffset),
        make_single_message(PartitionIndex, FetchOffset, FirstOffset, LastOffset, Message)
    end,
    % Note that we don't need to special-case 'infinity', because number() < atom().
    bounded(FirstOffset, infinity, PartitionDataBuilder);
repeat(Message) when is_map(Message) ->
    repeat(fun(_T, _P, _O) -> Message end);
repeat(Message) ->
    error(badarg, [Message]).

-spec range(
    FirstOffset :: non_neg_integer(),
    LastOffset :: non_neg_integer(),
    Message :: message() | message_builder_fun()
) -> partition_data_fun().

range(FirstOffset, LastOffset, MessageBuilder) when
    is_integer(FirstOffset),
    is_integer(LastOffset),
    FirstOffset =< LastOffset,
    is_function(MessageBuilder, 3)
->
    PartitionDataBuilder = fun(Topic, PartitionIndex, FetchOffset) ->
        Message = MessageBuilder(Topic, PartitionIndex, FetchOffset),
        make_single_message(PartitionIndex, FetchOffset, FirstOffset, LastOffset, Message)
    end,
    bounded(FirstOffset, LastOffset, PartitionDataBuilder);
range(FirstOffset, LastOffset, Message) when is_map(Message) ->
    range(FirstOffset, LastOffset, fun(_T, _P, _O) -> Message end);
range(FirstOffset, LastOffset, M) ->
    error(badarg, [FirstOffset, LastOffset, M]).

-spec batches
    (
        FirstOffset :: non_neg_integer(),
        LastOffset :: non_neg_integer(),
        BatchSize :: non_neg_integer(),
        Message :: message() | message_builder_fun()
    ) -> partition_data_fun();
    (
        FirstOffset :: non_neg_integer(),
        LastOffset :: non_neg_integer(),
        BatchLocator :: batch_locator_fun(),
        Message :: message() | message_builder_fun()
    ) -> partition_data_fun().

?DOC("""
Returns a partition data containing a batch with multiple messages, as if we'd produced all the messages at once.

A real broker always returns an entire batch, which means that it may return messages *before* the requested fetch
offset. We replicate this behaviour here, by having the fixed-size batch locator round down to the start of the batch.

Use as (e.g.) `batches(0, 20, 4, MessageBuilder)` to get a partition from offset 0, containing 5 batches, each with 4
messages. Ranges are exclusive, so the messages are numbered from offset 0 to offset 19, with 20 being the *next*
offset.

If you want more control over the batch size, you can pass a custom `BatchLocator` function.

For a single partition response containing multiple batches, use `bounded/3` with a `PartitionDataBuilder` function.
""").
batches(FirstOffset, LastOffset, BatchSize, MessageBuilder) when
    is_integer(FirstOffset),
    is_integer(LastOffset),
    FirstOffset =< LastOffset,
    is_integer(BatchSize),
    is_function(MessageBuilder, 3)
->
    batches(FirstOffset, LastOffset, kamock_batch_locator:fixed_size(BatchSize), MessageBuilder);
batches(FirstOffset, LastOffset, BatchLocator, MessageBuilder) when
    is_integer(FirstOffset),
    is_integer(LastOffset),
    FirstOffset =< LastOffset,
    is_function(BatchLocator, 3),
    is_function(MessageBuilder, 3)
->
    PartitionDataBuilder = fun(Topic, PartitionIndex, FetchOffset) ->
        make_batches(
            Topic,
            PartitionIndex,
            FirstOffset,
            LastOffset,
            FetchOffset,
            BatchLocator,
            MessageBuilder
        )
    end,
    bounded(FirstOffset, LastOffset, PartitionDataBuilder);
batches(FirstOffset, LastOffset, B, Message) when is_map(Message) ->
    batches(FirstOffset, LastOffset, B, fun(_T, _P, _O) -> Message end);
batches(FirstOffset, LastOffset, B, M) ->
    error(badarg, [FirstOffset, LastOffset, B, M]).

-type partition_data_builder_fun() :: fun(
    (
        Topic :: binary(),
        Partition :: non_neg_integer(),
        FetchOffset :: non_neg_integer()
    ) -> fetch_response:partition_data_11()
).

-spec bounded(
    FirstOffset :: non_neg_integer(),
    LastOffset :: non_neg_integer() | infinity,
    PartitionDataBuilder :: partition_data_builder_fun()
) -> partition_data_fun().

bounded(FirstOffset, LastOffset, PartitionDataBuilder) when
    is_integer(FirstOffset),
    is_integer(LastOffset) orelse LastOffset =:= infinity,
    is_function(PartitionDataBuilder, 3)
->
    fun
        (
            Topic,
            _FetchPartition = #{partition := PartitionIndex, fetch_offset := FetchOffset},
            _Env
        ) when
            FetchOffset >= FirstOffset andalso FetchOffset < LastOffset
        ->
            PartitionDataBuilder(Topic, PartitionIndex, FetchOffset);
        (_Topic, #{partition := PartitionIndex, fetch_offset := FetchOffset}, _Env) when
            FetchOffset == LastOffset
        ->
            make_empty(PartitionIndex, FirstOffset, LastOffset);
        (_Topic, #{partition := PartitionIndex, fetch_offset := _FetchOffset}, _Env) ->
            make_error(PartitionIndex, ?OFFSET_OUT_OF_RANGE)
    end.

make_batches(
    Topic, PartitionIndex, FirstOffset, LastOffset, FetchOffset, BatchLocator, MessageBuilder
) ->
    % We always return an entire batch, so we need to figure out which batch this offset falls into.
    {BaseOffset, BatchSize} = clamp_batch_location(
        FirstOffset, LastOffset, BatchLocator(FirstOffset, LastOffset, FetchOffset)
    ),

    % Then we make the batch.
    LastOffsetDelta = BatchSize - 1,

    Records = kamock_partition_data_builder:make_records([
        MessageBuilder(Topic, PartitionIndex, BaseOffset + OffsetDelta)
     || OffsetDelta <- lists:seq(0, LastOffsetDelta)
    ]),

    Timestamp = erlang:system_time(millisecond),
    RecordBatch = kamock_partition_data_builder:make_record_batch(
        BaseOffset, Timestamp, Records
    ),
    RecordBatches = [RecordBatch],

    kamock_partition_data_builder:make_partition_data(
        PartitionIndex, FirstOffset, LastOffset, RecordBatches
    ).

clamp_batch_location(FirstOffset, LastOffset, {BatchOffset, BatchSize}) when
    BatchOffset >= FirstOffset
->
    % Don't go past the end of the partition.
    Remaining = LastOffset - BatchOffset,
    ResultSize = min(BatchSize, Remaining),
    {BatchOffset, ResultSize}.

make_empty(PartitionIndex) ->
    kamock_partition_data_builder:make_empty(PartitionIndex).

make_empty(PartitionIndex, FirstOffset, LastOffset) ->
    kamock_partition_data_builder:make_empty(PartitionIndex, FirstOffset, LastOffset).

make_error(PartitionIndex, ErrorCode) ->
    #{
        partition_index => PartitionIndex,
        error_code => ErrorCode,
        log_start_offset => -1,
        high_watermark => -1,
        last_stable_offset => -1,
        aborted_transactions => null,
        preferred_read_replica => -1,
        records => []
    }.

make_single_message(PartitionIndex, FetchOffset, FirstOffset, LastOffset, Message) ->
    kamock_partition_data_builder:make_single_message(
        PartitionIndex, FetchOffset, FirstOffset, LastOffset, Message
    ).
