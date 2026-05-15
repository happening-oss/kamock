-module(kamock_partition_data_builder).
-include_lib("kafcod/include/error_code.hrl").

-export([
    make_empty/1,
    make_empty/3,

    make_single_message/5,

    make_records/1,
    make_record/2,
    make_record_batch/3,
    make_record_batch/4,
    make_partition_data/4
]).

make_empty(PartitionIndex) ->
    make_empty(PartitionIndex, 0, 0).

make_empty(PartitionIndex, FirstOffset, LastOffset) ->
    make_partition_data(PartitionIndex, FirstOffset, LastOffset, []).

make_single_message(PartitionIndex, FetchOffset, FirstOffset, LastOffset, Message) ->
    BaseOffset = FetchOffset,
    LastOffsetDelta = 0,
    Timestamp = erlang:system_time(millisecond),

    Records = make_records([Message]),
    RecordBatch = make_record_batch(BaseOffset, LastOffsetDelta, Timestamp, Records),
    RecordBatches = [RecordBatch],

    make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches).

make_records(Messages) when is_list(Messages) ->
    [
        make_record(OffsetDelta, Message)
     || {OffsetDelta, Message} <- lists:enumerate(0, Messages)
    ].

make_record(OffsetDelta, Message) ->
    #{
        attributes => 0,
        key => maps:get(key, Message, null),
        value => maps:get(value, Message, null),
        headers => maps:get(headers, Message, []),
        offset_delta => OffsetDelta,
        timestamp_delta => 0
    }.

make_record_batch(BaseOffset, BaseTimestamp, Records) ->
    #{offset_delta := LastOffsetDelta} = lists:last(Records),
    make_record_batch(BaseOffset, LastOffsetDelta, BaseTimestamp, Records).

make_record_batch(BaseOffset, LastOffsetDelta, BaseTimestamp, Records) ->
    MaxTimestamp = BaseTimestamp + get_max_timestamp_delta(Records),
    #{
        base_offset => BaseOffset,
        partition_leader_epoch => 0,
        magic => 2,
        crc => -1,
        attributes => #{compression => none},
        last_offset_delta => LastOffsetDelta,
        base_timestamp => BaseTimestamp,
        max_timestamp => MaxTimestamp,
        producer_id => -1,
        producer_epoch => -1,
        base_sequence => -1,
        records => Records
    }.

get_max_timestamp_delta(Records) ->
    lists:foldl(
        fun
            (#{timestamp_delta := TimestampDelta}, Max) when TimestampDelta > Max ->
                TimestampDelta;
            (_, Max) ->
                Max
        end,
        0,
        Records
    ).

make_partition_data(PartitionIndex, FirstOffset, LastOffset, RecordBatches) when
    is_integer(PartitionIndex),
    PartitionIndex >= 0,
    is_integer(FirstOffset),
    is_integer(LastOffset),
    LastOffset >= FirstOffset,
    is_list(RecordBatches)
->
    #{
        partition_index => PartitionIndex,
        error_code => ?NONE,
        log_start_offset => FirstOffset,
        high_watermark => LastOffset,
        last_stable_offset => LastOffset,
        aborted_transactions => [],
        preferred_read_replica => -1,
        % Here, 'records' is actually 'record batches'.
        records => RecordBatches
    }.
