-module(kamock_ets).
-export([
    produce_to_ets/1,
    fetch_from_ets/1,
    offsets_in_ets/1,

    high_watermark/3,
    insert/4,

    lookup/2,
    lookup/3
]).

-export([
    % Allow injecting per-partition errors, while still using ETS for everything else.
    partition_to_ets/3
]).

-include_lib("stdlib/include/ms_transform.hrl").
-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/timestamp.hrl").

produce_to_ets(Table) ->
    assert_ets(Table),

    fun(_ProduceRequest = #{correlation_id := CorrelationId, topic_data := TopicData}, _Env) ->
        #{
            correlation_id => CorrelationId,
            responses =>
                lists:map(
                    fun(_TopicProduceData = #{name := Topic, partition_data := PartitionData}) ->
                        topic_to_ets(Table, Topic, PartitionData)
                    end,
                    TopicData
                ),
            throttle_time_ms => 0
        }
    end.

-spec topic_to_ets(
    Table :: ets:table(),
    Topic :: binary(),
    PartitionData :: [produce_request:partition_produce_data_8()]
) -> produce_response:topic_produce_response_8().

topic_to_ets(Table, Topic, PartitionData) ->
    #{
        name => Topic,
        partition_responses => lists:map(
            fun(PartitionProduceData) ->
                partition_to_ets(Table, Topic, PartitionProduceData)
            end,
            PartitionData
        )
    }.

partition_to_ets(
    Table, Topic, _PartitionProduceData = #{index := PartitionIndex, records := RecordBatches}
) ->
    partition_to_ets(Table, Topic, PartitionIndex, RecordBatches).

-spec partition_to_ets(
    Table :: ets:table(),
    Topic :: binary(),
    PartitionIndex :: non_neg_integer(),
    % Note: non-empty; a real broker would return an error; we'll settle for a function_clause error.
    RecordBatches :: nonempty_list(kafcod_record_batch:record_batch())
) -> produce_response:partition_produce_response_8().

partition_to_ets(Table, Topic, PartitionIndex, RecordBatches = [_ | _]) ->
    Count = count_records(RecordBatches),
    {BaseOffset, _NextOffset} = update_offset(Table, Topic, PartitionIndex, Count),
    records_to_ets(Table, Topic, PartitionIndex, BaseOffset, RecordBatches),
    #{
        index => PartitionIndex,
        error_code => ?NONE,
        error_message => null,
        base_offset => BaseOffset,
        log_start_offset => 0,
        log_append_time_ms => 0,
        record_errors => []
    }.

count_records(RecordBatches) ->
    lists:foldl(
        fun(#{records := Records} = _RecordBatch, Count) ->
            Count + length(Records)
        end,
        0,
        RecordBatches
    ).

update_offset(Table, Topic, PartitionIndex, Count) ->
    NextOffset = update_counter(Table, {Topic, PartitionIndex}, Count),
    BaseOffset = NextOffset - Count,
    {BaseOffset, NextOffset}.

update_counter(Table, Key, Incr) ->
    % wrapped in a function (w/o spec) because eqwalizer doesn't spot that the return type depends on the input type.
    ets:update_counter(Table, Key, {2, Incr}, {Key, 0}).

records_to_ets(Table, Topic, PartitionIndex, InitialOffset, RecordBatches) ->
    lists:foldl(
        fun(#{records := Records} = RecordBatch, BaseOffset) ->
            RecordCount = length(Records),
            NextOffset = BaseOffset + RecordCount,
            RecordBatch2 = RecordBatch#{base_offset => BaseOffset},
            % We store NextOffset with the record batch; it makes it easier to filter by FetchOffset later:
            % if NextOffset > FetchOffset, then we want this batch.
            true = ets:insert(
                Table, {{Topic, PartitionIndex, BaseOffset, NextOffset}, RecordBatch2}
            ),
            NextOffset
        end,
        InitialOffset,
        RecordBatches
    ).

fetch_from_ets(Table) ->
    assert_ets(Table),

    fun(
        _FetchRequest = #{
            correlation_id := CorrelationId, topics := Topics
        },
        _Env
    ) ->
        % Inject a tiny sleep, just to avoid spamming ourselves.
        timer:sleep(125),

        #{
            correlation_id => CorrelationId,
            error_code => ?NONE,
            session_id => -1,
            throttle_time_ms => 0,
            responses => [topic_from_ets(Table, Topic) || Topic <- Topics]
        }
    end.

topic_from_ets(Table, #{topic := Topic, partitions := Partitions}) ->
    #{
        topic => Topic,
        partitions => [partition_from_ets(Table, Topic, Partition) || Partition <- Partitions]
    }.

partition_from_ets(Table, Topic, #{partition := Partition, fetch_offset := FetchOffset}) ->
    % Find record batches where the FetchOffset is inside the batch; we can use the batch's stored NextOffset field for
    % that.
    MatchSpec = ets:fun2ms(
        fun({{T, P, _B, O}, R}) when
            T =:= Topic, P =:= Partition, O > FetchOffset
        ->
            R
        end
    ),
    Records = ets:select(Table, MatchSpec),

    % There isn't a race here: even if records are inserted just after we do the 'select' above, the NextOffset _is_ the
    % high-water mark, even if we don't actually return record batched up to that point.
    NextOffset = ets:lookup_element(Table, {Topic, Partition}, 2, 0),

    #{
        partition_index => Partition,
        error_code => ?NONE,
        log_start_offset => 0,
        high_watermark => NextOffset,
        last_stable_offset => NextOffset,
        aborted_transactions => [],
        preferred_read_replica => -1,
        records => Records
    }.

offsets_in_ets(Table) ->
    fun(_ListOffsetsRequest = #{correlation_id := CorrelationId, topics := Topics}, _Env) ->
        #{
            correlation_id => CorrelationId,
            topics => lists:map(
                fun(#{name := TopicName, partitions := Partitions}) ->
                    #{
                        name => TopicName,
                        partitions => lists:map(
                            fun(
                                _Partition = #{
                                    partition_index := PartitionIndex,
                                    current_leader_epoch := LeaderEpoch,
                                    timestamp := Timestamp
                                }
                            ) ->
                                FirstOffset = 0,
                                Offset =
                                    case Timestamp of
                                        ?EARLIEST_TIMESTAMP ->
                                            FirstOffset;
                                        ?LATEST_TIMESTAMP ->
                                            high_watermark(Table, TopicName, PartitionIndex)
                                    end,

                                #{
                                    partition_index => PartitionIndex,
                                    error_code => ?NONE,
                                    % Always -1.
                                    timestamp => -1,
                                    offset => Offset,
                                    leader_epoch => LeaderEpoch
                                }
                            end,
                            Partitions
                        )
                    }
                end,
                Topics
            ),
            throttle_time_ms => 0
        }
    end.

high_watermark(Table, Topic, Partition) ->
    ets:lookup_element(Table, {Topic, Partition}, 2, 0).

insert(Table, Topic, Partition, Messages) when is_list(Messages) ->
    Count = length(Messages),
    {BaseOffset, NextOffset} = update_offset(Table, Topic, Partition, Count),
    [RecordBatch] = kafcod_message_set:prepare_message_set(
        #{compression => none}, sanitize_messages(Messages)
    ),
    true = ets:insert(
        Table, {{Topic, Partition, BaseOffset, NextOffset}, RecordBatch#{base_offset => BaseOffset}}
    ),
    ets:insert(Table, {{Topic, Partition}, NextOffset}),
    ok;
insert(Table, Topic, Partition, Message) when is_map(Message) ->
    insert(Table, Topic, Partition, [Message]).

sanitize_messages(Messages) ->
    % Make sure the messages have a key, value and headers (which is a KV-list).
    lists:map(fun sanitize_message/1, Messages).

sanitize_message(Message) ->
    Key = maps:get(key, Message, null),
    Value = maps:get(value, Message, null),
    Headers = maps:get(headers, Message, []),
    Message#{key => Key, value => Value, headers => sanitize_headers(Headers)}.

sanitize_headers(Headers) when is_list(Headers) ->
    Headers;
sanitize_headers(Headers) when is_map(Headers) ->
    maps:to_list(Headers).

lookup(Table, Topic) ->
    RecordBatches = ets:match(Table, {{Topic, '$2', '_', '_'}, '$1'}),
    lists:foldl(
        fun([RecordBatch, Partition], Acc) ->
            maps:update_with(
                Partition,
                fun(V) ->
                    % This slightly odd use of lists:flatten/1 helps to preserve the order of the record batches.
                    lists:flatten([V, flatten_record_batch(RecordBatch)])
                end,
                flatten_record_batch(RecordBatch),
                Acc
            )
        end,
        #{},
        RecordBatches
    ).

lookup(Table, Topic, Partition) ->
    % The ETS entry is `{{Topic, Partition, BaseOffset, NextOffset}, RecordBatch}`, and we're using ordered_set, so the
    % messages will always be returned in offset order.
    RecordBatches = ets:match(Table, {{Topic, Partition, '_', '_'}, '$1'}),
    lists:flatmap(
        fun([RecordBatch]) -> flatten_record_batch(RecordBatch) end,
        RecordBatches
    ).

flatten_record_batch(#{
    base_offset := BaseOffset,
    base_timestamp := BaseTimestamp,
    records := Records
}) ->
    lists:map(
        fun(#{offset_delta := OffsetDelta, timestamp_delta := TimestampDelta} = Record) ->
            Record#{
                offset => BaseOffset + OffsetDelta,
                timestamp => BaseTimestamp + TimestampDelta
            }
        end,
        Records
    ).

assert_ets(Table) when is_atom(Table); is_reference(Table) ->
    Info = ets:info(Table),
    assert_ets_info(Info).

assert_ets_info(Info) when is_list(Info) ->
    case proplists:get_value(type, Info) of
        ordered_set -> ok;
        _ -> error(expected_ordered_set)
    end,
    case proplists:get_value(protection, Info) of
        public -> ok;
        _ -> error(expected_public)
    end,
    ok.
