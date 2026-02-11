-module(kamock_simple_consumer).
-export([
    start_link/4,
    stop/1
]).
-behaviour(gen_server).
-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).
-include_lib("kafcod/include/error_code.hrl").

-define(FETCH_DELAY_MS, 125).
-define(FETCH_INTERVAL_MS, 250).

-type consumer_fun() :: fun(
    (Topic :: binary(), Partition :: non_neg_integer(), Record :: map()) -> any()
).
-type start_ret() :: gen_server:start_ret().

-spec start_link(
    Broker :: kamock:broker(),
    Topic :: binary(),
    Partitions :: [non_neg_integer()],
    ConsumerFun :: consumer_fun()
) -> start_ret().

start_link(#{host := _, port := _} = Broker, Topic, Partitions, Fun) when
    is_binary(Topic), is_list(Partitions), is_function(Fun, 3)
->
    gen_server:start_link(?MODULE, [Broker, Topic, Partitions, Fun], []).

stop(Pid) when is_pid(Pid) ->
    gen_server:stop(Pid).

-record(state, {
    broker, connection, topic, partitions, callback
}).

init([Broker, Topic, Partitions, Fun]) ->
    PartitionOffsets = #{P => 0 || P <- Partitions},
    {ok, Conn} = kafcod_connection:start_link(Broker),
    State = #state{
        broker = Broker,
        connection = Conn,
        topic = Topic,
        partitions = PartitionOffsets,
        callback = Fun
    },
    timer:send_after(?FETCH_DELAY_MS, fetch),
    {ok, State}.

handle_call(_Req, _From, State) ->
    {reply, ok, State}.

handle_cast(_Req, State) ->
    {noreply, State}.

handle_info(
    fetch,
    #state{connection = Conn, topic = Topic, partitions = Partitions, callback = Callback} = State
) ->
    FetchRequest = kamock_fetch_request:build_fetch_request(Topic, Partitions),
    {ok, FetchResponse} = kafcod_connection:call(
        Conn,
        fun fetch_request:encode_fetch_request_11/1,
        FetchRequest,
        fun fetch_response:decode_fetch_response_11/1
    ),

    % This is a very simplistic implementation of folding over the fetch response.
    % If you want to do something more complicated, use a proper client.
    State2 = fold_fetch_response(Callback, State, FetchResponse),

    timer:send_after(?FETCH_INTERVAL_MS, fetch),
    {noreply, State2}.

fold_fetch_response(Callback, Acc, #{error_code := ?NONE, responses := Responses}) ->
    lists:foldl(fold_fetchable_topic_response(Callback), Acc, Responses).

fold_fetchable_topic_response(Callback) ->
    fun(#{topic := Topic, partitions := Partitions}, Acc) ->
        lists:foldl(fold_partition_data(Callback, Topic), Acc, Partitions)
    end.

fold_partition_data(Callback, Topic) ->
    fun(#{partition_index := PartitionIndex, records := Records}, Acc) ->
        lists:foldl(fold_record_batch(Callback, Topic, PartitionIndex), Acc, Records)
    end.

fold_record_batch(Callback, Topic, PartitionIndex) ->
    fun(
        #{
            base_offset := BaseOffset,
            last_offset_delta := LastOffsetDelta,
            base_timestamp := BaseTimestamp,
            records := Records
        },
        #state{partitions = Partitions} = Acc
    ) ->
        lists:foreach(
            fold_record(Callback, Topic, PartitionIndex, BaseOffset, BaseTimestamp), Records
        ),
        NextOffset = BaseOffset + LastOffsetDelta + 1,
        Acc#state{partitions = Partitions#{PartitionIndex := NextOffset}}
    end.

fold_record(
    Callback,
    Topic,
    PartitionIndex,
    BaseOffset,
    BaseTimestamp
) ->
    fun(
        #{offset_delta := OffsetDelta, timestamp_delta := TimestampDelta} = Record
    ) ->
        Callback(Topic, PartitionIndex, Record#{
            offset => BaseOffset + OffsetDelta,
            timestamp => BaseTimestamp + TimestampDelta
        })
    end.
