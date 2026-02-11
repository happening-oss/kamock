-module(kamock_simple_producer).
-export([produce/4]).

-include_lib("kafcod/include/error_code.hrl").

-type message() :: map().
-spec produce(
    Broker :: kamock:broker(),
    Topic :: binary(),
    PartitionIndex :: non_neg_integer(),
    MessageOrMessages :: message() | [message()]
) -> ok.

produce(#{host := _, port := _} = Broker, Topic, PartitionIndex, Message) when
    is_binary(Topic), is_integer(PartitionIndex), is_map(Message)
->
    produce(Broker, Topic, PartitionIndex, [Message]);
produce(#{host := _, port := _} = Broker, Topic, PartitionIndex, Messages) when
    is_binary(Topic), is_integer(PartitionIndex), is_list(Messages)
->
    {ok, C} = kafcod_connection:start_link(Broker),
    BatchAttributes = #{compression => none},
    Records = kafcod_message_set:prepare_message_set(
        BatchAttributes,
        sanitize_messages(Messages)
    ),

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
        responses := [
            #{
                name := Topic,
                partition_responses := [#{index := PartitionIndex, error_code := ?NONE}]
            }
        ]
    }} =
        kafcod_connection:call(
            C,
            fun produce_request:encode_produce_request_4/1,
            ProduceRequest,
            fun produce_response:decode_produce_response_4/1
        ),
    kafcod_connection:stop(C),
    ok.

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
