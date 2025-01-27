-module(kamock_list_offsets_tests).
-include_lib("eunit/include/eunit.hrl").

-include_lib("kafcod/include/error_code.hrl").
-include_lib("kafcod/include/isolation_level.hrl").
-include_lib("kafcod/include/timestamp.hrl").

-define(BROKER_REF, {?MODULE, ?FUNCTION_NAME}).

all_test_() ->
    {foreach, fun setup/0, fun cleanup/1, [
        fun list_offsets_v2/0
    ]}.

setup() ->
    ok.

cleanup(_) ->
    meck:unload().

list_offsets_v2() ->
    % kafire uses v2 and we mangle the request on input. Did we do it right?
    {ok, Broker} = kamock_broker:start(?BROKER_REF),
    {ok, Connection} = kafcod_connection:start_link(Broker),

    {ok, #{topics := Topics}} = kafcod_connection:call(
        Connection,
        fun list_offsets_request:encode_list_offsets_request_2/1,
        #{
            topics => [
                #{
                    name => <<"cars">>,
                    partitions => [
                        #{partition_index => P, timestamp => ?EARLIEST_TIMESTAMP}
                     || P <- [0, 1, 2, 3]
                    ]
                }
            ],
            isolation_level => ?READ_COMMITTED,
            replica_id => -1
        },
        fun list_offsets_response:decode_list_offsets_response_2/1
    ),

    [#{name := <<"cars">>, partitions := Partitions}] = Topics,
    lists:foreach(
        fun(#{offset := 0, timestamp := -1, error_code := ?NONE, partition_index := _P}) -> ok end,
        Partitions
    ),

    kafcod_connection:stop(Connection),
    kamock_broker:stop(Broker),
    ok.
