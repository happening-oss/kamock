-module(kamock_partition_produce_response).
-export([make_partition_produce_response/3]).

-include_lib("kafcod/include/error_code.hrl").

make_partition_produce_response(
    _Topic, _PartitionProduceData = #{index := PartitionIndex, records := _Records}, _Env
) ->
    #{
        index => PartitionIndex,
        error_code => ?NONE,
        error_message => null,
        base_offset => 0,
        log_start_offset => 0,
        log_append_time_ms => 0,
        record_errors => []
    }.
