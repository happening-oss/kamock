-module(kamock_partition_produce_response).
-export([
    make_partition_produce_response/3,
    make_error_response/2
]).
-export([return_error/1]).

-include_lib("kafcod/include/error_code.hrl").

make_partition_produce_response(
    _Topic,
    _PartitionProduceData = #{
        index := PartitionIndex,
        % A real broker returns INVALID_RECORD if records is empty; we'll settle for a function_clause error.
        records := [_ | _]
    },
    _Env
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

make_error_response(PartitionIndex, ErrorCode) ->
    #{
        index => PartitionIndex,
        error_code => ErrorCode,
        error_message => null,
        base_offset => -1,
        log_start_offset => -1,
        log_append_time_ms => -1,
        record_errors => []
    }.

return_error(ErrorCode) when is_integer(ErrorCode) ->
    fun(
        _Topic,
        _PartitionProduceData = #{
            index := PartitionIndex,
            % Unlike above, we won't enforce that records /= [] here.
            records := _
        },
        _Env
    ) ->
        make_error_response(PartitionIndex, ErrorCode)
    end.
