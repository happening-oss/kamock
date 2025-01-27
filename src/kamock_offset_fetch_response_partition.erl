-module(kamock_offset_fetch_response_partition).
-export([make_offset_fetch_response_partition/3]).

-include_lib("kafcod/include/error_code.hrl").

make_offset_fetch_response_partition(_TopicName, PartitionIndex, _Env) ->
    #{
        partition_index => PartitionIndex,
        committed_offset => -1,
        % 'metadata' is a nullable string, but a real broker returns an empty string for unknown/new commits, so we'll
        % do that.
        metadata => <<>>,
        error_code => ?NONE
    }.
