-module(kamock_metadata_response_partition).
-export([make_metadata_response_partition/2]).
-export([make_metadata_response_partition/3]).
-export([get_partition_leader/2]).

-include_lib("kafcod/include/error_code.hrl").

make_metadata_response_partition(PartitionIndex, _Env = #{node_ids := NodeIds}) ->
    LeaderId = get_partition_leader(PartitionIndex, NodeIds),
    make_metadata_response_partition(PartitionIndex, LeaderId, NodeIds).

make_metadata_response_partition(PartitionIndex, LeaderId, NodeIds) ->
    ReplicaNodes = [LeaderId] ++ NodeIds -- [LeaderId],
    #{
        error_code => ?NONE,
        partition_index => PartitionIndex,
        leader_id => LeaderId,
        leader_epoch => 0,
        replica_nodes => ReplicaNodes,
        isr_nodes => ReplicaNodes,
        offline_replicas => []
    }.

get_partition_leader(PartitionIndex, NodeIds) ->
    Index = PartitionIndex rem length(NodeIds),
    lists:nth(Index + 1, NodeIds).
