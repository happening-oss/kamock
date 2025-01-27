-module(kamock_broker_handler).
-export([handle_request/4]).

-include_lib("kafcod/include/api_key.hrl").

handle_request(
    _ApiKey = ?PRODUCE,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    % kafire uses v4.
    {ProduceRequest, <<>>} = produce_request:decode_produce_request_4(Packet),
    ProduceResponse = kamock_produce:handle_produce_request(ProduceRequest, Env),
    {reply, produce_response:encode_produce_response_4(ProduceResponse)};
handle_request(
    _ApiKey = ?PRODUCE,
    _ApiVersion = 8,
    Packet,
    Env
) ->
    % kcat uses v8.
    {ProduceRequest, <<>>} = produce_request:decode_produce_request_8(Packet),
    ProduceResponse = kamock_produce:handle_produce_request(ProduceRequest, Env),
    {reply, produce_response:encode_produce_response_8(ProduceResponse)};
handle_request(
    _ApiKey = ?FETCH,
    _ApiVersion = 11,
    Packet,
    Env
) ->
    {FetchRequest, <<>>} = fetch_request:decode_fetch_request_11(Packet),
    FetchResponse = kamock_fetch:handle_fetch_request(FetchRequest, Env),
    {reply, fetch_response:encode_fetch_response_11(FetchResponse)};
handle_request(
    _ApiKey = ?LIST_OFFSETS,
    _ApiVersion = 2,
    Packet,
    Env
) ->
    % kafire uses v2.
    {ListOffsetsRequest0, <<>>} = list_offsets_request:decode_list_offsets_request_2(Packet),

    % Putting this code here is a bit of a hack; if we find ourselves needing to do this kind of thing too often, we
    % should pass the API version to the handler and put the hacks there.
    ListOffsetsRequest = patch_list_offsets_request(ListOffsetsRequest0),
    ListOffsetsResponse = kamock_list_offsets:handle_list_offsets_request(
        ListOffsetsRequest, Env
    ),
    {reply, list_offsets_response:encode_list_offsets_response_2(ListOffsetsResponse)};
handle_request(
    _ApiKey = ?LIST_OFFSETS,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    {ListOffsetsRequest, <<>>} = list_offsets_request:decode_list_offsets_request_5(Packet),
    ListOffsetsResponse = kamock_list_offsets:handle_list_offsets_request(
        ListOffsetsRequest, Env
    ),
    {reply, list_offsets_response:encode_list_offsets_response_5(ListOffsetsResponse)};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    % kafire uses Metadata v5
    {MetadataRequest, <<>>} = metadata_request:decode_metadata_request_5(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_5(MetadataResponse)};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 9,
    Packet,
    Env
) ->
    % homebrew kcat sends extra garbage; ignore it.
    {MetadataRequest, _Ignored} = metadata_request:decode_metadata_request_9(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_9(MetadataResponse)};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 10,
    Packet,
    Env
) ->
    % kafta uses Metadata v10
    {MetadataRequest, <<>>} = metadata_request:decode_metadata_request_10(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_10(MetadataResponse)};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 12,
    Packet,
    Env
) ->
    % If you advertise v12, librdkafka will use it.
    {MetadataRequest, _} = metadata_request:decode_metadata_request_12(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_12(MetadataResponse)};
handle_request(
    _ApiKey = ?OFFSET_COMMIT,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kafire uses v3
    {OffsetCommitRequest, <<>>} = offset_commit_request:decode_offset_commit_request_3(Packet),
    OffsetCommitResponse = kamock_offset_commit:handle_offset_commit_request(
        OffsetCommitRequest, Env
    ),
    {reply, offset_commit_response:encode_offset_commit_response_3(OffsetCommitResponse)};
handle_request(
    _ApiKey = ?OFFSET_COMMIT,
    _ApiVersion = 8,
    Packet,
    Env
) ->
    % kcat uses v8
    {OffsetCommitRequest, <<>>} = offset_commit_request:decode_offset_commit_request_8(Packet),
    OffsetCommitResponse = kamock_offset_commit:handle_offset_commit_request(
        OffsetCommitRequest, Env
    ),
    {reply, offset_commit_response:encode_offset_commit_response_8(OffsetCommitResponse)};
handle_request(
    _ApiKey = ?OFFSET_FETCH,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kafire uses v3
    {OffsetFetchRequest, <<>>} = offset_fetch_request:decode_offset_fetch_request_3(
        Packet
    ),
    OffsetFetchResponse = kamock_offset_fetch:handle_offset_fetch_request(
        OffsetFetchRequest, Env
    ),
    {reply, offset_fetch_response:encode_offset_fetch_response_3(OffsetFetchResponse)};
handle_request(
    _ApiKey = ?OFFSET_FETCH,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {OffsetFetchRequest, <<>>} = offset_fetch_request:decode_offset_fetch_request_4(
        Packet
    ),
    OffsetFetchResponse = kamock_offset_fetch:handle_offset_fetch_request(
        OffsetFetchRequest, Env
    ),
    {reply, offset_fetch_response:encode_offset_fetch_response_4(OffsetFetchResponse)};
handle_request(
    _ApiKey = ?FIND_COORDINATOR,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire requires FindCoordinator v1
    {FindCoordinatorRequest, <<>>} = find_coordinator_request:decode_find_coordinator_request_1(
        Packet
    ),
    FindCoordinatorResponse = kamock_find_coordinator:handle_find_coordinator_request(
        FindCoordinatorRequest, Env
    ),
    {reply, find_coordinator_response:encode_find_coordinator_response_1(FindCoordinatorResponse)};
handle_request(
    _ApiKey = ?FIND_COORDINATOR,
    _ApiVersion = 2,
    Packet,
    Env
) ->
    % kcat requires FindCoordinator v2
    {FindCoordinatorRequest, <<>>} = find_coordinator_request:decode_find_coordinator_request_2(
        Packet
    ),
    FindCoordinatorResponse = kamock_find_coordinator:handle_find_coordinator_request(
        FindCoordinatorRequest, Env
    ),
    {reply, find_coordinator_response:encode_find_coordinator_response_2(FindCoordinatorResponse)};
handle_request(
    _ApiKey = ?FIND_COORDINATOR,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    {FindCoordinatorRequest, <<>>} = find_coordinator_request:decode_find_coordinator_request_3(
        Packet
    ),
    FindCoordinatorResponse = kamock_find_coordinator:handle_find_coordinator_request(
        FindCoordinatorRequest, Env
    ),
    {reply, find_coordinator_response:encode_find_coordinator_response_3(FindCoordinatorResponse)};
handle_request(
    _ApiKey = ?JOIN_GROUP,
    _ApiVersion = 2,
    Packet,
    Env
) ->
    % kafire requires JoinGroup v2

    % v2 predates KIP-394, so clients are expecting us to return a member ID on the first pass.
    %
    % Putting this code here is a bit of a hack; if we find ourselves needing to do this kind of thing too often, we
    % should pass the API version to the handler and put the hacks there.
    {JoinGroupRequest0, <<>>} = join_group_request:decode_join_group_request_2(Packet),
    #{client_id := ClientId} = JoinGroupRequest0,
    JoinGroupRequest1 = maps:update_with(
        member_id,
        fun
            (_MemberId = <<>>) -> kamock_join_group:generate_member_id(ClientId);
            (MemberId) -> MemberId
        end,
        JoinGroupRequest0
    ),
    % v2 doesn't have group_instance_id, so fake it. The encoder will drop it from the response.
    JoinGroupRequest2 = JoinGroupRequest1#{group_instance_id => null},
    JoinGroupResponse = kamock_join_group:handle_join_group_request(JoinGroupRequest2, Env),
    {reply, join_group_response:encode_join_group_response_2(JoinGroupResponse)};
handle_request(
    _ApiKey = ?JOIN_GROUP,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    % kcat requires JoinGroup v5
    {JoinGroupRequest, <<>>} = join_group_request:decode_join_group_request_5(Packet),
    JoinGroupResponse = kamock_join_group:handle_join_group_request(JoinGroupRequest, Env),
    {reply, join_group_response:encode_join_group_response_5(JoinGroupResponse)};
handle_request(
    _ApiKey = ?JOIN_GROUP,
    _ApiVersion = 7,
    Packet,
    Env
) ->
    {JoinGroupRequest, <<>>} = join_group_request:decode_join_group_request_7(Packet),
    JoinGroupResponse = kamock_join_group:handle_join_group_request(JoinGroupRequest, Env),
    {reply, join_group_response:encode_join_group_response_7(JoinGroupResponse)};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire requires Heartbeat v1
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_1(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_1(HeartbeatResponse)};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kcat requires Heartbeat v3
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_3(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_3(HeartbeatResponse)};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_4(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_4(HeartbeatResponse)};
handle_request(
    _ApiKey = ?LEAVE_GROUP,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kcat requires LeaveGroup v1
    {LeaveGroupRequest, <<>>} = leave_group_request:decode_leave_group_request_1(Packet),
    LeaveGroupResponse = kamock_leave_group:handle_leave_group_request(LeaveGroupRequest, Env),
    {reply, leave_group_response:encode_leave_group_response_1(LeaveGroupResponse)};
handle_request(
    _ApiKey = ?LEAVE_GROUP,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {LeaveGroupRequest, <<>>} = leave_group_request:decode_leave_group_request_4(Packet),
    LeaveGroupResponse = kamock_leave_group:handle_leave_group_request(LeaveGroupRequest, Env),
    {reply, leave_group_response:encode_leave_group_response_4(LeaveGroupResponse)};
handle_request(
    _ApiKey = ?SYNC_GROUP,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire requires SyncGroup v1
    {SyncGroupRequest1, <<>>} = sync_group_request:decode_sync_group_request_1(Packet),
    SyncGroupRequest = SyncGroupRequest1#{protocol_type => null, protocol_name => null},
    SyncGroupResponse = kamock_sync_group:handle_sync_group_request(SyncGroupRequest, Env),
    {reply, sync_group_response:encode_sync_group_response_1(SyncGroupResponse)};
handle_request(
    _ApiKey = ?SYNC_GROUP,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kcat requires SyncGroup v3
    {SyncGroupRequest3, <<>>} = sync_group_request:decode_sync_group_request_3(Packet),
    SyncGroupRequest = SyncGroupRequest3#{protocol_type => null, protocol_name => null},
    SyncGroupResponse = kamock_sync_group:handle_sync_group_request(SyncGroupRequest, Env),
    {reply, sync_group_response:encode_sync_group_response_3(SyncGroupResponse)};
handle_request(
    _ApiKey = ?SYNC_GROUP,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    {SyncGroupRequest, <<>>} = sync_group_request:decode_sync_group_request_5(Packet),
    SyncGroupResponse = kamock_sync_group:handle_sync_group_request(SyncGroupRequest, Env),
    {reply, sync_group_response:encode_sync_group_response_5(SyncGroupResponse)};
handle_request(
    _ApiKey = ?API_VERSIONS,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire uses ApiVersions v1
    {ApiVersionsRequest, <<>>} = api_versions_request:decode_api_versions_request_1(Packet),
    ApiVersionsResponse = kamock_api_versions:handle_api_versions_request(
        ApiVersionsRequest, Env
    ),
    {reply, api_versions_response:encode_api_versions_response_1(ApiVersionsResponse)};
handle_request(
    _ApiKey = ?API_VERSIONS,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    {ApiVersionsRequest, <<>>} = api_versions_request:decode_api_versions_request_3(Packet),
    ApiVersionsResponse = kamock_api_versions:handle_api_versions_request(
        ApiVersionsRequest, Env
    ),
    {reply, api_versions_response:encode_api_versions_response_3(ApiVersionsResponse)};
handle_request(
    _ApiKey = ?DESCRIBE_CONFIGS,
    _ApiVersion = 0,
    Packet,
    Env
) ->
    % kafire uses DescribeConfigs v0
    {DescribeConfigsRequest, <<>>} = describe_configs_request:decode_describe_configs_request_0(
        Packet
    ),
    DescribeConfigsResponse = kamock_describe_configs:handle_describe_configs_request(
        DescribeConfigsRequest, Env
    ),
    {reply, describe_configs_response:encode_describe_configs_response_0(DescribeConfigsResponse)};
handle_request(
    _ApiKey = ?DESCRIBE_CONFIGS,
    _ApiVersion = 2,
    Packet,
    Env
) ->
    % kafta uses DescribeConfigs v2
    {DescribeConfigsRequest, <<>>} = describe_configs_request:decode_describe_configs_request_2(
        Packet
    ),
    DescribeConfigsResponse = kamock_describe_configs:handle_describe_configs_request(
        DescribeConfigsRequest, Env
    ),
    {reply, describe_configs_response:encode_describe_configs_response_2(DescribeConfigsResponse)}.

patch_list_offsets_request(ListOffsetsRequest0) ->
    maps:update_with(
        topics,
        fun(Topics) when is_list(Topics) ->
            lists:map(fun patch_list_offsets_topic/1, Topics)
        end,
        ListOffsetsRequest0
    ).

patch_list_offsets_topic(Topic) when is_map(Topic) ->
    maps:update_with(
        partitions,
        fun(Partitions) ->
            lists:map(fun patch_list_offsets_partition/1, Partitions)
        end,
        Topic
    ).

patch_list_offsets_partition(Partition) when is_map(Partition) ->
    maps:merge(#{current_leader_epoch => -1}, Partition).
