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
    {reply, produce_response:encode_produce_response_4(ProduceResponse), Env};
handle_request(
    _ApiKey = ?PRODUCE,
    _ApiVersion = 8,
    Packet,
    Env
) ->
    % old kcat uses v8.
    {ProduceRequest, <<>>} = produce_request:decode_produce_request_8(Packet),
    ProduceResponse = kamock_produce:handle_produce_request(ProduceRequest, Env),
    {reply, produce_response:encode_produce_response_8(ProduceResponse), Env};
handle_request(
    _ApiKey = ?FETCH,
    _ApiVersion = 11,
    Packet,
    Env
) ->
    % kafire uses v11.
    %
    % Note: recent kcat prefers v15, but v13+ uses topic_id (UUID) rather than topic (STRING or COMPACT_STRING), which
    % complicates things, so we'll not bother with that for now.
    {FetchRequest, <<>>} = fetch_request:decode_fetch_request_11(Packet),
    FetchResponse = kamock_fetch:handle_fetch_request(FetchRequest, Env),
    {reply, fetch_response:encode_fetch_response_11(FetchResponse), Env};
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
    {reply, list_offsets_response:encode_list_offsets_response_2(ListOffsetsResponse), Env};
handle_request(
    _ApiKey = ?LIST_OFFSETS,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {ListOffsetsRequest, <<>>} = list_offsets_request:decode_list_offsets_request_4(Packet),
    ListOffsetsResponse = kamock_list_offsets:handle_list_offsets_request(
        ListOffsetsRequest, Env
    ),
    {reply, list_offsets_response:encode_list_offsets_response_4(ListOffsetsResponse), Env};
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
    {reply, list_offsets_response:encode_list_offsets_response_5(ListOffsetsResponse), Env};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    % kafire uses Metadata v5
    {MetadataRequest, <<>>} = metadata_request:decode_metadata_request_5(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_5(MetadataResponse), Env};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 9,
    Packet,
    Env
) ->
    % homebrew kcat sends extra garbage; ignore it.
    {MetadataRequest, _Ignored} = metadata_request:decode_metadata_request_9(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_9(MetadataResponse), Env};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 10,
    Packet,
    Env
) ->
    % kafta uses Metadata v10
    {MetadataRequest, <<>>} = metadata_request:decode_metadata_request_10(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_10(MetadataResponse), Env};
handle_request(
    _ApiKey = ?METADATA,
    _ApiVersion = 12,
    Packet,
    Env
) ->
    % If you advertise v12, librdkafka will use it.
    {MetadataRequest, _} = metadata_request:decode_metadata_request_12(Packet),
    MetadataResponse = kamock_metadata:handle_metadata_request(MetadataRequest, Env),
    {reply, metadata_response:encode_metadata_response_12(MetadataResponse), Env};
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
    {reply, offset_commit_response:encode_offset_commit_response_3(OffsetCommitResponse), Env};
handle_request(
    _ApiKey = ?OFFSET_COMMIT,
    _ApiVersion = 7,
    Packet,
    Env
) ->
    % kafta uses v7
    {OffsetCommitRequest, <<>>} = offset_commit_request:decode_offset_commit_request_7(Packet),
    OffsetCommitResponse = kamock_offset_commit:handle_offset_commit_request(
        OffsetCommitRequest, Env
    ),
    {reply, offset_commit_response:encode_offset_commit_response_7(OffsetCommitResponse), Env};
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
    {reply, offset_commit_response:encode_offset_commit_response_8(OffsetCommitResponse), Env};
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
    {reply, offset_fetch_response:encode_offset_fetch_response_3(OffsetFetchResponse), Env};
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
    {reply, offset_fetch_response:encode_offset_fetch_response_4(OffsetFetchResponse), Env};
handle_request(
    _ApiKey = ?OFFSET_FETCH,
    _ApiVersion = 7,
    Packet,
    Env
) ->
    % kafta uses v7
    {OffsetFetchRequest, <<>>} = offset_fetch_request:decode_offset_fetch_request_7(
        Packet
    ),
    OffsetFetchResponse = kamock_offset_fetch:handle_offset_fetch_request(
        OffsetFetchRequest, Env
    ),
    {reply, offset_fetch_response:encode_offset_fetch_response_7(OffsetFetchResponse), Env};
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
    {reply, find_coordinator_response:encode_find_coordinator_response_1(FindCoordinatorResponse),
        Env};
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
    {reply, find_coordinator_response:encode_find_coordinator_response_2(FindCoordinatorResponse),
        Env};
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
    {reply, find_coordinator_response:encode_find_coordinator_response_3(FindCoordinatorResponse),
        Env};
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
    {reply, join_group_response:encode_join_group_response_2(JoinGroupResponse), Env};
handle_request(
    _ApiKey = ?JOIN_GROUP,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    % kcat requires JoinGroup v5
    {JoinGroupRequest, <<>>} = join_group_request:decode_join_group_request_5(Packet),
    JoinGroupResponse = kamock_join_group:handle_join_group_request(JoinGroupRequest, Env),
    {reply, join_group_response:encode_join_group_response_5(JoinGroupResponse), Env};
handle_request(
    _ApiKey = ?JOIN_GROUP,
    _ApiVersion = 7,
    Packet,
    Env
) ->
    {JoinGroupRequest, <<>>} = join_group_request:decode_join_group_request_7(Packet),
    JoinGroupResponse = kamock_join_group:handle_join_group_request(JoinGroupRequest, Env),
    {reply, join_group_response:encode_join_group_response_7(JoinGroupResponse), Env};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire requires Heartbeat v1
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_1(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_1(HeartbeatResponse), Env};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kcat requires Heartbeat v3
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_3(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_3(HeartbeatResponse), Env};
handle_request(
    _ApiKey = ?HEARTBEAT,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {HeartbeatRequest, <<>>} = heartbeat_request:decode_heartbeat_request_4(Packet),
    HeartbeatResponse = kamock_heartbeat:handle_heartbeat_request(HeartbeatRequest, Env),
    {reply, heartbeat_response:encode_heartbeat_response_4(HeartbeatResponse), Env};
handle_request(
    _ApiKey = ?LEAVE_GROUP,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kcat and kafire require LeaveGroup v1; we need to convert to/from v4:
    {LeaveGroupRequest1, <<>>} = leave_group_request:decode_leave_group_request_1(Packet),
    #{member_id := MemberId} = LeaveGroupRequest1,
    LeaveGroupRequest = LeaveGroupRequest1#{
        members => [#{member_id => MemberId, group_instance_id => MemberId}]
    },
    LeaveGroupResponse = kamock_leave_group:handle_leave_group_request(LeaveGroupRequest, Env),
    LeaveGroupResponse1 = downgrade_leave_group_response_1(LeaveGroupResponse),
    {reply, leave_group_response:encode_leave_group_response_1(LeaveGroupResponse1), Env};
handle_request(
    _ApiKey = ?LEAVE_GROUP,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    % kafta uses v3.
    {LeaveGroupRequest, <<>>} = leave_group_request:decode_leave_group_request_3(Packet),
    LeaveGroupResponse = kamock_leave_group:handle_leave_group_request(LeaveGroupRequest, Env),
    {reply, leave_group_response:encode_leave_group_response_3(LeaveGroupResponse), Env};
handle_request(
    _ApiKey = ?LEAVE_GROUP,
    _ApiVersion = 4,
    Packet,
    Env
) ->
    {LeaveGroupRequest, <<>>} = leave_group_request:decode_leave_group_request_4(Packet),
    LeaveGroupResponse = kamock_leave_group:handle_leave_group_request(LeaveGroupRequest, Env),
    {reply, leave_group_response:encode_leave_group_response_4(LeaveGroupResponse), Env};
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
    {reply, sync_group_response:encode_sync_group_response_1(SyncGroupResponse), Env};
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
    {reply, sync_group_response:encode_sync_group_response_3(SyncGroupResponse), Env};
handle_request(
    _ApiKey = ?SYNC_GROUP,
    _ApiVersion = 5,
    Packet,
    Env
) ->
    {SyncGroupRequest, <<>>} = sync_group_request:decode_sync_group_request_5(Packet),
    SyncGroupResponse = kamock_sync_group:handle_sync_group_request(SyncGroupRequest, Env),
    {reply, sync_group_response:encode_sync_group_response_5(SyncGroupResponse), Env};
handle_request(
    _ApiKey = ?API_VERSIONS,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire uses ApiVersions v1
    {ApiVersionsRequest, <<>>} = api_versions_request:decode_api_versions_request_1(Packet),
    handle_response(
        kamock_api_versions:handle_api_versions_request(ApiVersionsRequest, Env),
        fun api_versions_response:encode_api_versions_response_1/1,
        Env
    );
handle_request(
    _ApiKey = ?API_VERSIONS,
    _ApiVersion = 3,
    Packet,
    Env
) ->
    {ApiVersionsRequest, <<>>} = api_versions_request:decode_api_versions_request_3(Packet),
    handle_response(
        kamock_api_versions:handle_api_versions_request(ApiVersionsRequest, Env),
        fun api_versions_response:encode_api_versions_response_3/1,
        Env
    );
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
    {reply, describe_configs_response:encode_describe_configs_response_0(DescribeConfigsResponse),
        Env};
handle_request(
    _ApiKey = ?DESCRIBE_CONFIGS,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    % kafire recently added DescribeConfigs v1
    {DescribeConfigsRequest, <<>>} = describe_configs_request:decode_describe_configs_request_1(
        Packet
    ),
    DescribeConfigsResponse = kamock_describe_configs:handle_describe_configs_request(
        DescribeConfigsRequest, Env
    ),
    {reply, describe_configs_response:encode_describe_configs_response_1(DescribeConfigsResponse),
        Env};
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
    {reply, describe_configs_response:encode_describe_configs_response_2(DescribeConfigsResponse),
        Env};
handle_request(
    _ApiKey = ?SASL_HANDSHAKE,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    {SaslHandshakeRequest, <<>>} = sasl_handshake_request:decode_sasl_handshake_request_1(Packet),
    handle_response(
        kamock_sasl_handshake:handle_sasl_handshake_request(SaslHandshakeRequest, Env),
        fun sasl_handshake_response:encode_sasl_handshake_response_1/1,
        Env
    );
handle_request(
    _ApiKey = ?SASL_AUTHENTICATE,
    _ApiVersion = 1,
    Packet,
    Env
) ->
    {SaslAuthenticateRequest, <<>>} = sasl_authenticate_request:decode_sasl_authenticate_request_0(
        Packet
    ),
    handle_response(
        kamock_sasl_authenticate:handle_sasl_authenticate_request(SaslAuthenticateRequest, Env),
        fun sasl_authenticate_response:encode_sasl_authenticate_response_1/1,
        Env
    );
handle_request(
    _ApiKey = ?DESCRIBE_CLUSTER,
    _ApiVersion = 0,
    Packet,
    Env
) ->
    {DescribeClusterRequest, <<>>} = describe_cluster_request:decode_describe_cluster_request_0(
        Packet
    ),
    DescribeClusterResponse = kamock_describe_cluster:handle_describe_cluster_request(
        DescribeClusterRequest, Env
    ),
    {reply, describe_cluster_response:encode_describe_cluster_response_0(DescribeClusterResponse),
        Env}.

handle_response({reply, Response, Env}, Encoder, _) ->
    {reply, Encoder(Response), Env};
handle_response({reply, Response}, Encoder, Env) ->
    {reply, Encoder(Response), Env};
handle_response(stop, _Encoder, _Env) ->
    stop;
handle_response({stop, Reason}, _Encoder, _Env) ->
    {stop, Reason};
handle_response(noreply, _Encoder, _Env) ->
    noreply;
handle_response(Response, Encoder, Env) ->
    % older versions of kamock expected just the response, so we handle those as well.
    {reply, Encoder(Response), Env}.

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

downgrade_leave_group_response_1(#{members := [#{member_id := MemberId}]} = LeaveGroupResponse1) ->
    LeaveGroupResponse1#{member_id => MemberId};
downgrade_leave_group_response_1(LeaveGroupResponse1) ->
    LeaveGroupResponse1.
