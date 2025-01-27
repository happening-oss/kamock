-module(kamock_join_group).
-export([handle_join_group_request/2]).

-export([member_id_required/2]).
-export([generate_member_id/1]).
-export([
    as_leader/0,
    as_leader/1,

    as_follower/0,
    as_follower/1,
    as_follower/2
]).

-include_lib("kafcod/include/error_code.hrl").

handle_join_group_request(JoinGroupRequest = #{member_id := <<>>}, _Env) ->
    member_id_required(JoinGroupRequest);
handle_join_group_request(
    _JoinGroupRequest = #{
        correlation_id := CorrelationId,
        protocol_type := ProtocolType,
        protocols := Protocols,
        member_id := MemberId,
        group_instance_id := GroupInstanceId
    },
    _Env
) ->
    % Each member sends a JoinGroup Request containing their member ID and the list of protocols they support.
    %
    % The coordinator waits until all of the requests are in (group.initial.rebalance.delay.ms; default 3s) and then
    % picks a leader and a protocol.
    %
    % We only support a single member, so we don't wait.

    % The mock broker only supports a single member, which means that choosing a protocol is also simple:
    [Protocol | _] = Protocols,
    #{name := ProtocolName, metadata := Metadata} = Protocol,

    Members = [
        #{member_id => MemberId, group_instance_id => GroupInstanceId, metadata => Metadata}
    ],

    GenerationId = 0,
    LeaderId = MemberId,

    make_join_group_response(
        CorrelationId, GenerationId, ProtocolType, ProtocolName, LeaderId, MemberId, Members
    ).

member_id_required(
    _JoinGroupRequest = #{
        correlation_id := CorrelationId,
        client_id := ClientId
    }
) ->
    % Broker allocates member ID; see KIP-394.
    GeneratedMemberId = generate_member_id(ClientId),
    member_id_required(CorrelationId, GeneratedMemberId).

member_id_required(CorrelationId, GeneratedMemberId) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?MEMBER_ID_REQUIRED,
        generation_id => -1,
        protocol_type => <<>>,
        protocol_name => <<>>,
        leader => <<>>,
        member_id => GeneratedMemberId,
        members => []
    }.

generate_member_id(ClientId) ->
    Uuid = uuid:uuid_to_string(uuid:get_v4(), binary_standard),
    <<ClientId/binary, "-", Uuid/binary>>.

as_leader() ->
    GenerationId = 0,
    as_leader(GenerationId).

as_leader(GenerationId) ->
    fun
        (JoinGroupRequest = #{member_id := <<>>}, _Env) ->
            member_id_required(JoinGroupRequest);
        (
            _JoinGroupRequest = #{
                correlation_id := CorrelationId,
                protocol_type := ProtocolType,
                protocols := Protocols,
                member_id := MemberId,
                group_instance_id := GroupInstanceId
            },
            _Env
        ) ->
            [Protocol | _] = Protocols,
            #{name := ProtocolName, metadata := Metadata} = Protocol,

            Members = [
                #{member_id => MemberId, group_instance_id => GroupInstanceId, metadata => Metadata}
            ],

            LeaderId = MemberId,

            make_join_group_response(
                CorrelationId, GenerationId, ProtocolType, ProtocolName, LeaderId, MemberId, Members
            )
    end.

as_follower() ->
    LeaderId = generate_member_id(<<"leader">>),
    GenerationId = 0,
    as_follower(LeaderId, GenerationId).

as_follower(LeaderId) ->
    GenerationId = 0,
    as_follower(LeaderId, GenerationId).

as_follower(LeaderId, GenerationId) ->
    fun
        (JoinGroupRequest = #{member_id := <<>>}, _Env) ->
            member_id_required(JoinGroupRequest);
        (
            _JoinGroupRequest = #{
                correlation_id := CorrelationId,
                protocol_type := ProtocolType,
                protocols := Protocols,
                member_id := MemberId
            },
            _Env
        ) ->
            [Protocol | _] = Protocols,
            #{name := ProtocolName, metadata := _Metadata} = Protocol,

            % followers don't get to see the list of members
            Members = [],
            make_join_group_response(
                CorrelationId, GenerationId, ProtocolType, ProtocolName, LeaderId, MemberId, Members
            )
    end.

make_join_group_response(
    CorrelationId, GenerationId, ProtocolType, ProtocolName, LeaderId, MemberId, Members
) ->
    #{
        correlation_id => CorrelationId,
        throttle_time_ms => 0,
        error_code => ?NONE,
        generation_id => GenerationId,
        protocol_type => ProtocolType,
        protocol_name => ProtocolName,
        leader => LeaderId,
        member_id => MemberId,
        members => Members
    }.
