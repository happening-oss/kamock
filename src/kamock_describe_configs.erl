-module(kamock_describe_configs).
-export([handle_describe_configs_request/2]).

-include_lib("kafcod/include/error_code.hrl").

handle_describe_configs_request(
    _DescribeConfigsRequest = #{correlation_id := CorrelationId, resources := Resources}, _Env
) ->
    #{
        correlation_id => CorrelationId,
        results => lists:map(fun make_describe_configs_result/1, Resources),
        throttle_time_ms => 0
    }.

make_describe_configs_result(#{
    resource_type := 2, resource_name := TopicName, configuration_keys := ConfigurationKeys
}) ->
    Configs = [
        kamock_describe_configs_resource_result:make_describe_configs_resource_result(
            TopicName, Name, Value
        )
     || {Name, Value} <- filter_configs(default_configs(), ConfigurationKeys)
    ],
    #{
        error_code => ?NONE,
        error_message => <<>>,
        resource_type => 2,
        resource_name => TopicName,
        configs => Configs
    }.

filter_configs(Configs, ConfigurationKeys) when is_list(ConfigurationKeys) ->
    lists:filter(
        fun({Key, _Value}) ->
            lists:member(Key, ConfigurationKeys)
        end,
        Configs
    );
filter_configs(Configs, _ConfigurationKeys) ->
    Configs.

default_configs() ->
    [
        {<<"compression.type">>, <<"producer">>},
        {<<"leader.replication.throttled.replicas">>, <<>>},
        {<<"message.downconversion.enable">>, <<"true">>},
        {<<"min.insync.replicas">>, <<"1">>},
        {<<"segment.jitter.ms">>, <<"0">>},
        {<<"cleanup.policy">>, <<"delete">>},
        {<<"flush.ms">>, <<"9223372036854775807">>},
        {<<"follower.replication.throttled.replicas">>, <<>>},
        {<<"segment.bytes">>, <<"1073741824">>},
        {<<"retention.ms">>, <<"604800000">>},
        {<<"flush.messages">>, <<"9223372036854775807">>},
        {<<"message.format.version">>, <<"3.0-IV1">>},
        {<<"max.compaction.lag.ms">>, <<"9223372036854775807">>},
        {<<"file.delete.delay.ms">>, <<"60000">>},
        {<<"max.message.bytes">>, <<"1048588">>},
        {<<"min.compaction.lag.ms">>, <<"0">>},
        {<<"message.timestamp.type">>, <<"CreateTime">>},
        {<<"preallocate">>, <<"false">>},
        {<<"min.cleanable.dirty.ratio">>, <<"0.5">>},
        {<<"index.interval.bytes">>, <<"4096">>},
        {<<"unclean.leader.election.enable">>, <<"false">>},
        {<<"retention.bytes">>, <<"-1">>},
        {<<"delete.retention.ms">>, <<"86400000">>},
        {<<"segment.ms">>, <<"604800000">>},
        {<<"message.timestamp.difference.max.ms">>, <<"9223372036854775807">>},
        {<<"segment.index.bytes">>, <<"10485760">>}
    ].
