-module(kamock_metadata_response_topic).
-export([make_metadata_response_topic/2]).
-export([partitions/1]).

-include_lib("kafcod/include/error_code.hrl").

-define(AUTHORIZED_OPERATIONS, -2147483648).
-define(TOPIC_AUTHORIZED_OPERATIONS, ?AUTHORIZED_OPERATIONS).

-define(PARTITION_COUNT, 4).

make_metadata_response_topic(Topic, Env) ->
    make_metadata_response_topic(Topic, ?PARTITION_COUNT, Env).

make_metadata_response_topic(_Topic = #{name := TopicName}, PartitionCount, Env) ->
    % Generate a v5 (SHA1) UUID from the topic name; the same name will always get the same UUID.
    TopicId = uuid:uuid_to_string(uuid:get_v5(TopicName), binary_standard),
    #{
        error_code => ?NONE,
        name => TopicName,
        % 'topic_id' was added in v10; encoder will ignore it for earlier versions.
        topic_id => TopicId,
        is_internal => false,
        partitions => [
            kamock_metadata_response_partition:make_metadata_response_partition(
                PartitionIndex, Env
            )
         || PartitionIndex <- lists:seq(0, PartitionCount - 1)
        ],
        topic_authorized_operations => ?TOPIC_AUTHORIZED_OPERATIONS
    }.

partitions(PartitionIndexes) ->
    fun(_Topic = #{name := TopicName}, Env) ->
        #{
            error_code => ?NONE,
            name => TopicName,
            is_internal => false,
            partitions => [
                kamock_metadata_response_partition:make_metadata_response_partition(
                    PartitionIndex, Env
                )
             || PartitionIndex <- PartitionIndexes
            ],
            topic_authorized_operations => ?TOPIC_AUTHORIZED_OPERATIONS
        }
    end.
