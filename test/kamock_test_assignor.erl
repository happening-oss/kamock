-module(kamock_test_assignor).

-export([assign/2]).

assign(Topics, Members) ->
    % Naively; give the first member everything; give everyone else nothing.
    [#{member_id := MemberId1} | OtherMembers] = Members,
    AssignedPartitions = all_partitions(Topics),
    Assignments0 = #{MemberId1 => #{assigned_partitions => AssignedPartitions, user_data => <<>>}},
    lists:foldl(fun assign_nothing/2, Assignments0, OtherMembers).

assign_nothing(#{member_id := MemberId}, Acc) ->
    Acc#{MemberId => #{assigned_partitions => #{}, user_data => <<>>}}.

all_partitions(Topics) ->
    lists:foldl(
        fun(Topic, Acc) ->
            PartitionIndexes = [0, 1, 2, 3],
            Acc#{Topic => PartitionIndexes}
        end,
        #{},
        Topics
    ).
