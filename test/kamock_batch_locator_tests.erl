-module(kamock_batch_locator_tests).
-include_lib("eunit/include/eunit.hrl").

fixed_size_locator_exact_test_() ->
    % When the partition length is a multiple of the batch size:
    Loc = kamock_batch_locator:fixed_size(10),
    [
        {"single batch, at start", ?_assertEqual({0, 10}, Loc(0, 10, 0))},
        {"single batch, in middle", ?_assertEqual({0, 10}, Loc(0, 10, 3))},
        {"single batch, at end", ?_assertError(_, Loc(0, 10, 10))},

        {"multiple batches, at start of first", ?_assertEqual({0, 10}, Loc(0, 30, 0))},
        {"multiple batches, in middle of first", ?_assertEqual({0, 10}, Loc(0, 30, 5))},

        {"multiple batches, at start of second", ?_assertEqual({10, 10}, Loc(0, 30, 10))},
        {"multiple batches, in middle of second", ?_assertEqual({10, 10}, Loc(0, 30, 12))},

        {"multiple batches, at end of last", ?_assertEqual({20, 10}, Loc(0, 30, 29))},

        % Note: The locator doesn't have to deal with FetchOffset >= LastOffset;
        % that's dropped in kamock_partition_data:batches()
        {"multiple batches, at end", ?_assertError(_, Loc(0, 30, 30))}
    ].

fixed_size_locator_not_exact_test_() ->
    % When the partition length is NOT a multiple of the batch size:
    Loc = kamock_batch_locator:fixed_size(7),
    [
        {"multiple batches, at start of first", ?_assertEqual({0, 7}, Loc(0, 29, 0))},
        {"multiple batches, in middle of first", ?_assertEqual({0, 7}, Loc(0, 29, 5))},

        {"multiple batches, at start of second", ?_assertEqual({7, 7}, Loc(0, 29, 7))},
        {"multiple batches, in middle of second", ?_assertEqual({7, 7}, Loc(0, 29, 12))},

        {"multiple batches, almost near end of last", ?_assertEqual({21, 7}, Loc(0, 29, 27))},

        % Note that we don't have to clamp to the last offset; batches() will do that for us.
        {"multiple batches, near end of last", ?_assertEqual({28, 7}, Loc(0, 29, 28))},

        % Note: The locator doesn't have to deal with FetchOffset >= LastOffset;
        % that's dropped in kamock_partition_data:batches()
        {"multiple batches, at end of last", ?_assertError(_, Loc(0, 29, 29))}
    ].

fixed_size_locator_another_test_() ->
    % This one's taken from kamock_fetch_batch_tests:direct_test_, stripping away some of the fluff.
    Loc = kamock_batch_locator:fixed_size(3),
    [
        ?_assertEqual({0, 3}, Loc(0, 14, 0)),

        % Note that we don't have to clamp to the last offset; batches() will do that for us.
        ?_assertEqual({12, 3}, Loc(0, 14, 12)),
        ?_assertEqual({12, 3}, Loc(0, 14, 13)),

        % Note: The locator doesn't have to deal with FetchOffset >= LastOffset;
        % that's dropped in kamock_partition_data:batches()
        ?_assertError(_, Loc(0, 14, 14))
    ].

round_robin_locator_test_() ->
    Loc = kamock_batch_locator:round_robin([1, 3, 2]),
    [
        ?_assertEqual({0, 1}, Loc(0, 20, 0)),
        ?_assertEqual({1, 3}, Loc(0, 20, 1)),
        ?_assertEqual({1, 3}, Loc(0, 20, 2)),
        ?_assertEqual({1, 3}, Loc(0, 20, 3)),
        ?_assertEqual({4, 2}, Loc(0, 20, 4)),
        ?_assertEqual({4, 2}, Loc(0, 20, 5)),
        ?_assertEqual({6, 1}, Loc(0, 20, 6)),
        ?_assertEqual({7, 3}, Loc(0, 20, 7)),
        ?_assertEqual({7, 3}, Loc(0, 20, 8)),
        ?_assertEqual({7, 3}, Loc(0, 20, 9)),
        ?_assertEqual({10, 2}, Loc(0, 20, 10)),

        ?_assertError(_, Loc(0, 20, 20))
    ].
