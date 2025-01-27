-module(kamock_broker_start_tests).
-include_lib("eunit/include/eunit.hrl").

start_test() ->
    {ok, Broker} = kamock_broker:start(),
    #{port := Port} = Broker,
    ?assertNotEqual(0, Port),

    % Can we look it up by Broker and by Ref?
    #{ref := Ref} = Broker,
    ?assertMatch(
        #{
            pid := _,
            ip := _,
            port := _,
            status := _,
            protocol := kamock_broker_protocol
        },
        kamock_broker:info(Broker)
    ),
    ?assertMatch(
        #{
            pid := _,
            ip := _,
            port := _,
            status := _,
            protocol := kamock_broker_protocol
        },
        kamock_broker:info(Ref)
    ),

    kamock_broker:stop(Broker),

    % There should be no listeners.

    % Note: If it fails here, it's probably because one of the other tests failed and/or forgot to stop its broker.
    % Since the unit tests pass their {module, function} as the broker ref, it should be easy to figure out which one.
    ?assertEqual(#{}, kamock_broker:info()),
    ok.
