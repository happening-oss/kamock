-module(test_helper).
-export([start/0]).

start() ->
    {ok, _} = application:ensure_all_started([ranch, telemetry]),
    ok.
