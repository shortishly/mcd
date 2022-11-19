%% Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.


-module(mcd_stat).


-export([all/0]).
-export([callback_mode/0]).
-export([gauge/1]).
-export([init/1]).
-export([start_link/0]).
-export([start_link/1]).
-include("mcd.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/logger.hrl").


start_link() ->
    ?FUNCTION_NAME(#{}).


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


all() ->
    ets:foldl(
      fun
          ({Name, #{counter := Counter}}, A) ->
              [{Name, counters:get(Counter, 1)} | A]
      end,
      [],
      ?MODULE).


gauge(#{name := _, delta := Value} = Arg) ->
    observation(
      Arg#{type => ?FUNCTION_NAME,
           op => delta(#{ix => 1, value => Value})});

gauge(#{name := _} = Arg) ->
    ?FUNCTION_NAME(Arg#{delta => 1});

gauge(Name) when is_atom(Name) ->
    ?FUNCTION_NAME(#{name => Name, delta => 1}).


init([Arg]) ->
    process_flag(trap_exit, true),
    ets:insert_new(
      ets:new(?MODULE, [public, named_table]),
      lists:map(
        fun
            (Name) ->
                {Name,
                 #{type => gauge, counter => counters:new(1, [])}}
        end,
        gauges())),
    {ok, ready, #{arg => Arg}}.


gauges() ->
    [cmd_flush,
     bytes_read,
     bytes_written].


callback_mode() ->
    handle_event_function.


observation(#{name := Name, type := Type, op := Op} = Arg) ->
    case ets:lookup(?MODULE, Name) of
        [{_, #{type := Type, counter := Counter}}] ->
            ok = Op(Counter);

        [{_, _}] ->
            error(badarg, [Arg]);

        [] ->
            Counter = counters:new(1, []),
            ok = Op(Counter),
            case ets:insert_new(
                   ?MODULE,
                   {Name, #{type => Type, counter => Counter}}) of

                true ->
                    ok;

                false ->
                    ?FUNCTION_NAME(Arg)
            end
    end.


delta(#{value := Value} = Arg) when Value > 0 ->
    add(Arg);

delta(#{value := Value} = Arg) ->
    sub(Arg#{value := abs(Value)}).


add(#{ix := Ix, value := Value}) ->
    fun
        (Counter) ->
            counters:add(Counter, Ix, Value)
    end.


sub(#{ix := Ix, value := Value}) ->
    fun
        (Counter) ->
            counters:sub(Counter, Ix, Value)
    end.
