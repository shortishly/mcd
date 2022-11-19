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


-module(mcd_reaper).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([start_link/1]).


start_link(Arg) ->
    gen_statem:start_link({local, ?MODULE},
                          ?MODULE,
                          [Arg],
                          envy_gen:options(?MODULE)).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok, ready, #{arg => Arg}}.


callback_mode() ->
    handle_event_function.


handle_event({timeout, Key},
             expire,
             _,
             #{arg := #{callback := Module},
               callback_data := CallbackData}) ->
    case Module:expire(#{key => Key, data => CallbackData}) of
        ok ->
            keep_state_and_data;

        stop ->
            stop;

        {stop, Reason} ->
            {stop, Reason}
    end;


handle_event({timeout, _},
             flush_all,
             _,
             #{arg := #{callback := Module},
               callback_data := CallbackData}) ->
    case Module:flush_all(#{data => CallbackData}) of
        ok ->
            keep_state_and_data;

        stop ->
            stop;

        {stop, Reason} ->
            {stop, Reason}
    end;

handle_event({call, From}, {callback, #{data := CallbackData}}, _, Data) ->
    {keep_state,
     Data#{callback_data => CallbackData},
     {reply, From, ok}};

handle_event({call, From},
             {expire = Type, #{key := Key, seconds := Expiry}},
             _,
             _) ->
    {keep_state_and_data,
     [timeout_action(Key, Expiry, Type), {reply, From, ok}]};

handle_event({call, From},
             {flush_all = Type, Timeout},
             _,
             _) ->
    {keep_state_and_data,
     [timeout_action(Type, Timeout, Type), {reply, From, ok}]}.


timeout_action(Name, 0, expire) ->
    {{timeout, Name}, cancel};

timeout_action(Name, Time, Content) when Time < 60 * 60 * 24 * 30 ->
    {{timeout, Name},
     erlang:convert_time_unit(Time, second, millisecond),
     Content};

timeout_action(Name, Time, Content) ->
    {{timeout, Name},
     mcd_time:system_to_monotonic(Time, second, millisecond),
     Content,
     [{abs, true}]}.
