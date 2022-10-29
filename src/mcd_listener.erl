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


-module(mcd_listener).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([start/0]).
-export([start/1]).
-export([start_link/0]).
-export([start_link/1]).
-import(mcd_statem, [nei/1]).
-include("mcd.hrl").
-include_lib("kernel/include/inet.hrl").
-include_lib("kernel/include/logger.hrl").


start() ->
    ?FUNCTION_NAME(#{}).


start(Arg) ->
    gen_statem:start(?MODULE, [Arg], envy_gen:options(?MODULE)).


start_link() ->
    ?FUNCTION_NAME(#{}).


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok,
     unready,
     #{arg => Arg, requests => gen_statem:reqids_new()},
     [nei(callback_init), nei(open)]}.


callback_mode() ->
    handle_event_function.

handle_event(internal,
             callback_init,
             _,
             #{arg := #{callback := Module}} = Data) ->
    case Module:init([]) of
        {ok, CallbackData} ->
            {keep_state, Data#{callback_data => CallbackData}};

        stop ->
            stop;

        {stop, Reason} ->
            {stop, Reason}
    end;

handle_event(internal,
             {connect, Connection},
             _,
             #{callback_data := CallbackData,
               arg := #{callback := Module}}) ->
    {ok, _} = mcd_connection_sup:start_child(
                #{socket => Connection,
                  callback => #{data => CallbackData,
                                module => Module}}),
    keep_state_and_data;

handle_event(info,
             {'$socket', Listener, select, Handle},
             _,
             #{socket := Listener}) ->
    case socket:accept(Listener, Handle) of
        {ok, Connected} ->
            {keep_state_and_data,
             [nei({connect, Connected}), nei(accept)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;


handle_event(internal, accept, _, #{socket := Listener}) ->
    case socket:accept(Listener, nowait) of
        {ok, Connected} ->
            {keep_state_and_data,
             [nei({connect, Connected}), nei(accept)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, listen, _, #{socket := Listener}) ->
    case socket:listen(Listener, mcd_config:socket(backlog)) of
        ok ->
            {keep_state_and_data, nei(accept)};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, open, _, Data) ->
    case socket:open(inet, stream, tcp) of
        {ok, Listener} ->
            {keep_state, Data#{socket => Listener}, nei(bind)};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, bind, _, #{socket := Listener} = Data) ->
    case socket:bind(Listener,
                     #{family => inet,
                       port => mcd_config:memcached(port),
                       addr => any}) of
        ok ->
            {next_state, ready, Data, nei(listen)};

        {error, Reason} ->
            {stop, Reason}
    end.
