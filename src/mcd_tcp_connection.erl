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


-module(mcd_tcp_connection).


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
     ready,
     #{arg => Arg,
       partial => <<>>,
       requests => gen_statem:reqids_new()},
     nei(recv)}.


callback_mode() ->
    handle_event_function.


handle_event(internal,
             {callback, F, A},
             _,
             #{arg := #{callback := #{module := M}}}) ->
    try apply(M, F, A) of
        {continue, Actions} when is_list(Actions) ->
            {keep_state_and_data, [nei(Action) || Action <- Actions]};

        {continue, Action} ->
            {keep_state_and_data, nei(Action)};

        continue ->
            keep_state_and_data;

        stop ->
            stop;

        {stop, Reason} ->
            {stop, Reason}
    catch
        error:badarg ->
            {keep_state_and_data,
             nei({encode, #{reason => <<"bad command line format">>,
                            command => client_error}})}
    end;

handle_event(info,
             {'$socket', Socket, select, Handle},
             _,
             #{arg := #{socket := Socket},
               partial := Partial} = Data) ->
    case socket:recv(Socket, 0, Handle) of
        {ok, Received} ->
            mcd_stat:gauge(#{name => bytes_read,
                             delta => byte_size(Received)}),

            ?LOG_DEBUG(#{received => Received}),

            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, econnreset} ->
            stop;

        {error, closed} ->
            stop;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, {recv, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal,
             {recv,
              <<Magic:8,
                _Opcode:8,
                _KeyLength:16,
                _ExtraLength:8,
                0:8,
                _BucketOrStatus:16,
                TotalBodyLength:32,
                _Opaque:32,
                _CAS:64,
                Body:TotalBodyLength/bytes,
                Remainder/bytes>> = Encoded},
             _,
             _) when Magic == ?REQUEST;
                     Magic == ?RESPONSE ->
    {keep_state_and_data,
     [nei({decode, <<(binary:part(Encoded, {0, 24}))/bytes, Body/bytes>>}),
      nei({recv, Remainder})]};

handle_event(internal, {recv, <<"stats", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"quit", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"flush_all", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"verbosity ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"incr ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"decr ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"set ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"append ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"prepend ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"cas ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"get ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"gets ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"add ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"replace ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"delete ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ma ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"md ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"me ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"mg ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"mn", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ms ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, _}, _, _) ->
    {keep_state_and_data, nei({encode, #{command => error}})};

handle_event(internal,
             {decode, Encoded},
             _,
             #{arg := #{callback := #{data := CallbackData}},
               partial := Partial} = Data) ->
    try mcd_protocol:decode(Encoded) of
        {Decoded, Remainder} ->
            cmd_stats(Decoded),
            {keep_state_and_data,
             [nei({callback,
                   recv,
                   [#{message => Decoded,
                      data => CallbackData}]}),
              nei({recv, Remainder})]};

        partial ->
            {keep_state, Data#{partial := [Partial, Encoded]}}
    catch
        error:badarg ->
            ?LOG_ERROR(#{encoded => Encoded}),
            {keep_state_and_data,
             nei({encode, #{reason => <<"bad command line format">>,
                            command => client_error}})}
    end;

handle_event(internal,
             {expire, _} = Expire,
             _,
             #{requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := gen_statem:send_request(
                         mcd_reaper,
                         Expire,
                         expire,
                         Requests)}};

handle_event(internal,
             {flush_all, _} = FlushAll,
             _,
             #{requests := Requests} = Data) ->
    {keep_state,
     Data#{requests := gen_statem:send_request(
                         mcd_reaper,
                         FlushAll,
                         flush_all,
                         Requests)}};

handle_event(internal, {encode, Command}, _, _) when is_atom(Command) ->
    {keep_state_and_data, nei({encode, #{command => Command}})};

handle_event(internal, {encode, Decoded}, _, _) ->
    {keep_state_and_data, nei({send, mcd_protocol:encode(Decoded)})};

handle_event(internal, {send, Encoded}, _, #{arg := #{socket := Socket}}) ->
    case socket:send(Socket, Encoded) of
        ok ->
            mcd_stat:gauge(#{name => bytes_written,
                             delta => iolist_size(Encoded)}),
            keep_state_and_data;

        {error, econnreset} ->
            stop;

        {error, closed} ->
            stop;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal,
             recv,
             _,
             #{arg := #{socket := Socket},
               partial := Partial} = Data) ->
    case socket:recv(Socket, 0, nowait) of
        {ok, Received} ->
            mcd_stat:gauge(#{name => bytes_read,
                             delta => byte_size(Received)}),

            ?LOG_DEBUG(#{received => Received}),

            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, econnreset} ->
            stop;

        {error, closed} ->
            stop;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, ok}, expire, Updated} ->
            {keep_state, Data#{requests := Updated}};

        {{reply, ok}, flush_all, Updated} ->
            {keep_state, Data#{requests := Updated}};

        no_request ->
            ?LOG_ERROR(#{msg => Msg, data => Data}),
            keep_state_and_data;

        no_reply ->
            ?LOG_ERROR(#{msg => Msg, data => Data}),
            keep_state_and_data
    end.


cmd_stats(#{header := #{opcode := flush}}) ->
    mcd_stat:gauge(cmd_flush);

cmd_stats(_) ->
    ok.
