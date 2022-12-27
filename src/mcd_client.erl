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


-module(mcd_client).


-export([callback_mode/0]).
-export([handle_event/4]).
-export([init/1]).
-export([send/1]).
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
    gen_statem:start(?MODULE, [Arg], []).


start_link() ->
    ?FUNCTION_NAME(#{}).


start_link(Arg) ->
    gen_statem:start_link(?MODULE, [Arg], envy_gen:options(?MODULE)).


send(#{data := Data} = Arg) ->
    send_request(
      maps:without(
        [data],
        Arg#{request => {?FUNCTION_NAME, Data}})).


send_request(#{label := _} = Arg) ->
    mcd_statem:send_request(Arg);

send_request(Arg) ->
    mcd_statem:send_request(Arg#{label => ?MODULE}).


init([Arg]) ->
    process_flag(trap_exit, true),
    {ok, disconnected, Arg#{requests => gen_statem:reqids_new()}}.


callback_mode() ->
    handle_event_function.


handle_event({call, _}, {send, _}, disconnected, Data) ->
    {next_state, connecting, Data, [postpone, nei(open)]};

handle_event({call, _}, {send, _}, connecting, _) ->
    {keep_state_and_data, postpone};

handle_event({call, _}, {send, _}, {busy, _}, _) ->
    {keep_state_and_data, postpone};

handle_event({call, From}, {send, _} = Send, connected, Data) ->
    {next_state,
     {busy, From},
     Data#{replies => []},
     nei(Send)};

handle_event(internal, open, connecting, Data) ->
    case socket:open(inet, stream, default) of
        {ok, Socket} ->
            {keep_state, Data#{socket => Socket}, nei(connect)};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal,
             {send, Decoded},
             {busy, _},
             #{socket := Socket}) when is_list(Decoded) ->
    case socket:send(Socket, mcd_protocol:encode(Decoded)) of
        ok ->
            {keep_state_and_data,
             nei({reply_expected, mcd_protocol:reply_expected(Decoded)})};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, {send, Decoded}, {busy, _}, #{socket := Socket}) ->
    case socket:send(Socket, mcd_protocol:encode(Decoded)) of
        ok ->
            {keep_state_and_data,
             nei({reply_expected, mcd_protocol:reply_expected([Decoded])})};

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(internal, {reply_expected, []}, {busy, From}, Data) ->
    {next_state,
     connected,
     maps:without([replies], Data),
     {reply, From, ok}};

handle_event(internal, {reply_expected, ReplyExpected}, {busy, _}, Data) ->
    {keep_state, Data#{reply_expected => ReplyExpected}};

handle_event(internal,
             recv,
             _,
             #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, nowait) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(info,
             {'$socket', Socket, select, Handle},
             _,
             #{socket := Socket, partial := Partial} = Data) ->
    case socket:recv(Socket, 0, Handle) of
        {ok, Received} ->
            {keep_state,
             Data#{partial := <<>>},
             [nei({recv, iolist_to_binary([Partial, Received])}), nei(recv)]};

        {select, {select_info, _, _}} ->
            keep_state_and_data;

        {error, Reason} ->
            {stop, Reason}
    end;

handle_event(info, Msg, _, #{requests := Existing} = Data) ->
    case gen_statem:check_response(Msg, Existing, true) of
        {{reply, Reply}, Label, Updated} ->
            {keep_state,
             Data#{requests := Updated},
             nei({response, #{label => Label, reply => Reply}})};

        {{error, {Reason, ServerRef}}, Label, UpdatedRequests} ->
                {stop,
                 #{reason => Reason,
                   server_ref => ServerRef,
                   label => Label},
                 Data#{requests := UpdatedRequests}}
    end;

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
             {busy, _},
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

handle_event(internal, {recv, <<"gat ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"gats ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"touch ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"EN", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"EX", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"HD", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"NF", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"NOT_FOUND", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"NOT_STORED", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"OK", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"SERVER_ERROR", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"CLIENT_ERROR", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"DELETED", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"END", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ERROR", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"EXISTS", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"STAT ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"STORED", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"TOUCHED", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"VALUE ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"NS", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"VA", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ma", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"md ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"me ", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ME", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"mg", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"mn", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"MN", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<"ms", _/bytes>> = Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {recv, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal, {recv, Command}, _, _) ->
    {keep_state_and_data, nei({decode, Command})};

handle_event(internal, {decode, <<>>}, _, _) ->
    keep_state_and_data;

handle_event(internal, {decode, Encoded}, _, _) ->
    {keep_state_and_data, nei({message, mcd_protocol:decode(Encoded)})};

handle_event(internal,
             {message, #{header := #{opcode := stat,
                                     status := no_error},
                         key := _,
                         value := _} = Decoded},
             {busy, _},
             #{replies := Replies} = Data) ->
    {keep_state, Data#{replies => [Decoded | Replies]}};

handle_event(internal,
             {message, #{header := #{opcode := stat,
                                     status := no_error},
                         key := _, value := _} = Decoded},
             {busy, _},
             Data) ->
    {keep_state, Data#{replies => [Decoded]}};

handle_event(internal,
             {message, #{header := #{opcode := stat,
                                     status := no_error}} = Decoded},
             {busy, From},
             #{replies := Replies} = Data) ->
    {next_state,
     connected,
     maps:without([replies, reply_expected], Data),
     {reply, From, lists:reverse([Decoded | Replies])}};


handle_event(internal,
             {message, {#{command := value} = Decoded, Encoded}},
             {busy, _},
             #{replies := Replies} = Data) ->
    {keep_state, Data#{replies := [Decoded | Replies]}, nei({decode, Encoded})};

handle_event(internal,
             {message, {Decoded, <<>>}},
             {busy, From},
             #{replies := [], reply_expected := [_]} = Data) ->
    {next_state,
     connected,
     maps:without([replies, reply_expected], Data),
     {reply, From, Decoded}};

handle_event(internal,
             {message, {Decoded, <<>>}},
             {busy, From},
             #{replies := Replies, reply_expected := [_]} = Data) ->
    {next_state,
     connected,
     maps:without([replies, reply_expected], Data),
     {reply, From, lists:reverse([Decoded | Replies])}};

handle_event(internal,
             {message, {Decoded, Encoded}},
             {busy, _},
             #{replies := Replies, reply_expected := [_ | T]} = Data) ->
    {keep_state,
     Data#{replies := [Decoded | Replies], reply_expected := T},
     nei({decode, Encoded})};

handle_event(internal, connect, connecting, #{socket := Socket} = Data) ->
    case socket:connect(
           Socket,
           #{family => inet,
             port => mcd_config:memcached(port),
             addr => addr()}) of

        ok ->
            {next_state, connected, Data#{partial => <<>>}, nei(recv)};

        {error, Reason} ->
            {stop, Reason}
    end.


addr() ->
    ?FUNCTION_NAME(mcd_config:memcached(hostname)).


addr(Hostname) ->
    {ok, #hostent{h_addr_list = Addresses}} = inet:gethostbyname(Hostname),
    pick_one(Addresses).


pick_one(L) ->
    lists:nth(rand:uniform(length(L)), L).
