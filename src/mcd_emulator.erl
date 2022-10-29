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


-module(mcd_emulator).


-export([init/1]).
-export([recv/1]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").


-record(entry, {key, flags, expiry, data, cas = 1}).

init([]) ->
    {ok, #{table => ets:new(?MODULE, [{keypos, 2}, public, named_table])}}.


%%
%% Binary TCP Protocol
%%

recv(#{message := #{extra := #{expiration := Expiry,
                               flags := Flags},
                    header := #{magic := request,
                                cas := 0,
                                opcode := set} = Header,
                    key := Key,
                    value := Value},
       data := #{table := Table}}) ->

    case mcd_config:maximum(value_size) of
        Maximum when byte_size(Value) > Maximum ->
            ets:delete(Table, Key),
            {encode,#{header => response_header(Header, server_error)}};

        _SmallerThanMaximum ->
            ets:insert(Table,
                       #entry{key = Key,
                              flags = Flags,
                              expiry = Expiry,
                              data = Value}),
            {encode, stored}
    end;


recv(#{message := #{header := #{magic := request,
                                opcode := noop} = Header}}) ->
    {encode,#{header => response_header(Header)}};

recv(#{message := #{header := #{magic := request,
                                opcode := stat} = Header}}) ->
    {encode,#{header => response_header(Header)}};

recv(#{message := #{header := #{magic := request,
                                opcode := flush} = Header}}) ->
    {encode,#{header => response_header(Header)}};

recv(#{message := #{header := #{magic := request,
                                opcode := version} = Header}}) ->
    {encode,
     #{header => response_header(Header), value => <<"1.3.1">>}};

%%
%% Meta Commands
%%

recv(#{message := #{command := meta_debug,
                    key := Key},
       data := #{table := Table}}) ->
    case ets:lookup(Table, Key) of
        [#entry{cas = Expected}] ->
            {encode, #{command => meta_debug_reply,
                       key => Key,
                       internal => [{"cas", integer_to_list(Expected)}]}};
        [] ->
            {encode, meta_miss}
    end;

recv(#{message := #{command := meta_set,
                    data := _Data,
                    flags := _Flags,
                    key := _Key},
       data := #{table := _Table}}) ->
    {encode, abc};

recv(#{message := #{command := meta_get,
                    flags := Flags,
                    key := Key} = Message,
       data := #{table := Table}}) ->
    case ets:lookup(Table, Key) of
        [#entry{data = Data}] ->
            {encode,
             maps:merge(
               case lists:member(value, Flags) of
                   true ->
                       #{data => Data};
                   false ->
                       #{}
               end,
               #{command => response_command(Message),
                 key => Key,
                 flags => lists:foldl(
                            fun
                                (value, A) ->
                                    A;

                                (size, A) ->
                                    [{size, byte_size(Data)} | A]
                            end,
                            [],
                            Flags)})};

        [] ->
            {encode, meta_miss}
    end;

%%
%% Text Storage Commands
%%

recv(#{message := #{command := set,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),

    case mcd_config:maximum(value_size) of
        Maximum when byte_size(Data) > Maximum ->
            ets:delete(Table, Key),
            {encode, #{command => server_error,
                       reason => "object too large for cache"}};

        _SmallerThanMaximum ->
            ets:insert(Table,
                       #entry{key = Key,
                              flags = Flags,
                              expiry = Expiry,
                              data = Data}),
            {encode, stored}
    end;

recv(#{message := #{command := cas,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    unique := Expected,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),
    case ets:lookup(Table, Key) of
        [#entry{cas = Expected}] ->
            case ets:select_replace(
                   Table,
                   ets:fun2ms(
                     fun
                         (#entry{key = Candiate,
                                 cas = Actual} = Existing)
                           when Candiate =:= Key,
                                Expected == Actual ->
                             Existing#entry{flags = Flags,
                                            expiry = Expiry,
                                            cas = Actual + 1,
                                            data = Data}
                     end)) of

                1 ->
                    {encode, stored};

                0 ->
                    {encode, exists}
            end;

        [#entry{}] ->
            {encode, exists};

        [] ->
            {error, not_found}
    end;

recv(#{message := #{command := add,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),
    case ets:insert_new(Table,
                        #entry{key = Key,
                               flags = Flags,
                               expiry = Expiry,
                               data = Data}) of
        true ->
            {encode, #{command => stored}};

        false ->
            {encode, #{command => not_stored}}
    end;

recv(#{message := #{command := replace,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),
    case ets:select_replace(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candiate} = Existing) when Candiate =:= Key ->
                     Existing#entry{flags = Flags, expiry = Expiry, data = Data}
             end)) of
        1 ->
            {encode, stored};

        0 ->
            {encode, not_stored}
    end;

recv(#{message := #{command := get, keys := Keys},
       data := #{table := Table}}) ->
    {encode,
     lists:foldl(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags, expiry = Expiry, data = Data}] ->
                       [#{command => value,
                          key => Key,
                          flags => Flags,
                          expiry => Expiry,
                          data => Data} | A];

                   [] ->
                       A
               end
       end,
       ['end'],
       Keys)};

recv(#{message := #{command := gets, keys := Keys},
       data := #{table := Table}}) ->
    {encode,
     lists:foldl(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags,
                           expiry = Expiry,
                           cas = Unique,
                           data = Data}] ->
                       [#{command => value,
                          key => Key,
                          flags => Flags,
                          cas => Unique,
                          expiry => Expiry,
                          data => Data} | A];

                   [] ->
                       A
               end
       end,
       ['end'],
       Keys)};

recv(#{message := #{command := delete, key := Key, noreply := false},
       data := #{table := Table}}) ->
    case ets:select_delete(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate}) ->
                     Candidate =:= Key
             end)) of
        0 ->
            {encode, not_found};

        1 ->
            {encode, deleted}
    end.


response_header(RequestHeader) ->
    ?FUNCTION_NAME(RequestHeader, no_error).


response_header(RequestHeader, Status) ->
    maps:without(
      [vbucket_id],
      RequestHeader#{magic := response,
                     status => Status}).


response_command(#{command := meta_get, flags := Flags}) ->
    case lists:member(value, Flags) of
        true ->
            meta_value;

        false ->
            meta_stored
    end.
