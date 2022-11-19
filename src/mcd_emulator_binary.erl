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


-module(mcd_emulator_binary).


-export([recv/1]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("mcd_emulator.hrl").


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
            {continue,
             {encode, #{header => response_header(Header, server_error)}}};

        _SmallerThanMaximum ->
            ets:insert(Table,
                       #entry{key = Key,
                              flags = Flags,
                              expiry = Expiry,
                              data = Value}),
            {continue,
             [{encode,
               #{header => response_header(Header, no_error)}},
              {expire, #{key => Key, seconds => Expiry}}]}
    end;

recv(#{message := #{header := #{magic := request,
                                cas := 0,
                                opcode := get} = Header,
                    key := Key},
       data := #{table := Table}}) ->
    case ets:lookup(Table, Key) of
        [#entry{data = Data, flags = Flags}] ->
            {continue,
             {encode,
              #{header => response_header(Header, no_error),
                extra => #{flags => Flags},
                value => Data}}};

        [] ->
            {continue,
             {encode,
              #{header => response_header(Header, key_not_found)}}}
    end;

recv(#{message := #{header := #{magic := request,
                                cas := 0,
                                opcode := delete} = Header,
                    key := Key},
       data := #{table := Table}}) ->
    ets:delete(Table, Key),
    {continue, {encode, #{header => response_header(Header, no_error)}}};

recv(#{message := #{header := #{magic := request,
                                opcode := no_op} = Header}}) ->
    {continue, {encode, #{header => response_header(Header)}}};

recv(#{message := #{header := #{magic := request,
                                opcode := stat} = Header}}) ->
    {continue,
     lists:foldl(
       fun
           ({Key, Value}, A) when is_integer(Value) ->
               [{encode, #{header => response_header(Header),
                           key => atom_to_binary(Key),
                           value => integer_to_binary(Value)}} | A]
       end,
       [{encode, #{header => response_header(Header)}}],
       mcd_stat:all())};

recv(#{message := #{header := #{magic := request,
                                opcode := flush} = Header},
       data := #{table := Table}}) ->
    ets:delete_all_objects(Table),
    {continue, {encode, #{header => response_header(Header)}}};

recv(#{message := #{header := #{magic := request,
                                opcode := version} = Header}}) ->
    {continue,
     {encode,
      #{header => response_header(Header), value => <<"1.3.1">>}}}.


response_header(RequestHeader) ->
    ?FUNCTION_NAME(RequestHeader, no_error).


response_header(RequestHeader, Status) ->
    maps:without(
      [vbucket_id],
      RequestHeader#{magic := response,
                     status => Status}).
