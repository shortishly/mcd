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


-module(mcd_emulator_text).


-export([recv/1]).
-include_lib("kernel/include/logger.hrl").
-include_lib("stdlib/include/ms_transform.hrl").
-include("mcd_emulator.hrl").


recv(#{message := #{command := set,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    noreply := Noreply,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),

    case mcd_config:maximum(value_size) of
        Maximum when byte_size(Data) > Maximum ->
            ets:delete(Table, Key),
            {continue,
             {encode, #{command => server_error,
                        reason => "object too large for cache"}}};

        _SmallerThanMaximum ->
            ets:insert(Table,
                       #entry{key = Key,
                              flags = Flags,
                              expiry = Expiry,
                              data = Data}),
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} |
              [{encode, #{command => stored}} || not(Noreply)]]}
    end;

recv(#{message := #{command := gat,
                    keys := Keys,
                    expiry := Expiry},
       data := #{table := Table}}) ->
    {continue,
     lists:foldr(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags, data = Data}] ->
                       [{encode,
                         #{command => value,
                           key => Key,
                           flags => Flags,
                           data => Data}},

                        {expire,
                         #{key => Key,
                           seconds => Expiry}} | A];

                   [] ->
                       A
               end
       end,
       [{encode, 'end'}],
       Keys)};

recv(#{message := #{command := gats,
                    keys := Keys,
                    expiry := Expiry},
       data := #{table := Table}}) ->
    {continue,
     lists:foldr(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags, cas = Unique, data = Data}] ->
                       [{encode,
                         #{command => value,
                           key => Key,
                           flags => Flags,
                           cas => Unique,
                           data => Data}},

                        {expire,
                         #{key => Key,
                           seconds => Expiry}} | A];

                   [] ->
                       A
               end
       end,
       [{encode, 'end'}],
       Keys)};

recv(#{message := #{command := touch,
                    key := Key,
                    expiry := Expiry,
                    noreply := Noreply},
       data := #{table := Table}}) ->
    case ets:update_element(
           Table,
           Key,
           [{#entry.expiry, Expiry},
            {#entry.touched, erlang:monotonic_time()}]) of
        true ->
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} |
              [{encode,
                #{command => touched}} || not(Noreply)]]};

        false ->
            {continue,
             [{encode, #{command => not_found}} || not(Noreply)]}
    end;

recv(#{message := #{command := append,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    noreply := Noreply,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),

    case ets:select_replace(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate,
                         cas = CAS,
                         data = D0} = Existing)
                   when Candidate =:= Key ->
                     Existing#entry{flags = Flags,
                                    expiry = Expiry,
                                    cas = CAS + 1,
                                    data = [D0, Data]}
             end)) of
        1 ->
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} |
              [{encode,
                #{command => stored}} || not(Noreply)]]};

        0 ->
            {continue,
             [{encode, #{command => not_stored}} || not(Noreply)]}
    end;

recv(#{message := #{command := prepend,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    noreply := Noreply,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),

    case ets:select_replace(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate,
                         cas = CAS,
                         data = D0} = Existing)
                   when Candidate =:= Key ->
                     Existing#entry{flags = Flags,
                                    expiry = Expiry,
                                    cas = CAS + 1,
                                    data = [Data, D0]}
             end)) of
        1 ->
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} |
              [{encode,
                #{command => stored}} || not(Noreply)]]};

        0 ->
            {continue,
             [{encode, #{command => not_stored}} || not(Noreply)]}
    end;

recv(#{message := #{command := cas,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    unique := Expected,
                    noreply := Noreply,
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
                    {continue,
                     [{expire,
                       #{key => Key,
                         seconds => Expiry}} |
                      [{encode, #{command => stored}} || not(Noreply)]]};

                0 ->
                    {continue,
                     [{encode,
                       #{command => exists}} || not(Noreply)]}
            end;

        [#entry{}] ->
            {continue,
             [{encode,
               #{command => exists}} || not(Noreply)]};

        [] ->
            {continue,
             [{encode,
               #{command => not_found}} || not(Noreply)]}
    end;

recv(#{message := #{command := add,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    noreply := Noreply,
                    flags := Flags},
       data := #{table := Table}} = Arg) ->
    ?LOG_DEBUG(Arg),
    case ets:insert_new(Table,
                        #entry{key = Key,
                               flags = Flags,
                               expiry = Expiry,
                               data = Data}) of
        true ->
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} | [{encode, stored} || not(Noreply)]]};

        false ->
            {continue, [{encode, not_stored} || not(Noreply)]}
    end;

recv(#{message := #{command := replace,
                    key := Key,
                    data := Data,
                    expiry := Expiry,
                    noreply := Noreply,
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
            {continue,
             [{expire,
               #{key => Key,
                 seconds => Expiry}} | [{encode, stored} || not(Noreply)]]};

        0 ->
            {continue, [{encode, not_stored} || not(Noreply)]}
    end;

recv(#{message := #{command := get, keys := Keys},
       data := #{table := Table}}) ->
    {continue,
     lists:foldr(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags, data = Data}] ->
                       [{encode,
                         #{command => value,
                           key => Key,
                           flags => Flags,
                           data => Data}} | A];

                   [] ->
                       A
               end
       end,
       [{encode, 'end'}],
       Keys)};

recv(#{message := #{command := gets, keys := Keys},
       data := #{table := Table}}) ->
    {continue,
     lists:foldr(
       fun
           (Key, A) ->
               case ets:lookup(Table, Key) of
                   [#entry{flags = Flags,
                           cas = Unique,
                           data = Data}] ->
                       [{encode,
                         #{command => value,
                           key => Key,
                           flags => Flags,
                           cas => Unique,
                           data => Data}} | A];

                   [] ->
                       A
               end
       end,
       [{encode, 'end'}],
       Keys)};

recv(#{message := #{command := delete, key := Key, noreply := Noreply},
       data := #{table := Table}}) ->
    case ets:select_delete(
           Table,
           ets:fun2ms(
             fun
                 (#entry{key = Candidate}) ->
                     Candidate == Key
             end)) of
        0 ->
            {continue, [{encode, #{command => not_found}}  || not(Noreply)]};

        1 ->
            {continue, [{encode, #{command => deleted}} || not(Noreply)]}
    end;

%% Text Commands

recv(#{data := #{table := Table},
       message := #{command := Command,
                    key := Key,
                    noreply := Noreply}} = Arg)
  when Command == incr; Command == decr ->
    try ets:update_counter(
          Table,
          Key,
          [delta(Arg), cas(Arg)]) of

        [Result, _] when is_integer(Result) ->
            {continue,
             [{encode,
               #{command => incrdecr, value => Result}} || not(Noreply)]}

    catch
        error:badarg ->
            case ets:lookup(Table, Key) of
                [#entry{data = ExistingText}] when is_binary(ExistingText) ->
                    try
                        ExistingTextAsInteger = binary_to_integer(ExistingText),

                        ets:select_replace(
                          Table,
                          ets:fun2ms(
                            fun
                                (#entry{key = FoundKey,
                                        data = FoundData} = Entry)
                                  when FoundKey =:= Key,
                                       FoundData == ExistingText ->
                                    Entry#entry{data = ExistingTextAsInteger}
                            end)),

                        ?FUNCTION_NAME(Arg)

                    catch
                        error:badarg ->
                            {continue,
                             {encode,
                              [#{command => client_error,
                                reason =>  "cannot increment or decrement "
                                "non-numeric value"} || not(Noreply)]}}
                    end;

                [] ->
                    {continue,
                     {encode,
                      [#{command => not_found} || not(Noreply)]}}
            end
    end;

recv(#{message := #{command := flush_all,
                    expiry := Expiry,
                    noreply := Noreply}}) ->
    {continue, [{flush_all, Expiry} | [{encode, ok} || not(Noreply)]]};

recv(#{message := #{command := verbosity,
                    level := _,
                    noreply := Noreply}}) ->
    {continue, [{encode, ok} || not(Noreply)]};

recv(#{message := #{command := stats}}) ->
    {continue,
     lists:foldl(
       fun
           ({Key, Value}, A) when is_integer(Value) ->
               [{encode,
                 #{command => stat,
                   key => atom_to_list(Key),
                   value => integer_to_list(Value)}} | A]
       end,
       [{encode, 'end'}],
       mcd_stat:all())}.


delta(#{message := #{command := incr, value := Delta}}) ->
    {#entry.data, Delta, mcd_util:max(uint64), 0};

delta(#{message := #{command := decr, value := Delta}}) ->
    {#entry.data, -Delta, mcd_util:min(uint64), 0}.


cas(#{message := #{command := Command}}) when Command == incr;
                                              Command == decr ->
    {#entry.cas, 1, mcd_util:max(uint64), 0}.
