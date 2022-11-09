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


-module(mcd_protocol).


-export([decode/1]).
-export([encode/1]).
-include("mcd.hrl").


-spec decode(binary()) -> {mcd:protocol(), binary()} | partial | error.

decode(<<"STAT ", Remainder/bytes>>) ->
    decode_text_command(stat, Remainder);

decode(<<"stats", Remainder/bytes>>) ->
    decode_text_command(stats, Remainder);

decode(<<"stat", Remainder/bytes>>) ->
    decode_text_command(stat, Remainder);

decode(<<"set ", Remainder/bytes>>) ->
    decode_text_command(set, Remainder);

decode(<<"cas ", Remainder/bytes>>) ->
    decode_text_command(cas, Remainder);

decode(<<"add ", Remainder/bytes>>) ->
    decode_text_command(add, Remainder);

decode(<<"replace ", Remainder/bytes>>) ->
    decode_text_command(replace, Remainder);

decode(<<"append ", Remainder/bytes>>) ->
    decode_text_command(append, Remainder);

decode(<<"prepend ", Remainder/bytes>>) ->
    decode_text_command(prepend, Remainder);

decode(<<"get ", Remainder/bytes>>) ->
    decode_text_command(get, Remainder);

decode(<<"gets ", Remainder/bytes>>) ->
    decode_text_command(gets, Remainder);

decode(<<"gat ", Remainder/bytes>>) ->
    decode_text_command(gat, Remainder);

decode(<<"gats ", Remainder/bytes>>) ->
    decode_text_command(gats, Remainder);

decode(<<"delete ", Remainder/bytes>>) ->
    decode_text_command(delete, Remainder);

decode(<<"incr ", Remainder/bytes>>) ->
    decode_text_command(incr, Remainder);

decode(<<"decr ", Remainder/bytes>>) ->
    decode_text_command(decr, Remainder);

decode(<<"touch ", Remainder/bytes>>) ->
    decode_text_command(touch, Remainder);

decode(<<"VALUE ", Remainder/bytes>>) ->
    decode_text_command(value, Remainder);

decode(<<"me ", Remainder/bytes>>) ->
    decode_text_command(meta_debug, Remainder);

decode(<<"ME ", Remainder/bytes>>) ->
    decode_text_command(meta_debug_reply, Remainder);

decode(<<"mg ", Remainder/bytes>>) ->
    decode_text_command(meta_get, Remainder);

decode(<<"ms ", Remainder/bytes>>) ->
    decode_text_command(meta_set, Remainder);

decode(<<"md ", Remainder/bytes>>) ->
    decode_text_command(meta_delete, Remainder);

decode(<<"VA ", Remainder/bytes>>) ->
    decode_text_command(meta_value, Remainder);

decode(<<"HD ", Remainder/bytes>>) ->
    decode_text_command(meta_stored, Remainder);

decode(<<"NS ", Remainder/bytes>>) ->
    decode_text_command(meta_not_stored, Remainder);

decode(<<"EX ", Remainder/bytes>>) ->
    decode_text_command(meta_exists, Remainder);

decode(<<"NF ", Remainder/bytes>>) ->
    decode_text_command(meta_not_found, Remainder);

decode(<<"CLIENT_ERROR ", Remainder/bytes>>) ->
    decode_text_command(client_error, Remainder);

decode(<<"SERVER_ERROR ", Remainder/bytes>>) ->
    decode_text_command(server_error, Remainder);

decode(<<"ERROR\r\n", Remainder/bytes>>) ->
    {#{command => error}, Remainder};

decode(<<"TOUCHED\r\n", Remainder/bytes>>) ->
    {#{command => touched}, Remainder};

decode(<<"STORED\r\n", Remainder/bytes>>) ->
    {#{command => stored}, Remainder};

decode(<<"NOT_STORED\r\n", Remainder/bytes>>) ->
    {#{command => not_stored}, Remainder};

decode(<<"EXISTS\r\n", Remainder/bytes>>) ->
    {#{command => exists}, Remainder};

decode(<<"NOT_FOUND\r\n", Remainder/bytes>>) ->
    {#{command => not_found}, Remainder};

decode(<<"END\r\n", Remainder/bytes>>) ->
    {#{command => 'end'}, Remainder};

decode(<<"DELETED\r\n", Remainder/bytes>>) ->
    {#{command => deleted}, Remainder};

decode(<<"EN\r\n", Remainder/bytes>>) ->
    {#{command => meta_miss}, Remainder};

decode(<<?RESPONSE:8,
         Opcode:8,
         KeyLength:16,
         ExtraLength:8,
         ?RAW:8,
         Status:16,
         TotalBodyLength:32,
         Opaque:32,
         CAS:64,
         Body:TotalBodyLength/bytes,
         Remainder/bytes>>) ->

    <<Extra:ExtraLength/bytes,
      Key:KeyLength/bytes,
      Value/bytes>> = Body,

    {decode_binary(
       #{magic => response,
         opcode => mcd_opcode:lookup(Opcode),
         data_type => raw,
         status => mcd_status:lookup(Status),
         opaque => Opaque,
         cas => CAS},
       Extra,
       Key,
       Value),
     Remainder};

decode(<<?REQUEST:8,
         Opcode:8,
         KeyLength:16,
         ExtraLength:8,
         ?RAW:8,
         VBucketId:16,
         TotalBodyLength:32,
         Opaque:32,
         CAS:64,
         Body:TotalBodyLength/bytes,
         Remainder/bytes>>) ->

    <<Extra:ExtraLength/bytes, Key:KeyLength/bytes, Value/bytes>> = Body,

    {decode_binary(
       #{magic => request,
         opcode => mcd_opcode:lookup(Opcode),
         data_type => raw,
         vbucket_id => VBucketId,
         opaque => Opaque,
         cas => CAS},
       Extra,
       Key,
       Value),
     Remainder};

decode(_) ->
    error.



%%
%% get, getq, getk, getkq
%%
decode_binary(#{magic := request, opcode := Opcode} = Header,
              <<>>,
              Key,
              <<>>)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header, key => Key};

decode_binary(#{magic := response,
                opcode := Opcode,
                status := key_not_found} = Header,
              <<>>,
              <<>>,
              <<>>)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<Flags:32>>,
              <<>>,
              <<>>)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header, extra => #{flags => Flags}};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<Flags:32>>,
              <<>>,
              Value)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header,
      extra => #{flags => Flags},
      value => Value};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<Flags:32>>,
              Key,
              <<>>)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header,
      extra => #{flags => Flags},
      key => Key};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<Flags:32>>,
              Key,
              Value)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    #{header => Header,
      extra => #{flags => Flags},
      key => Key,
      value => Value};

%%
%% set, add, replace
%%
decode_binary(#{magic := request,
                opcode := Opcode} = Header,
              <<Flags:32, Expiration:32>>,
              Key,
              Value)
  when Opcode == set;
       Opcode == add;
       Opcode == replace ->
    #{header => Header,
      extra => #{flags => Flags, expiration => Expiration},
      key => Key,
      value => Value};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<>>,
              <<>>,
              <<>>)
  when Opcode == set;
       Opcode == add;
       Opcode == replace ->
    #{header => Header};

%%
%% delete
%%

decode_binary(#{magic := request,
                opcode := delete} = Header,
              <<>>,
              Key,
              <<>>)
  when Key /= <<>> ->
    #{header => Header, key => Key};

decode_binary(#{magic := response,
                opcode := delete} = Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};


%%
%% increment, decrement
%%


decode_binary(#{magic := request,
                opcode := Opcode} = Header,
              <<Delta:64, Initial:64, Expiration:32>>,
              Key,
              <<>>)
  when Opcode == increment;
       Opcode == decrement ->
    #{header => Header,
      extra => #{delta => Delta,
                 initial => Initial,
                 expiration => Expiration},
      key => Key};

decode_binary(#{magic := response,
                opcode := Opcode} = Header,
              <<>>,
              <<>>,
              Value)
  when Opcode == increment;
       Opcode == decrement ->
    #{header => Header, value => Value};


%% quit

decode_binary(#{magic := request,
                opcode := quit} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};

decode_binary(#{magic := response,
                opcode := quit} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};


%% flush


decode_binary(#{magic := request,
                opcode := flush} =  Header,
              <<Expiration:32>>,
              <<>>,
              <<>>) ->
    #{header => Header, extra => #{expiration => Expiration}};

decode_binary(#{opcode := flush} =  Header, <<>>, <<>>, <<>>) ->
    #{header => Header};

%% noop

decode_binary(#{opcode := noop} =  Header, <<>>, <<>>, <<>>) ->
    #{header => Header};

%% version


decode_binary(#{magic := request,
                opcode := version} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};

decode_binary(#{magic := response,
                opcode := version} =  Header,
              <<>>,
              <<>>,
              Value) ->
    #{header => Header, value => Value};


%% append, prepend


decode_binary(#{magic := request,
                opcode := Opcode} =  Header,
              <<>>,
              Key,
              Value)
  when Opcode == append;
       Opcode == prepend ->
    #{header => Header, key => Key, value => Value};

decode_binary(#{magic := response,
                opcode := Opcode} =  Header,
              <<>>,
              <<>>,
              <<>>)
  when Opcode == append;
       Opcode == prepend ->
    #{header => Header};


%% stat


decode_binary(#{magic := request,
                opcode := stat} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};

decode_binary(#{magic := request,
                opcode := stat} =  Header,
              <<>>,
              Key,
              <<>>) ->
    #{header => Header, key => Key};

decode_binary(#{magic := response,
                opcode := stat} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};

decode_binary(#{magic := response,
                opcode := stat} =  Header,
              <<>>,
              Key,
              Value) ->
    #{header => Header, key => Key, value => Value};


%%
%% verbosity
%%


decode_binary(#{magic := request, opcode := verbosity} =  Header,
              <<Verbosity:32>>,
              <<>>,
              <<>>) ->
    #{header => Header, extra => #{verbosity => Verbosity}};

decode_binary(#{magic := response, opcode := verbosity} =  Header,
              <<>>,
              <<>>,
              <<>>) ->
    #{header => Header};


%%
%% touch, gat and gatq
%%


decode_binary(#{magic := request, opcode := Opcode} =  Header,
              <<Expiration:32>>,
              Key,
              <<>>)
  when Key /= <<>>,
       Opcode == touch;
       Opcode == gat;
       Opcode == gatq ->
    #{header => Header, extra => #{expiration => Expiration}}.

encode(#{command := Command}) when Command == stats ->
    [atom_to_list(Command), ?RN];

encode(#{command := Command,
         key := Key,
         value := Value}) when Command == stat ->
    ["STAT ", Key, " ", Value, ?RN];

encode(#{command := Command,
         keys := Keys}) when Command == get; Command == gets ->
    [atom_to_list(Command),
     " ",
     lists:join(" ", Keys),
     ?RN];

encode(#{command := set = Command,
         key := Key,
         flags := Flags,
         expiry := Expiry,
         noreply := Noreply,
         data := Data}) ->
    [io_lib:format("~p ~s ~p ~p ~p",
                   [Command, Key, Flags, Expiry, byte_size(Data)]),
     [" noreply" || Noreply],
     ?RN,
     Data,
     ?RN];

encode(#{command := cas = Command,
         key := Key,
         flags := Flags,
         expiry := Expiry,
         unique := Unique,
         noreply := Noreply,
         data := Data}) ->
    [io_lib:format("~p ~s ~p ~p ~p ~p",
                   [Command, Key, Flags, Expiry, byte_size(Data), Unique]),
     [" noreply" || Noreply],
     ?RN,
     Data,
     ?RN];

encode(#{command := Command,
         expiry := Expiry,
         keys := Keys}) when Command == gat; Command == gats ->
    [lists:join(
       " ",
       [atom_to_list(Command), integer_to_list(Expiry) | Keys]),
     ?RN];

encode(#{command := delete, key := Key, noreply := Noreply}) ->
    ["delete ", Key, [" noreply" || Noreply], ?RN];

encode(#{command := Command, key := Key, value := Value, noreply := Noreply})
  when Command == incr; Command == decr ->
    [io_lib:format("~p ~s ~p", [Command, Key, Value]),
     [" noreply" || Noreply],
     ?RN];

encode(#{command := touch = Command,
         key := Key,
         expiry := Expiry,
         noreply := Noreply}) ->
    [io_lib:format("~p ~s ~p", [Command, Key, Expiry]),
     [" noreply" || Noreply],
     ?RN];

encode(#{command := value,
         key := Key,
         cas := CAS,
         flags := Flags,
         data := Data}) ->
    [io_lib:format(
       "VALUE ~s ~p ~p ~p",
       [Key,
        Flags,
        byte_size(Data),
        CAS]),
     ?RN,
     Data,
     ?RN];

encode(#{command := value,
         key := Key,
         flags := Flags,
         data := Data}) ->
    [io_lib:format(
       "VALUE ~s ~p ~p",
       [Key,
        Flags,
        byte_size(Data)]),
     ?RN,
     Data,
     ?RN];

encode(#{command := 'end'}) ->
    ["END", ?RN];

encode(#{command := meta_set = Meta,
         flags := Flags,
         key := Key,
         data := Data}) ->
    [command(Meta),
     " ",
     Key,
     " ",
     integer_to_binary(byte_size(Data)),
     [[" ", mcd_meta_flags:encode(Flags)] || length(Flags) > 0],
     ?RN,
     Data,
     ?RN];

encode(#{command := meta_miss, flags := Flags}) ->
    ["EN ", mcd_meta_flags:encode(Flags), ?RN];

encode(#{command := meta_stored, flags := Flags}) ->
    ["HD ", mcd_meta_flags:encode(Flags), ?RN];

encode(#{command := meta_not_found, flags := Flags}) ->
    ["NF ", mcd_meta_flags:encode(Flags), ?RN];

encode(#{command := meta_not_stored, flags := Flags}) ->
    ["NS ", mcd_meta_flags:encode(Flags), ?RN];

encode(#{command := meta_value,
         flags := Flags,
         data := Data}) ->
    ["VA ",
     integer_to_binary(byte_size(Data)),
     " ",
     mcd_meta_flags:encode(Flags),
     ?RN,
     Data,
     ?RN];

encode(#{command := meta_miss}) ->
    ["EN", ?RN];

encode(#{command := Meta, flags := Flags, key := Key})
  when Meta == meta_debug; Meta == meta_get; Meta == meta_delete ->
    [command(Meta),
     " ",
     Key,
     " ",
     mcd_meta_flags:encode(Flags),
     ?RN];

encode(#{command := Meta, key := Key})
  when Meta == meta_debug; Meta == meta_get; Meta == meta_debug ->
    [command(Meta), " ", Key, ?RN];

encode(#{command := meta_debug_reply = Meta, key := Key, internal := KV}) ->
    [command(Meta), " ", Key, [[" ", K, "=", V] || {K, V} <- KV], ?RN];

encode(#{command := error}) ->
    ["ERROR", ?RN];

encode(#{command := touched}) ->
    ["TOUCHED", ?RN];

encode(#{command := client_error, reason := Reason}) ->
    ["CLIENT_ERROR ", Reason, ?RN];

encode(#{command := server_error, reason := Reason}) ->
    ["SERVER_ERROR ", Reason, ?RN];

encode(#{command := stored}) ->
    ["STORED", ?RN];

encode(#{command := not_stored}) ->
    ["NOT_STORED", ?RN];

encode(#{command := exists}) ->
    ["EXISTS", ?RN];

encode(#{command := not_found}) ->
    ["NOT_FOUND", ?RN];

encode(#{command := deleted}) ->
    ["DELETED", ?RN];

encode(#{header := #{magic := request, opcode := Opcode},
         key := _} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(value, Decoded)),
       Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    marshal(Decoded);

encode(#{header := #{magic := response,
                     status := key_not_found,
                     opcode := Opcode}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    marshal(Decoded);

encode(#{header := #{magic := response,
                     status := no_error,
                     opcode := Opcode},
         extra := #{flags := Flags}} = Decoded)
  when Opcode == get;
       Opcode == getq;
       Opcode == getk;
       Opcode == getkq ->
    marshal(Decoded#{extra := <<Flags:32>>});

encode(#{header := #{magic := request, opcode := Opcode},
         key := _,
         extra := #{flags := Flags, expiration := Expiration}} = Decoded)
  when Opcode == set;
       Opcode == add;
       Opcode == replace ->
    marshal(Decoded#{extra := <<Flags:32, Expiration:32>>});

encode(#{header := #{magic := response, opcode := Opcode}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)),
       Opcode == set;
       Opcode == add;
       Opcode == replace ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := Opcode},
         key := _,
         value := _} = Decoded)
  when not(is_map_key(extra, Decoded)),
       Opcode == append;
       Opcode == prepend ->
    marshal(Decoded);

encode(#{header := #{magic := response, opcode := Opcode}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)),
       Opcode == append;
       Opcode == prepend ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := delete},
         key := _} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := flush},
         extra := #{expiration := Expiration}} = Decoded)
  when not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded#{extra := <<Expiration:32>>});

encode(#{header := #{opcode := flush}} = Decoded)
  when not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{opcode := noop}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{opcode := quit}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := version}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := response, opcode := version},
         value := _} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := stat}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(value, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := response, opcode := stat}} = Decoded)
  when not(is_map_key(extra, Decoded)) ->
    marshal(Decoded);

encode(#{header := #{magic := request, opcode := Opcode},
         key := _,
         extra := #{delta := Delta,
                    initial := Initial,
                    expiration := Expiration}} = Decoded)
  when not(is_map_key(value, Decoded)),
       Opcode == increment;
       Opcode == decrement ->
    marshal(Decoded#{extra := <<Delta:64, Initial:64, Expiration:32>>});

encode(#{header := #{magic := response, opcode := Opcode},
         value := _} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
       Opcode == increment;
       Opcode == decrement ->
    marshal(Decoded).


marshal(#{header := #{magic := request,
                      opcode := Opcode,
                      data_type := DataType,
                      vbucket_id := VBucketId,
                      opaque := Opaque,
                      cas := CAS}} = Decoded) ->
    ?FUNCTION_NAME(
       ?REQUEST,
       mcd_opcode:lookup(Opcode),
       DataType,
       VBucketId,
       Opaque,
       CAS,
       maps:without([header], Decoded));

marshal(#{header := #{magic := response,
                      opcode := Opcode,
                      data_type := DataType,
                      status := Status,
                      opaque := Opaque,
                      cas := CAS}} = Decoded) ->
    ?FUNCTION_NAME(
       ?RESPONSE,
       mcd_opcode:lookup(Opcode),
       DataType,
       mcd_status:lookup(Status),
       Opaque,
       CAS,
       maps:without([header], Decoded)).


marshal(Magic,
        Opcode,
        DataType,
        BucketOrStatus,
        Opaque,
        CAS,
        Decoded) ->
    ?FUNCTION_NAME(Magic,
                   Opcode,
                   DataType,
                   BucketOrStatus,
                   Opaque,
                   CAS,
                   maps:get(extra, Decoded, <<>>),
                   maps:get(key, Decoded, <<>>),
                   maps:get(value, Decoded, <<>>)).


marshal(Magic,
        Opcode,
        raw,
        BucketOrStatus,
        Opaque,
        CAS,
        Extra,
        Key,
        Value) ->
    [Magic,
     Opcode,
     <<(byte_size(Key)):16>>,
     byte_size(Extra),
     ?RAW,
     <<BucketOrStatus:16,
       (byte_size(Extra) + byte_size(Key) + byte_size(Value)):32,
        Opaque:32,
        CAS:64>>,
     Extra,
     Key,
     Value].


decode_text_command(Command, Remainder)
  when Command == set;
       Command == cas;
       Command == add;
       Command == replace;
       Command == append;
       Command == prepend ->
    decode_text_storage_command(Command, Remainder);

decode_text_command(Command, Remainder)
  when Command == get;
       Command == gets;
       Command == gat;
       Command == gats ->
    decode_text_retrieval_command(Command, Remainder);

decode_text_command(Command, Remainder)
  when Command == meta_debug;
       Command == meta_debug_reply;
       Command == meta_stored;
       Command == meta_not_found;
       Command == meta_miss;
       Command == meta_not_stored;
       Command == meta_get;
       Command == meta_set;
       Command == meta_delete;
       Command == meta_value ->
    decode_text_meta_command(Command, Remainder);

decode_text_command(delete, R0) ->
    [Parameters, Encoded] = string:split(R0, ?RN),

    {ok, MP} = re:compile("(?<key>[^\\s]+)( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => delete},
               lists:zip(NL, Matches)),
             Encoded}
    end;

decode_text_command(value = Command, R0) ->
    [Key, R1] = string:split(R0, <<" ">>),
    [Flags, R2] = string:split(R1, <<" ">>),
    case string:split(R2, <<" ">>) of
        [R3] ->
            [DataLength, R4] = string:split(R3, ?RN),
            Length = binary_to_integer(DataLength),
            <<Data:Length/bytes, ?RN, Encoded/bytes>> = R4,
            {#{command => Command,
               key => Key,
               flags => binary_to_integer(Flags),
               data => Data},
             Encoded};

        [DataLength, R3] ->
            Length = binary_to_integer(DataLength),
            [CAS, <<Data:Length/bytes, ?RN, Encoded/bytes>>] = string:split(R3, ?RN),
            {#{command => Command,
               key => Key,
               flags => binary_to_integer(Flags),
               cas => binary_to_integer(CAS),
               data => Data},
            Encoded}
    end;

decode_text_command(Command, Remainder)
  when Command == client_error;
       Command == server_error ->
    [Reason, Encoded] = string:split(Remainder, ?RN),
    {#{command => Command, reason => Reason}, Encoded};

decode_text_command(touch = Command, Remainder) ->
    [Parameters, Encoded] = string:split(Remainder, ?RN),

    {ok, MP} = re:compile(
                 "(?<key>[^\\s]+) (?<expiry>[0-9]+)"
                 "( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({<<"expiry">> = Name, Value}, A) ->
                       A#{binary_to_atom(Name) => binary_to_integer(Value)};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => Command},
               lists:zip(NL, Matches)),
             Encoded}
    end;

decode_text_command(stats = Command, Remainder) ->
    [CommandLine, Encoded] = string:split(Remainder, ?RN),
    {maps:filter(
       fun
           (arg, <<>>) ->
               false;

           (_, _) ->
               true
       end,
       re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(?<arg>\\w+)?"})),
     Encoded};

decode_text_command(stat = Command, Remainder) ->
    [CommandLine, Encoded] = string:split(Remainder, ?RN),
    {re_run(
       #{command => Command,
         subject => CommandLine,
         re => "(?<key>\\w+)\\s+(?<value>.+)"}),
     Encoded};

decode_text_command(Command, Remainder)
  when Command == incr;
       Command == decr ->
    [Parameters, Encoded] = string:split(Remainder, ?RN),

    {ok, MP} = re:compile(
                 "(?<key>[^\\s]+) (?<value>[0-9]+)"
                 "( (?<noreply>noreply))?"),
    {namelist, NL} = re:inspect(MP, namelist),

    case re:run(Parameters, MP, [{capture, all_names, binary}]) of
        {match, Matches} ->
            {lists:foldl(
               fun
                   ({<<"noreply">>, Optional}, A) ->
                       A#{noreply => Optional /= <<>>};

                   ({<<"value">> = Name, Value}, A) ->
                       A#{binary_to_atom(Name) => binary_to_integer(Value)};

                   ({Name, Value}, A) ->
                       A#{binary_to_atom(Name) => Value}
               end,
               #{command => Command},
               lists:zip(NL, Matches)),
             Encoded}
    end.


decode_text_storage_command(Command, Remainder)
  when Command == set;
       Command == add;
       Command == replace;
       Command == append;
       Command == prepend ->
    [CommandLine, DataBlock] = string:split(Remainder, ?RN),
    #{bytes := Size} =
        Decoded =
        re_run(
          #{command => Command,
            subject => CommandLine,
            re => "(?<key>\\w+) (?<flags>\\d+) (?<expiry>\\d+) (?<bytes>\\d+)( (?<noreply>noreply))?",
            mapping => #{flags => fun erlang:binary_to_integer/1,
                         expiry => fun erlang:binary_to_integer/1,
                         noreply => fun optional/1,
                         bytes => fun erlang:binary_to_integer/1}}),
    case DataBlock of
        <<Data:Size/bytes, ?RN, Encoded/bytes>> ->
            {maps:without([bytes], Decoded#{data => Data}), Encoded};

        _Otherwise ->
            partial
    end;

decode_text_storage_command(cas = Command, Remainder) ->
    [CommandLine, DataBlock] = string:split(Remainder, ?RN),
    #{bytes := Size} = Decoded = re_run(
                                   #{command => Command,
                                     subject => CommandLine,
                                     re => "(?<key>\\w+) (?<flags>\\d+) (?<expiry>\\d+) (?<bytes>\\d+) (?<unique>\\d+)( (?<noreply>noreply))?",
                                     mapping => #{flags => fun erlang:binary_to_integer/1,
                                                  expiry => fun erlang:binary_to_integer/1,
                                                  unique => fun erlang:binary_to_integer/1,
                                                  noreply => fun optional/1,
                                                  bytes => fun erlang:binary_to_integer/1}}),
    <<Data:Size/bytes, ?RN, Encoded/bytes>> = DataBlock,
    {maps:without([bytes], Decoded#{data => Data}), Encoded}.


optional(Optional) ->
    Optional /= <<>>.


decode_text_retrieval_command(Command, Remainder)
  when Command == get; Command == gets ->
    case string:split(Remainder, ?RN) of
        [Keys, Encoded] ->
            {#{command => Command,
               keys => string:split(string:trim(Keys), <<" ">>, all)},
             Encoded};

        [_] ->
            partial
    end;

decode_text_retrieval_command(Command, Remainder)
  when Command == gat; Command == gats ->
    [CommandLine, Encoded] = string:split(Remainder, ?RN),
    {re_run(#{command => Command,
              subject => CommandLine,
              re => "(?<expiry>[0-9]+)\\s(?<keys>([^\\s]+\\s*)+)",
              mapping => #{expiry => fun erlang:binary_to_integer/1,
                           keys => fun
                                       (Keys) ->
                                           string:split(Keys, " ", all)
                                  end}}),
     Encoded}.

decode_text_meta_command(meta_debug_reply = Command, Remainder) ->
    [CommandLine, Encoded] = string:split(Remainder, ?RN),
    {re_run(#{command => Command,
              subject => CommandLine,
              re => "(?<key>\\w+) (?<internal>(\\w+=\\w+ ?)+)",
              mapping => #{internal => fun decode_text_meta_debug_internal/1}}),
     Encoded};

decode_text_meta_command(Command, Remainder) when Command == meta_stored;
                                                  Command == meta_not_stored;
                                                  Command == meta_not_found ->
    [Flags, Encoded] = string:split(Remainder, ?RN),
    {#{command => Command, flags => mcd_meta_flags:decode(Flags)}, Encoded};

decode_text_meta_command(Command, R0) when Command == meta_debug;
                                           Command == meta_get;
                                           Command == meta_delete ->
    case string:split(R0, <<" ">>) of
        [Key, R1] ->
            [Flags, Encoded] = string:split(R1, ?RN),
            {#{command => Command,
               key => Key,
               flags => mcd_meta_flags:decode(Flags)},
             Encoded};

        [R1] ->
            [Key, Encoded] = string:split(R1, ?RN),
            {#{command => Command, key => Key}, Encoded}
    end;

decode_text_meta_command(meta_set = Command, Remainder) ->
    [CommandLine, DataLine] = string:split(Remainder, ?RN),
    #{datalen := Length} =
        Decoded =
        re_run(
          #{command => Command,
            subject => CommandLine,
            re => "(?<key>\\w+) (?<datalen>\\d+)(?<flags>(\\s+\\w\\d*)*)",
            mapping => #{datalen => fun erlang:binary_to_integer/1,
                         flags => fun mcd_meta_flags:decode/1}}),
    <<Data:Length/bytes, ?RN, Encoded/bytes>> = DataLine,
    {Decoded#{data => Data}, Encoded};

decode_text_meta_command(meta_value = Command, R0) ->
    [DataLength, R1] = string:split(R0, <<" ">>),
    Length = binary_to_integer(DataLength),
    [Flags, <<Data:Length/bytes, ?RN, Encoded/bytes>>] = string:split(R1, ?RN),
    {#{command => Command,
       flags => mcd_meta_flags:decode(Flags),
       data => Data},
     Encoded}.


re_run(#{subject := Subject, re := RE} = Arg) ->
    {ok, MP} = re:compile(RE),
    {namelist, NL} = re:inspect(MP, namelist),
    {match, Matches} = re:run(
                         Subject,
                         MP,
                         maps:get(
                           options,
                           Arg,
                           [{newline, crlf},
                            {capture, all_names, binary}])),
    lists:foldl(
      fun
          ({K, V}, A) ->
              Key = binary_to_existing_atom(K),
              A#{Key => case maps:find(
                               Key,
                               maps:get(mapping, Arg, #{})) of
                            {ok, Mapper} ->
                                Mapper(V);

                            error ->
                                V
                        end}
      end,
      maps:get(acc0, Arg, maps:with([command], Arg)),
      lists:zip(NL, Matches)).


decode_text_meta_debug_internal(Encoded) ->
    {match, Matches} = re:run(Encoded,
                              "(?<k>\\w+)=(?<v>\\w+)\s?",
                              [global,
                               {capture, all_names, binary}]),
    [list_to_tuple(Match) || Match <- Matches].


command(meta_debug) ->
    "me";
command(meta_debug_reply) ->
    "ME";
command(meta_get) ->
    "mg";
command(meta_set) ->
    "ms";
command(meta_delete) ->
    "md".
