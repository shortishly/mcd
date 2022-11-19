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


-module(mcd_protocol_binary).


-export([decode/1]).
-export([encode/1]).
-include("mcd.hrl").


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
     Remainder}.




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

%% no op

decode_binary(#{opcode := no_op} =  Header, <<>>, <<>>, <<>>) ->
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

encode(#{header := #{magic := response, opcode := delete}} = Decoded)
  when not(is_map_key(extra, Decoded)),
       not(is_map_key(key, Decoded)),
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

encode(#{header := #{opcode := no_op}} = Decoded)
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
