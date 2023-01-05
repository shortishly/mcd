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


-module(mcd).


-export([priv_consult/1]).
-export([priv_dir/0]).
-export([start/0]).
-export_type([int16/0]).
-export_type([int32/0]).
-export_type([int64/0]).
-export_type([int8/0]).
-export_type([protocol/0]).
-export_type([uint16/0]).
-export_type([uint32/0]).
-export_type([uint64/0]).
-export_type([uint8/0]).


-type int16() :: -32_768..32_767.
-type int32() :: -2_147_483_648..2_147_483_647.
-type int64() :: -9_223_372_036_854_775_808..9_223_372_036_854_775_807.
-type int8() :: -128..127.
-type uint16() :: 0..65_535.
-type uint32() :: 0..4_294_967_295.
-type uint64() :: 0..18_446_744_073_709_551_615.
-type uint8() :: 0..255.


-type protocol() :: text_protocol()
                  | meta_protocol()
                  | binary_protocol().


-type text_protocol() :: #{command := text_command()}.


-type text_command() :: set
                      | add
                      | get
                      | replace
                      | append
                      | prepend.

-type meta_command() :: mg
                      | ms
                      | md.

-type meta_protocol() :: #{meta := meta_command(),
                           key := binary(),
                           flags := [meta_flag()]}.

-type meta_flag() :: base64
                   | cas
                   | {cas, uint32()}
                   | flags
                   | {flags, uint32()}
                   | hit
                   | invalidate
                   | key_as_token
                   | last_accessed
                   | {mode, binary()}
                   | {vivify, uint32()}
                   | {opaque, binary()}
                   | noreply
                   | {remaining, uint32()}
                   | size
                   | ttl
                   | {ttl, uint32()}
                   | dont_bump_lru
                   | value.

-type binary_protocol() :: #{header := binary_header(),
                             extra => any(),
                             key => binary(),
                             value => binary()}.

-type binary_header() :: binary_request_header()
                       | binary_response_header().

-type binary_request_header() :: #{magic := binary_magic(),
                                   opcode := binary_opcode(),
                                   data_type := binary_data_type(),
                                   vbucket_id := uint32(),
                                   opaque := uint32(),
                                   cas := uint32()}.

-type binary_response_header() :: #{magic := binary_magic(),
                                    opcode := binary_opcode(),
                                    data_type := binary_data_type(),
                                    status := binary_status(),
                                    opaque := uint32(),
                                    cas := uint32()}.

-type binary_magic() :: request | response.

-type binary_data_type() :: raw.

-type binary_opcode() :: get
                       | set
                       | add
                       | replace
                       | delete
                       | increment
                       | decrement
                       | quit
                       | flush
                       | getq
                       | no_op
                       | version
                       | getk
                       | getkq
                       | append
                       | prepend
                       | stat
                       | setq
                       | addq
                       | replaceq
                       | deleteq
                       | incrementq
                       | decrementq
                       | quitq
                       | flushq
                       | appendq
                       | prependq
                       | verbosity
                       | touch
                       | gat
                       | gatq
                       | sasl_list_mechs
                       | sasl_auth
                       | sasl_step
                       | rget
                       | rset
                       | rsetq
                       | rappend
                       | rappendq
                       | rprepend
                       | rprependq
                       | rdelete
                       | rdeleteq
                       | rincr
                       | rincrq
                       | rdecr
                       | rdecrq
                       | set_vbucket
                       | get_vbucket
                       | del_vbucket
                       | tap_connect
                       | tap_mutation
                       | tap_delete
                       | tap_flush
                       | tap_opaque
                       | tap_vbucket_set
                       | tap_checkpoint_start
                       | tap_checkpoint_end.

-type binary_status() :: no_error
                       | key_not_found
                       | key_exists
                       | value_too_large
                       | invalid_arguments
                       | item_not_stored
                       | incrdecr_on_nonnumeric_value
                       | the_vbucket_belongs_to_another_server
                       | authentication_error
                       | authentication_continue
                       | unknown_command
                       | out_of_memory
                       | not_supported
                       | internal_error
                       | busy
                       | temporary_failure.


start() ->
    application:ensure_all_started(?MODULE).


priv_dir() ->
    code:priv_dir(?MODULE).


priv_consult(Filename) ->
    phrase_file:consult(filename:join(priv_dir(), Filename)).
