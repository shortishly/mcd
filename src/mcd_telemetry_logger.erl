%% Copyright (c) 2023 Peter Morgan <peter.james.morgan@gmail.com>
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


-module(mcd_telemetry_logger).


-export([handle/4]).
-include_lib("kernel/include/logger.hrl").

%% An example telemetry handler
handle(EventName, Measurements, Metadata, Config) ->
    ?LOG_INFO(#{event_name => EventName,
                measurements => Measurements,
                metadata => Metadata,
                config => Config}).
