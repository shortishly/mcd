#-*- mode: makefile-gmake -*-
# Copyright (c) 2022 Peter Morgan <peter.james.morgan@gmail.com>
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
PROJECT = mcd
PROJECT_DESCRIPTION = memcache protocol
PROJECT_VERSION = ${shell git describe --tags}

COVER = 1
COVER_REPORT_DIR = _site/cover
CT_LOGS_DIR = _site/ct

DEPS = \
	envy \
	phrase \
	recon \
	telemetry

BUILD_DEPS += relx
RELX_TAR = 0

SHELL_DEPS = \
	beaming \
	sync

SHELL_OPTS = \
	-config dev.config \
	-s $(PROJECT) \
	-s sync \
	+pc unicode

PLT_APPS = \
	any \
	asn1 \
	bbmustache \
	compiler \
	crypto \
	inets \
	mnesia \
	phrase \
	public_key \
	runtime_tools \
	ssl \
	stdlib \
	syntax_tools \
	telemetry \
	tools \
	xmerl

EDOC_OPTS = {preprocess, true}, {dir, "_site/edoc"}

dep_beaming = git https://github.com/shortishly/beaming.git
dep_envy = git https://github.com/shortishly/envy.git
dep_phrase = git https://github.com/shortishly/phrase.git
dep_telemetry = git https://github.com/beam-telemetry/telemetry.git

dep_beaming_commit = 0.2.0
dep_envy_commit = 0.9.2
dep_phrase_commit = 0.2.1
dep_recon_commit = 2.5.4
dep_telemetry_commit = v1.1.0


include $(if $(ERLANG_MK_FILENAME),$(ERLANG_MK_FILENAME),erlang.mk)


app:: rebar.config
