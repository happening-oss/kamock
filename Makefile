PROJECT = kamock
PROJECT_DESCRIPTION = Mock Kafka Broker
PROJECT_VERSION = $(shell scripts/git-vsn)

compile:
	rebar3 compile

GNU_TAR ?= gtar
ARCHIVE := ../$(PROJECT)-$(PROJECT_VERSION).tar

archive:
	sed -i .bak 's,git@github\.com:.*/.*kafcod,https://github.com/happening-oss/kafcod,' rebar.config
	sed -i .bak 's,git@github\.com:.*/.*kamock,https://github.com/happening-oss/kamock,' README.md
	sed -i .bak 's,master,main,' README.md
	$(GNU_TAR) -c -f $(ARCHIVE) --exclude-from .archive-exclude .
