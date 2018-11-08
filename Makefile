MODULE_big = median
EXTENSION = median
DATA = median--1.0.sql
DOCS = README.md
EXTRA_CLEAN = *~ median.tar.gz
REGRESS := median
PG_USER = postgres
REGRESS_OPTS := \
	--load-extension=$(EXTENSION) \
	--user=$(PG_USER) \
	--inputdir=test \
	--outputdir=test \

SRCS = median.c
OBJS = $(patsubst %.c,%.o,$(SRCS))

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
include $(PGXS)

.PHONY: tarball

median.tar.gz: $(SRCS) Makefile README.md median--1.0.sql test/sql/median.sql test/expected/median.out median.control
	tar -zcvf $@ $^

tarball: median.tar.gz
