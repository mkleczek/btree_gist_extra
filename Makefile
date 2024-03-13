MODULE_big = btree_gist_extra
EXTENSION = btree_gist_extra
DATA = btree_gist_extra--1.0.0.sql
OBJS += btree_gist_extra.o
#DOCS = README.isbn_issn
#HEADERS_isbn_issn = isbn_issn.h

PG_CONFIG = pg_config
PGXS := $(shell $(PG_CONFIG) --pgxs)
PG_LDFLAGS += -L$(shell $(PG_CONFIG) --pkglibdir) #-lbtree_gist
#PG_LIBS += btree_gist
#SHLIB_LINK += btree_gist.dylib
include $(PGXS)
ifeq ($(PORTNAME), darwin)
override CFLAGS += -undefined dynamic_lookup
#override LDFLAGS += -dylib_file libbtree_gist.dylib:btree_gist.dylib
endif
