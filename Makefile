#!/usr/bin/make

PREFIX=$(shell pwd)

#Sources
BOSSLite_SRC=$(shell pwd)/

# Targets
LIB_DIR=$(PREFIX)/lib
SHARE_DIR=$(PREFIX)/share


.PHONY: all
.PHONY: install

build:
	python setup.py build --build-lib=$(LIB_DIR)
	/bin/mkdir -p $(SHARE_DIR)	
	/bin/cp $(BOSSLite_SRC)/src/python/ProdCommon/BossLite/DbObjects/*.sql  $(SHARE_DIR)
	/bin/chmod +x $(LIB_DIR)/ProdCommon/BossLite/Scheduler/GLiteStatusQuery.py
setup:
	/bin/mkdir -p $(LIB_DIR)

install: setup build

all: setup build


clean:
	/bin/rm -rf $(LIB_DIR)/*
