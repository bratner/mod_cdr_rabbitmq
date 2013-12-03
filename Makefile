#LOCAL_CFLAGS=-I./driver/src
LOCAL_INSERT_LDFLAGS=echo -lrabbitmq;
include ../../../../build/modmake.rules
