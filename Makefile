CXX=c++
RM=rm -f
CPPFLAGS=-O3 -std=c++17 -Wall
LDLIBS=-larrow -lparquet -lpq

SRCS=main.cc pg2arrow.cc
OBJS=$(subst .cc,.o,$(SRCS))

all: pg2arrow

pg2arrow: $(OBJS)
	$(CXX) -o pg2arrow $(OBJS) $(LDLIBS)

depend: .depend

.depend: $(SRCS)
	$(RM) ./.depend
	$(CXX) $(CPPFLAGS) -MM $^>>./.depend;

clean:
	$(RM) $(OBJS)

distclean: clean
	$(RM) *~ .depend
	$(RM) pg2arrow

include .depend
