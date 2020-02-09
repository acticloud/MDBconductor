

ITEMS=$(shell seq 1 10)

default: $(ITEMS)

%:
	curl http://localhost:8080
