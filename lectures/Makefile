.PHONY: help list lecture sync build

# Корень для целей внутри lectures/.
LECTURES_DIR := $(CURDIR)
REPO_ROOT := $(CURDIR)/..

help:
	@echo "make list                                   - вывести дерево лекций"
	@echo "make lecture L=<path>                       - запустить make в директории лекции"
	@echo "  пример: make lecture L=01-foundations/01-01-architecture-and-kraft"
	@echo "make sync                                   - go work sync"
	@echo "make build                                  - собрать все workspace-модули"

list:
	@find . -mindepth 2 -maxdepth 3 -name 'README.md' -not -path './internal/*' \
	  | sed -e 's|^./||' -e 's|/README.md$$||' \
	  | sort

lecture:
ifndef L
	$(error L is required: make lecture L=01-foundations/01-01-architecture-and-kraft)
endif
	@test -d "$(LECTURES_DIR)/$(L)" || (echo "lecture not found: $(L)"; exit 1)
	$(MAKE) -C "$(LECTURES_DIR)/$(L)"

sync:
	go work sync

# Собирает каждый модуль из go.work отдельно — `go build ./...` из workspace
# root в Go 1.26 ругается «directory prefix . does not contain modules».
build:
	@for d in $$(go list -f '{{.Dir}}' -m); do \
	  echo "==> $$d"; \
	  (cd "$$d" && go build ./...) || exit 1; \
	done
