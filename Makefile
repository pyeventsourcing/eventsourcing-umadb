.EXPORT_ALL_VARIABLES:

PYTHONUNBUFFERED=1
POETRY_VERSION=2.2.1
POETRY ?= poetry@$(POETRY_VERSION)

.PHONY: install-poetry
install-poetry:
	@pipx install --suffix="@$(POETRY_VERSION)" "poetry==$(POETRY_VERSION)"
	$(POETRY) --version

.PHONY: install
install:
	$(POETRY) sync --all-extras $(opts)

.PHONY: update
update: update-lock install

.PHONY: update-lock
update-lock:
	$(POETRY) update --lock -v

.PHONY: lint-black
lint-black:
	$(POETRY) run black --check --diff .

.PHONY: lint-isort
lint-isort:
	$(POETRY) run isort --check-only --diff .

.PHONY: lint-mypy
lint-mypy:
	$(POETRY) run mypy

.PHONY: lint-python
lint-python: lint-black lint-isort lint-mypy

.PHONY: lint
lint: lint-python

.PHONY: fmt-black
fmt-black:
	$(POETRY) run black .

.PHONY: fmt-isort
fmt-isort:
	$(POETRY) run isort .

.PHONY: fmt
fmt: fmt-black fmt-isort

.PHONY: test
test:
	$(POETRY) run python -m unittest discover tests -v

.PHONY: benchmark-umadb
benchmark-umadb:
	TEST_BENCHMARK_NUM_ITERS=30 $(POETRY) run python -m unittest tests.test_umadb.TestUmaDbClient

.PHONY: build
build:
	$(POETRY) build

.PHONY: publish
publish:
	$(POETRY) publish

.PHONY: start-umadb-docker
start-umadb-docker:
	docker run --rm -d --name my-umadb -p 50051:50051 umadb/umadb:latest
	sleep 1


.PHONY: stop-umadb-docker
stop-umadb-docker:
	docker stop my-umadb

.PHONY: start-umadb
start-umadb:
	UMADB_PAGE_CACHE_MAX_MB=2000 $(POETRY) run umadb > umadb.log 2>&1 & echo $$! > umadb.pid
	sleep 1
	cat umadb.log

.PHONY: stop-umadb
stop-umadb:
	kill -15 `cat umadb.pid`
	rm -f umadb.pid
