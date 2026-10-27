PYTHON ?= .venv/bin/python
PIP ?= .venv/bin/pip
export PYTHONPATH := src

.PHONY: setup generate pipeline test lint format dashboard diagrams clean

setup:
	python3 -m venv .venv
	$(PIP) install --upgrade pip
	$(PIP) install -r requirements-dev.txt

generate:
	$(PYTHON) -m hc_platform.cli generate --scale small --output data/sample --seed 42

generate-tiny:
	$(PYTHON) -m hc_platform.cli generate --scale tiny --output data/landing/tiny --seed 42

pipeline:
	$(PYTHON) -m hc_platform.cli run --env dev

test:
	$(PYTHON) -m pytest tests --cov=hc_platform --cov-report=term-missing

lint:
	$(PYTHON) -m ruff check src tests dashboard
	$(PYTHON) -m ruff format --check src tests dashboard

format:
	$(PYTHON) -m ruff format src tests dashboard
	$(PYTHON) -m ruff check --fix src tests dashboard

dashboard:
	$(PYTHON) -m hc_platform.cli dashboard --port 8501

diagrams:
	$(PYTHON) scripts/render_diagrams.py

clean:
	rm -rf data/lake data/logs data/lake-test data/logs-test .pytest_cache .ruff_cache htmlcov .coverage
