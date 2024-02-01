.PHONY: help clean dev docs package test

help:
	@echo "The following make targets are available:"
	@echo "	 devenv		create venv and install all deps for dev env (assumes python3 cmd exists)"
	@echo "	 dev 		install all deps for dev env (assumes venv is present)"
	@echo "  docs		create pydocs for all relveant modules (assumes venv is present)"
	@echo "	 package	package for pypi"
	@echo "	 test		run all tests with coverage (assumes venv is present)"

devenv:
	pip3 install -r requirements.txt
	fugue-jupyter install startup
	pre-commit install

dev:
	pip3 install -r requirements.txt

docs:
	rm -rf docs/api
	rm -rf docs/build
	sphinx-apidoc --no-toc -f -t=docs/_templates -o docs/api fugue_bigquery/
	sphinx-build -b html docs/ docs/build/

lint:
	pre-commit run --all-files

package:
	rm -rf dist/*
	python3 setup.py sdist
	python3 setup.py bdist_wheel

test:
	python3 -bb -m pytest tests/

testbq:
	python3 -bb -m pytest tests/fugue_bigquery --cov=fugue_bigquery

trinodocker:
	docker run --name trino -d -p 8181:8080 trinodb/trino

testtrino:
	python3 -bb -m pytest tests/fugue_trino --cov=fugue_trino

testsf:
	python3 -bb -m pytest tests/fugue_snowflake --cov=fugue_snowflake

lab:
	mkdir -p tmp
	pip install .
	jupyter lab --port=8000 --ip=0.0.0.0 --no-browser --allow-root --NotebookApp.token='' --NotebookApp.password='' --NotebookApp.allow_origin='*'
