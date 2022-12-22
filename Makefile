install:
	poetry install

update:
	poetry update

build:
	poetry build

publish:
	poetry publish

pip:
	poetry export --without-hashes --format=requirements.txt > requirements.txt && \
	pip install --upgrade pip && \
	pip install -r requirements.txt && \
	rm -rf requirements.txt