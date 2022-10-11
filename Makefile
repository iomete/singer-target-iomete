venv:
	python3 -m venv venv ;\
	. ./venv/bin/activate ;\
	pip install --upgrade pip setuptools wheel ;\
	pip install -e .[test]

pylint:
	. ./venv/bin/activate ;\
	pylint --rcfile pylintrc singer_target_iomete/


docker-build:
	docker build -t singer-target-iomete .