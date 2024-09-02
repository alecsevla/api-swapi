#install dependencies
install-deps:
	pip install -r requirements.txt

#run pipeline
run-pipeline:
	python data_pipeline/pipeline.py

#run unit tests
run-test:
	python -m unittest data_pipeline/tests.py

#run API
run-api:
	python api/app.py

#clean temporary files
clean:
	rm -f data_pipeline/starwars.db
	rm -rf __pycache__