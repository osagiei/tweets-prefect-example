build:
	docker build -t twitter_analysis .

run:
	docker run -v $(PWD)/data:/mnt/data twitter_analysis

clean:
	docker rmi -f twitter_analysis
	# rm -rf twitter_analysis.db

