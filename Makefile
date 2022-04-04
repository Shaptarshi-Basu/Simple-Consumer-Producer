integration-test:
				docker-compose down && docker-compose up -d && go test && docker-compose down