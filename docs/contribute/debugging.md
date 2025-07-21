# Debugging

## Debugging Unit Tests

## Debugging Integration Tests

## Debugging E2E Tests
1. Start BanyanDB in debug mode.
2. From the root directory of the project, run `e2e run -c test/debug/e2e.yaml` to start oap, agent, provider and consumer and ingest data into BanyanDB.
3. Use `docker ps` to check the exposed port of oap.
4. Use `swctl` to query oap. For example:
	```
	export PATH=/tmp/skywalking-infra-e2e/bin:$PATH
	swctl --display yaml --base-url=http://localhost:<port>/graphql logs list --service-name=e2e-service-provider
	```
   Replace `<port>` with the actual port number.
5. Execute `e2e cleanup -c test/debug/e2e.yaml` to clean up the environment.
