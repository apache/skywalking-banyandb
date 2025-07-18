# Debugging

## Debugging Unit Tests

## Debugging Integration Tests

## Debugging E2E Tests
1. Start BanyanDB in debug mode.
2. From the root directory of the project, run `e2e run -c test/debug/e2e.yaml` to start oap, agent, provider and consumer.
3. Use `docker ps` to check the exposed port of oap.
4. Use `swctl` to query oap. For example:
	```
	swctl --display yaml --base-url=http://localhost:<port>/graphql logs list --service-name=e2e-service-provider
	```
   Replace `<port>` with the actual port number.
