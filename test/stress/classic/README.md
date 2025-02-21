# Stress Test - Classic

This is a stress test for the classic version of the application. It is designed to test the application's ability to handle a large number of requests.

## Build BanyanD and Bydbctl

Before running the stress test, you need to build the BanyanD and Bydbctl binaries. You can do this by running the following command:

```bash
make build-server
```

## Running the Stress Test in Development Mode

To run the stress test in development mode, you can use the following command:

```bash
make dev-up
```

## Running the Stress Test in Production Mode

To run the stress test in production mode, you can use the following command:

```bash
make up
```
