# Error Checklist

When facing issues with BanyanDB, follow this checklist to effectively troubleshoot and resolve errors.

## 1. Collect Information

Gather detailed information about the error to assist in diagnosing the issue:

- **Logs**: Collect relevant log files from the BanyanDB system. See the [Logging](../observability.md#logging) section for more information.
- **Query Tracing**: If the error is related to a query, enable query tracing to capture detailed information about the query execution. See the [Query Tracing](../observability.md#query-tracing) section for more information.
- **Environment**: Document the environment details, including OS, BanyanDB version, and hardware specifications.
- **Database Schema**: Provide the schema details related to the error. See the [Schema Management](../../interacting/bydbctl/schema/) section for more information.
- **Data Sample**: If applicable, include a sample of the data causing the error.
- **Configuration Settings**: Share the relevant configuration settings.
- **Data Files**: Attach any relevant data files. See the [Configuration](../configuration.md) section to find where the date files is stored.
- **Reproduction Steps**: Describe the steps to reproduce the error.

## 2. Define Error Type

Classify the error to streamline the troubleshooting process:

- **Configuration Error**: Issues related to incorrect configuration settings.
- **Network Error**: Problems caused by network connectivity.
- **Performance Error**: Slowdowns or high resource usage.
- **Data Error**: Inconsistencies or corruption in stored data.

## 3. Error Support Procedure

Follow this procedure to address the identified error type:

- Identify the error type based on the collected information.
- Refer to the relevant sections in the documentation to troubleshoot the error.
- Refer to the issues section in the SkyWalking repository for known issues and solutions.
- If the issue persists, submit a discussion in the [SkyWalking Discussion](https://github.com/apache/skywalking/discussions) for assistance.
- You can also raise a bug report in the [SkyWalking Issue Tracker](https://github.com/apache/skywalking/issues) if the issue is not resolved.
- Finally, As a OpenSource project, you could try to fix the issue by yourself and submit a pull request.

Here's an expanded section on common issues for your BanyanDB troubleshooting document:

- [Troubleshooting Crash Issues](./crash.md)
- [Troubleshooting Overhead Issues](./overhead.md)
- [Troubleshooting No Data Issues](./no-data.md)
- [Troubleshooting Query Issues](./query.md)
- [Troubleshooting Installation Issues](./install.md)
