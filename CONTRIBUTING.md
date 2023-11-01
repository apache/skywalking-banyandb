# Contributing to Apache SkyWalking BanyanDB

Firstly, thanks for your interest in contributing! We hope that this will be a
pleasant first experience for you, and that you will return to continue
contributing.

## Code of Conduct

The Apache software Foundation's [Code of Conduct](http://www.apache.org/foundation/policies/conduct.html) governs this project, and everyone participating in it.
By participating, you are expected to adhere to this code. If you are aware of unacceptable behavior, please visit the
[Reporting Guidelines page](http://www.apache.org/foundation/policies/conduct.html#reporting-guidelines)
and follow the instructions there.

## How to contribute?

Most of the contributions that we receive are code contributions, but you can
also contribute to the documentation or report solid bugs
for us to fix.

## How to report a bug?

* **Ensure no one did report the bug** by searching on GitHub under [Issues](https://github.com/apache/skywalking/issues).

* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/apache/skywalking/issues/new).
Be sure to include a **title and clear description**, as much relevant information as possible,
and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## How to add a new feature or change an existing one

_Before making any significant changes, please [open an issue](https://github.com/apache/skywalking/issues)._
Discussing your proposed changes ahead of time will make the contribution process smooth for everyone.

Once we've discussed your changes and you've got your code ready, make sure that tests are passing and open your pull request. Your PR is most likely to be accepted if it:

* Update the README.md with details of changes to the interface.
* Includes tests for new functionality.
* References the original issue in the description, e.g., "Resolves #123".
* Has a [good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

## Requirements

Users who want to build a binary from sources have to set up:

* Go 1.21
* Node 20.9
* Git >= 2.30
* Linux, macOS or Windows+WSL2
* GNU make

### Windows

BanyanDB is built on Linux and macOS that introduced several platform-specific characters to the building system. Therefore, we highly recommend you use [WSL2+Ubuntu](https://ubuntu.com/wsl) to execute tasks of the Makefile.

#### End of line sequence

BanyanDB ALWAYS uses `LF`(`\n`) as the line endings, even on Windows. So we need your development tool and IDEs to generate new files with `LF` as its end of lines.

## Building and Testing

Clone the source code and check the necessary tools by

```shell
make check-req
```

Once the checking passes, you should generate files for building by

```shell
make generate
```

Finally, run `make build` in the source directory, which will build the default binary file in `<sub_project>/build/bin/`.

```shell
make build
```

Please refer to the [installation](./docs/installation.md) for more details.

Test your changes before submitting them by

```shell
make test
```

## Linting your codes

We have some rules for the code style and please lint your codes locally before opening a pull request.

```shell
make lint
```

If you found some errors in the output of the above command, try to `make format` to fix some obvious style issues. As for the complicated errors, please correct them manually.

## Update licenses

If you import new dependencies or upgrade an existing one, trigger the licenses generator
to update the license files.

```shell
make license-dep 
```

> Caveat: This task is a step of `make pre-push`. You can run it to update licenses.

## Test your changes before pushing

After you commit the local changes, we have a series of checking tests to verify your changes by

```shell
git commit
make pre-push
```

Please fix any errors raised by the above command.
