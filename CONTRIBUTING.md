# Contributing to Apache SkyWalking Cloud on Kubernetes

Firstly, thanks for your interest in contributing! We hope that this will be a
pleasant first experience for you, and that you will return to continue
contributing.

## Code of Conduct

This project and everyone participating in it is governed by the Apache
software Foundation's [Code of Conduct](http://www.apache.org/foundation/policies/conduct.html).
By participating, you are expected to adhere to this code. If you are aware of unacceptable behavior, please visit the
[Reporting Guidelines page](http://www.apache.org/foundation/policies/conduct.html#reporting-guidelines)
and follow the instructions there.

## How to contribute?

Most of the contributions that we receive are code contributions, but you can
also contribute to the documentation or simply report solid bugs
for us to fix.

## How to report a bug?

* **Ensure the bug was not already reported** by searching on GitHub under [Issues](https://github.com/apache/skywalking/issues).

* If you're unable to find an open issue addressing the problem, [open a new one](https://github.com/apache/skywalking/issues/new).
Be sure to include a **title and clear description**, as much relevant information as possible,
and a **code sample** or an **executable test case** demonstrating the expected behavior that is not occurring.

## How to add a new feature or change an existing one

_Before making any significant changes, please [open an issue](https://github.com/apache/skywalking/issues)._
Discussing your proposed changes ahead of time will make the contribution process smooth for everyone.

Once we've discussed your changes and you've got your code ready, make sure that tests are passing and open your pull request. Your PR is most likely to be accepted if it:

* Update the README.md with details of changes to the interface.
* Includes tests for new functionality.
* References the original issue in description, e.g. "Resolves #123".
* Has a [good commit message](http://tbaggery.com/2008/04/19/a-note-about-git-commit-messages.html).

## Compiling and building
Clone the source code and simply run `make` in the source directory,
this will download all necessary dependencies and run tests, lint, and build three binary files in `./bin/`, for Windows, Linux, MacOS respectively.

```shell
make
```

## Linting your codes
We have some rules for the code style and please lint your codes locally before opening a pull request

```shell
make lint
```

if you found some errors in the output of the above command, try `make format` to fix some obvious style issues, as for the complicated errors, please fix them manually.

## Checking license
The Apache Software Foundation requires every source file to contain a license header, run `make license` to check that there is license header in every source file.

```shell
make license
``` 

## How to release
This section guides committers and PMC members to release SkyWalking Cloud on Kubernetes in Apache Way.

### Prerequisites
- [x] [GNU Make](https://www.gnu.org/software/make/manual/make.html) is installed
- [x] [GPG tool](https://gpgtools.org) is installed
- [x] [Add your GPG key](docs/release.md#add-your-gpg-public-key)

### Release steps
- Export the version that is to be released, `export VERSION=0.1.0 `
- Tag the latest commit that is to be released with `git tag ${VERSION}` and push the tag with `git push https://github.com/apache/skywalking-swck ${VERSION}`
- Verify licenses, build and sign distribution packages, simply run `make release`, distribution packages and checksums are generated
- [Upload the packages to SVN repository](docs/release.md#upload-to-apache-svn) 
- [Send internal announcement](docs/release.md#make-the-internal-announcements)
- [Wait at least 48 hours for test responses](docs/release.md#wait-at-least-48-hours-for-test-responses)
- [Call for vote](docs/release.md#call-a-vote-in-dev)
- [Publish release](docs/release.md#publish-release)
