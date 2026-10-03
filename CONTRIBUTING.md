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

* Go 1.25.13
* Node.js >= 24.6.0
* Git >= 2.30
* Linux, macOS or Windows + WSL2
* GNU make
* Docker, for the canonical build of committed artifacts (see [Update licenses](#update-licenses))

The Go and Node versions above are derived from `go.mod` and `.node-version` respectively;
`make check-node-version` fails if the two ever disagree with what a project declares in
its `package.json`.

### Windows

BanyanDB is built on Linux and macOS that introduced several platform-specific characters to the building system. Therefore, we highly recommend you use [WSL2+Ubuntu](https://ubuntu.com/desktop/wsl) to execute tasks of the Makefile.

#### End of line sequence

BanyanDB ALWAYS uses `LF`(`\n`) as the line endings, even on Windows. So we need your development tool and IDEs to generate new files with `LF` as its end of lines.

Git enforces this for you: `.gitattributes` declares `* text=auto eol=lf`, so a checkout
produces LF bytes on every platform no matter what your `core.autocrlf` is set to. There is
nothing to configure. If a file does end up with CRLF, `make check-license-outputs` will
name it.

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

Please refer to the [installation](./docs/installation/binaries.md#Build-From-Source) for more details.

Test your changes before submitting them by

```shell
make test
```

### Testing in Docker with Constrained Resources

To test your changes in a controlled environment with limited resources (useful for catching race conditions and resource-related issues), use the `test-docker` target:

```shell
# Test a specific package
make test-docker PKG=./banyand/trace

# Test all packages (default)
make test-docker
```

This target runs tests in a Docker container with:
- **2 CPU cores** - Helps expose concurrency issues
- **4GB RAM** - Tests memory constraints
- **Race detector enabled** - Detects data races
- **Go version from go.mod** - Ensures version consistency

The Docker test environment automatically uses the Go version specified in `go.mod`, ensuring consistency between local development and CI environments.

## Linting your codes

We have some rules for the code style and please lint your codes locally before opening a pull request.

```shell
make lint
```

If you found some errors in the output of the above command, try to `make format` to fix some obvious style issues. As for the complicated errors, please correct them manually.

## AI-Assisted Development

If you're using AI assistants (like Claude, Cursor, GitHub Copilot, etc.) to help with code generation, please refer to our [AI Coding Guidelines](AGENTS.md) to ensure the generated code follows our project's coding standards and linting rules.

The guidelines cover:
- Variable shadowing prevention
- Import organization and aliases
- Error handling patterns
- Code style and documentation standards
- Common patterns to avoid and preferred alternatives

## Update licenses

If you import new dependencies or upgrade an existing one, trigger the licenses generator
to update the license files.

```shell
make docker-license-dep
```

This is the canonical command, on every operating system. It runs the generator in a pinned,
digest-verified build environment, so the license files it produces are byte-identical regardless of
which host you build on: the Go and Node versions, the package index and the module cache all live
inside the image rather than on your machine. The source tree is streamed into the container over a
pipe and only the generated license files come back, so your `node_modules` and `bin/` are left
alone and your line endings cannot reach the output.

**On Windows this works natively.** The image is Linux, but Docker Desktop runs Linux containers
directly on Windows, so a PowerShell or cmd session with `docker` and GNU `make` is enough; the
target also needs a `tar` with `--exclude`, which is the bsdtar that ships with Windows 10+. WSL2 is
the smoother option because it additionally gives you GNU make and GNU tar without extra installs,
and it remains the recommended way to build BanyanDB for the other reasons already described above.
Two notes:

- The container is not run as your uid on Windows, because Windows filesystems have no uid
  ownership; that is not a loss, because there is nothing for it to protect.
- The post-generation `check-license-outputs` is a bash script. Without WSL2 or Git Bash it is
  skipped with a notice rather than failing your build — CI runs it unconditionally, and either of
  those shells can run it locally.

This is enforced, not merely recommended: CI regenerates the artifacts with this command and fails if
the result differs from what is committed. If you commit license files produced any other way and
they differ, the build breaks.

If you would rather not use Docker, the native target still works:

```shell
make license-dep
```

It produces the same bytes **when your Go and Node match the pinned versions** — that is the one
thing the container guarantees and a native run cannot. Both paths normalize CRLF to LF, CI runs
both and compares the two manifests, and `make check-license-outputs` verifies the committed bytes,
so a divergence is caught rather than discovered later.

### The build system is checked on all three operating systems

CI runs the build system's own checks — the Node resolver, the license verifier in both its passing
and failing modes, and a single cheap container run that builds the pinned image and confirms it
carries the versions this repository declares — on Linux, macOS and Windows. It deliberately does
*not* regenerate the license artifacts on macOS or Windows: that is the expensive part, and the
ubuntu job already covers it. What this catches is the thing you would hit first as a contributor
there — a command from this document that does not work on your machine, or a check that silently
does nothing.

So if `make docker-license-dep` or `make check-license-outputs` fails for you on macOS or Windows,
that is a bug in the build system rather than in your setup, and the workflow
(`.github/workflows/test-build-system.yml`) is where it should be fixed. `shell: bash` throughout is
deliberate: the verifier is a shell script, so on Windows it runs under Git Bash rather than cmd.

To verify the license files in your worktree without regenerating them:

```shell
make check-license-outputs
```

This compares the raw bytes on disk against the committed blobs, checks that no
generated file is missing or untracked, and fails on any CRLF. It deliberately does not
use `git diff`, which compares blobs after Git's own normalization and cannot see a file
that was generated and then deleted.

> Caveat: This task is a step of `make pre-push`. You can run it to update licenses.

## Test your changes before pushing

After you commit the local changes, we have a series of checking tests to verify your changes by

```shell
git commit
make pre-push
```

Please fix any errors raised by the above command.
