# Apache SkyWalking BanyanDB release guide

This documentation guides the release manager to release the SkyWalking BanyanDB in the Apache Way, and also helps people to check the release for vote.

## Prerequisites

1. Close(if finished, or move to next milestone otherwise) all issues in the current milestone from [skywalking-banyandb](https://github.com/apache/skywalking-banyandb/milestones) and [skywalking](https://github.com/apache/skywalking/milestones), create a new milestone if needed.
2. Update [CHANGES.md](../CHANGES.md).


## Add your GPG public key to Apache svn

1. Log in [id.apache.org](https://id.apache.org/) and submit your key fingerprint.

1. Add your GPG public key into [SkyWalking GPG KEYS](https://dist.apache.org/repos/dist/release/skywalking/KEYS) file, **you can do this only if you are a PMC member**.  You can ask a PMC member for help. **DO NOT override the existed `KEYS` file content, only append your key at the end of the file.**

## Build and sign the source code package

```shell
export VERSION=<the version to release>
git clone git@github.com:apache/skywalking-banyandb && cd skywalking-banyandb
git tag -a "v$VERSION" -m "Release Apache SkyWalking BanyanDB $VERSION"
git push --tags
make clean && make release-assembly
```

The `skywalking-banyandb-${VERSION}-bin.tgz`, `skywalking-banyandb-${VERSION}-src.tgz`, and their corresponding `asc`, `sha512`. **In total, six files should be automatically generated in the directory.**

## Upload to Apache svn

```shell
svn co https://dist.apache.org/repos/dist/dev/skywalking/
mkdir -p skywalking/banyandb/"$VERSION"
cp skywalking-banyandb/build/skywalking-banyandb*.tgz skywalking/banyandb/"$VERSION"
cp skywalking-banyandb/build/skywalking-banyandb*.tgz.asc skywalking/banyandb/"$VERSION"
cp skywalking-banyandb/build/skywalking-banyandb*.tgz.sha512 skywalking/banyandb/"$VERSION"

cd skywalking/banyandb && svn add "$VERSION" && svn commit -m "Draft Apache SkyWalking BanyanDB release $VERSION"
```

## Call for vote in dev@ mailing list

Call for vote in `dev@skywalking.apache.org`

```text
Subject: [VOTE] Release Apache SkyWalking BanyanDB version $VERSION

Content:

Hi the SkyWalking Community:
This is a call for vote to release Apache SkyWalking BanyanDB version $VERSION.

Release notes:

 * https://github.com/apache/skywalking-banyandb/blob/v$VERSION/CHANGES.md

Release Candidate:

 * https://dist.apache.org/repos/dist/dev/skywalking/banyandb/$VERSION
 * sha512 checksums
   - sha512xxxxyyyzzz apache-skywalking-banyandb-src-x.x.x.tgz
   - sha512xxxxyyyzzz apache-skywalking-banyandb-bin-x.x.x.tgz

Release Tag :

 * (Git Tag) v$VERSION

Release Commit Hash :

 * https://github.com/apache/skywalking-banyandb/tree/<Git Commit Hash>

Keys to verify the Release Candidate :

 * https://dist.apache.org/repos/dist/release/skywalking/KEYS

Guide to build the release from source :

 * https://github.com/apache/skywalking-banyandb/blob/v$VERSION/docs/installation.md

Voting will start now and will remain open for at least 72 hours, all PMC members are required to give their votes.

[ ] +1 Release this package.
[ ] +0 No opinion.
[ ] -1 Do not release this package because....

Thanks.

[1] https://github.com/apache/skywalking/blob/master/docs/en/guides/How-to-release.md#vote-check
```

## Vote Check

All PMC members and committers should check these before voting +1:

1. Features test.
1. All artifacts in staging repository are published with `.asc`, `.md5`, and `sha` files.
1. Source codes and distribution packages (`apache-skywalking-banyandb-{src,bin}-$VERSION.tgz`)
are in `https://dist.apache.org/repos/dist/dev/skywalking/banyandb/$VERSION` with `.asc`, `.sha512`.
1. `LICENSE` and `NOTICE` are in source codes and distribution package.
1. Check `shasum -c apache-skywalking-banyandb-{src,bin}-$VERSION.tgz.sha512`.
1. Check GPG signature. Download KEYS and import them by `curl https://www.apache.org/dist/skywalking/KEYS -o KEYS && gpg --import KEYS`. Check `gpg --batch --verify apache-skywalking-banyandb-{src,bin}-$VERSION.tgz.asc apache-skywalking-banyandb-{src,bin}-$VERSION.tgz`
1. Build distribution from source code package by following this [the build guide](#build-and-sign-the-source-code-package).
1. Licenses header check.

Vote result should follow these:

1. PMC vote is +1 binding, all others is +1 no binding.

1. Within 72 hours, you get at least 3 (+1 binding), and have more +1 than -1. Vote pass. 

1. **Send the closing vote mail to announce the result**.  When count the binding and no binding votes, please list the names of voters. An example like this:

   ```
   [RESULT][VOTE] Release Apache SkyWalking BanyanDB version $VERSION
   
   3 days passed, we’ve got ($NUMBER) +1 bindings:
   xxx
   xxx
   xxx
   ...
   (list names)
    
   I’ll continue the release process.
   ```

## Publish release

1. Move source codes tar balls and distributions to `https://dist.apache.org/repos/dist/release/skywalking/`, **you can do this only if you are a PMC member**.

    ```shell
    export SVN_EDITOR=vim
    svn mv https://dist.apache.org/repos/dist/dev/skywalking/banyandb/$VERSION https://dist.apache.org/repos/dist/release/skywalking/banyandb
    # ....
    # enter your apache password
    # ....
    ```
 
1. Remove last released tar balls from `https://dist.apache.org/repos/dist/release/skywalking`

1. Refer to the previous [PR](https://github.com/apache/skywalking-website/pull/118), update news and links on the website. There are seven files need to modify.

1. Update [Github release page](https://github.com/apache/skywalking-banyandb/releases), follow the previous convention.

1. Send ANNOUNCE email to `dev@skywalking.apache.org` and `announce@apache.org`, the sender should use his/her Apache email account. You can get the permlink of vote thread at [here](https://lists.apache.org/list.html?dev@skywalking.apache.org).

    ```
    Subject: [ANNOUNCEMENT] Apache SkyWalking BanyanDB $VERSION Released

    Content:

    Hi the SkyWalking Community

    On behalf of the SkyWalking Team, I’m glad to announce that SkyWalking BanyanDB $VERSION is now released.

    SkyWalking BanyanDB: An observability database, aims to ingest, analyze and store Metrics, Tracing and Logging data.

    SkyWalking: APM (application performance monitor) tool for distributed systems, especially designed for microservices, cloud native and container-based (Docker, Kubernetes, Mesos) architectures.

    Vote Thread: $VOTE_THREAD_PERMALINK

    Download Links: https://skywalking.apache.org/downloads/

    Release Notes : https://github.com/apache/skywalking-banyandb/blob/v$VERSION/CHANGES.md

    Website: https://skywalking.apache.org/

    SkyWalking BanyanDB Resources:
    - Issue: https://github.com/apache/skywalking/issues
    - Mailing list: dev@skywalkiing.apache.org
    - Documents: https://github.com/apache/skywalking-banyandb/blob/v$VERSION/README.md

    The Apache SkyWalking Team
    ```
    
    