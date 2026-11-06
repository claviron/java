# Maven Central release workflow

The release coordinates configured in `release-config.json` are currently
`dev.claviron:orbit-java:0.1.0`. They are proposed coordinates, not an available
Central release. Review the project, license, developer and SCM metadata before
a first publication. Java packages are `dev.claviron.orbit`.

## Artifacts and targets

One Maven component contains:

- `orbit-java-<version>.jar`: platform-neutral Java 25 API and packaged loader.
- `orbit-java-<version>-sources.jar`: Java sources and the native Rust workspace sources.
- `orbit-java-<version>-javadoc.jar`: API documentation.
- `orbit-java-<version>.pom`: coordinates, license, developer and SCM metadata.
- One native resource JAR per classifier below.

| Classifier | Rust target | Library |
| --- | --- | --- |
| `linux-x86_64` | `x86_64-unknown-linux-gnu` | `liborbit_java.so` |
| `linux-aarch64` | `aarch64-unknown-linux-gnu` | `liborbit_java.so` |
| `macos-aarch64` | `aarch64-apple-darwin` | `liborbit_java.dylib` |
| `freebsd-x86_64` | `x86_64-unknown-freebsd` | `liborbit_java.so` |
| `freebsd-aarch64` | `aarch64-unknown-freebsd` | `liborbit_java.so` |

A native JAR is named `orbit-java-<version>-<classifier>.jar`. Linux targets use
glibc. Matching OS/architecture alone does not establish libc compatibility;
validate each prepared binary on the oldest supported environment. x86-64 uses
`target-cpu=x86-64`; ARM64 uses `target-cpu=generic`. Preparation replaces local
Rust flags for all dependencies and the library. Header checks distinguish Linux
from FreeBSD ELF and reject wrong architectures, executables and incompatible
Mach-O files. Header validation is not execution on the target operating system.

## Native preparation

Native builders need Python 3.11+, stable Rust 1.98+, Bash and a native toolchain.
The requested Rust target and a matching linker/sysroot must be installed for a
cross build. Package assembly additionally requires JDK 25+ via `JAVA_HOME`.
Signing requires GnuPG. SSH and rsync are needed for remote preparation/collection.
No Maven or Gradle installation is needed to create the artifacts.

```sh
bash scripts/publish.sh --help
bash scripts/publish.sh --prepare
# Explicit target, on a builder with its toolchain:
bash scripts/publish.sh --prepare --target aarch64-unknown-freebsd
```

Preparation stages only `prebuilds/<classifier>/<library>` and `release.json`.
It builds a release profile from a temporary source snapshot outside the working
checkout, so ignored development `.cargo/config.toml` patches do not enter the
release. Dependency boundaries compare canonical filesystem paths, including
symlinked temporary directories. External local-path dependencies are refused.
Dependency resolution uses published Cargo packages. The generated `prebuilds/Cargo.lock` is the release's
shared dependency snapshot; the development checkout's `Cargo.lock` is preserved.
Native builds use workspace/all-features/all-targets and `--locked`.

The receipt binds coordinates, ABI, source hash, dependency lock hash, Rust target,
profile, portable CPU baseline and binary SHA-256. Receipts and binaries are
replaced by rename, without truncating a library a process may have mapped.
Keep the same release dependency snapshot across targets. To intentionally refresh
it, remove `prebuilds/Cargo.lock` and prepare every native artifact again.
A changed source or dependency snapshot invalidates earlier receipts.

## Prepare on several hosts

Copy `.hosts.example` to ignored `.hosts`, then enter operator-owned SSH aliases,
absolute repository paths and targets. Each remote repository must already exist;
the script fetches a missing requested commit from that repository's `origin`.
Optional `python`, `cache`, `jobs` and `nice` fields select the Python executable,
remote cache, Cargo concurrency and process priority. The JSON shape matches
orbit-js. Its target names `linux-x64-gnu`, `linux-arm64-gnu`, `darwin-arm64`,
`freebsd-x64` and `freebsd-arm64` are accepted aliases for the Maven classifiers
above. Python is an executable
name/path, not a shell command with flags. Keep private settings outside Git.

```sh
bash scripts/build-all.sh --dry-run
# Commit/push the source and builder changes before an actual remote build.
bash scripts/build-all.sh
bash scripts/build-all.sh --host linux-amd64 --host freebsd-arm64
```

When publishing from a macOS arm64 machine, prepare its native artifact locally
with `bash scripts/publish.sh --prepare`. The four other targets can be collected
from the Linux/FreeBSD entries in `.hosts`; macOS need not be an SSH builder.

The dry run reads configuration and prints the plan; it starts no build or SSH
session. Actual builds use isolated clones of the current commit, not another
host's development checkout. The orchestrator sends the canonical release
lockfile into each job. Workers verify source/dependency identity before compiling.
Build jobs have separate sessions and logs, so loss of the SSH connection does not
terminate the native build. Per-target cache locks prevent competing builders;
a retained lock after a crash requires checking its owner before operator cleanup.
There is no guessed expiration, polling interval or forced build deadline.

Hosts run concurrently. Failure of one does not cancel the others. Only the
native library and receipt are collected with rsync. Collection validates the
binary, checksum and receipt before replacing a local prebuild. Combined results
and per-host logs are under `target/build-all/`. Remote job/cache paths are
reported in the logs. No compilation or publication occurs in another host's
working checkout.

For separately prepared artifacts, create ignored `.release-sync.local.json`:

```json
{
  "targets": {
    "linux-x86_64": {"source": "build-linux:/srv/artifacts/orbit-java/prebuilds/linux-x86_64"},
    "macos-aarch64": {"source": "/srv/artifacts/orbit-java/prebuilds/macos-aarch64"}
  }
}
```

First place the matching canonical `Cargo.lock` at `prebuilds/Cargo.lock`, then:

```sh
bash scripts/sync.sh
# Or select a different ignored configuration:
bash scripts/sync.sh --config .release-sync.local.json
```

Sync collects and verifies artifacts; it starts no native build or publication.
The same preparation/collection commands work from a macOS publishing checkout.

## Assemble and inspect locally

```sh
export JAVA_HOME=/path/to/jdk-25
# Local preview for the one target already prepared:
bash scripts/publish.sh --package --target linux-x86_64
# Complete five-target unsigned preview, after all prebuilds have been collected:
bash scripts/publish.sh --dry-run
```

Output is under `target/central/<version>/repository/` using the standard Maven
repository layout. `bundle.json` records exact artifact hashes, source/dependency
identity and included targets. A partial preview cannot be signed or uploaded.
`--dry-run` requires all five native artifacts, creates
`central-bundle-unsigned.zip`, and performs no network upload or GPG signing.
A missing, mismatched or stale target is reported explicitly. Preview packages
contain no test classes, fixture native library, local configuration or credentials.

For a local Gradle consumer, point a Maven repository at the generated repository
folder. Once published to Central, consumers can use `mavenCentral()` instead.
Add the API and the deployment platform's native runtime dependency:

```kotlin
dependencies {
    implementation("dev.claviron:orbit-java:0.1.0")
    runtimeOnly("dev.claviron:orbit-java:0.1.0:linux-x86_64")
}
```

Equivalent Maven dependencies, after configuring the local preview repository or
once these coordinates are actually published:

```xml
<dependency>
  <groupId>dev.claviron</groupId><artifactId>orbit-java</artifactId><version>0.1.0</version>
</dependency>
<dependency>
  <groupId>dev.claviron</groupId><artifactId>orbit-java</artifactId><version>0.1.0</version>
  <classifier>linux-x86_64</classifier><scope>runtime</scope>
</dependency>
```

Build tools resolve dependencies; the Java runtime loader never downloads them.
Classpath deployment is the validated loading mode. Retain an explicit
`--enable-native-access=ALL-UNNAMED` grant when launching the application.

## Sign and publish later

These commands are separate operator actions. They are not part of native
preparation, collection or a dry run.

```sh
bash scripts/publish.sh --sign --sign-key YOUR_GPG_KEY_ID
# Optional explicit bundle directory:
bash scripts/publish.sh --sign --bundle target/central/0.1.0 --sign-key YOUR_GPG_KEY_ID
```

GPG can use the publishing machine's normal agent/pinentry. The script never
exports a private key or accepts a key passphrase in argv. Each JAR and POM gets
an armored detached signature, MD5/SHA-1/SHA-256 checksums. Signatures and hashes
are verified locally before `central-bundle.zip` is created. Artifact mutation,
missing classifiers or unexpected repository files refuse upload. Signing tests
use an ephemeral fixture key; that key is never used for a real release.

The signed ZIP can be uploaded manually through the Central Publisher Portal,
including its browser login/2FA, on the publishing machine. An optional API path
uses a Portal user token supplied only through `CENTRAL_TOKEN_USERNAME` and
`CENTRAL_TOKEN_PASSWORD`, then an explicitly invoked command:

```sh
bash scripts/publish.sh --publish --bundle target/central/0.1.0
```

That command makes one `USER_MANAGED` upload. It does not poll, retry, or
request automatic publication. The operator finishes validation and releases
through the browser at <https://central.sonatype.com/publishing>. Namespace
verification and token/signing-key setup are outside the source build.
No real upload or publication is required to run the validation tests.

References: [Central bundle layout](https://central.sonatype.org/publish/publish-portal-upload/),
[required release files](https://central.sonatype.org/publish/requirements/),
[Portal API and USER_MANAGED deployments](https://central.sonatype.org/publish/publish-portal-api/).

## Validation

```sh
python3 scripts/test.py
python3 scripts/package-test.py --native target/debug/liborbit_java.so
# Use the prepared release binary for its packaged loading acceptance:
python3 scripts/package-test.py --native prebuilds/linux-x86_64/liborbit_java.so
```

The package tests exercise actual Java classpath loading, ABI/version/checksum
refusals, duplicate/missing classifier resources, the native-access grant,
resource cleanup, POM/JAR contents, partial-bundle refusal, ephemeral GPG signing,
and a mocked USER_MANAGED upload that sends no network request. Remote workflow
tests use local isolated Git clones and stub SSH/rsync/Cargo; they do not execute
on or validate foreign operating systems. Synthetic foreign binary headers exist
only in temporary test fixtures, never in real `prebuilds/`.
