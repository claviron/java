#!/usr/bin/env python3
"""Prepare portable native artifacts and Maven Central bundles. No default action."""
import argparse
import base64
import datetime
import hashlib
import json
import os
from pathlib import Path
import platform
import re
import shutil
import subprocess
import sys
import tempfile
import tomllib
import urllib.request
import uuid
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[1]
ABI = 3
JAVA_RELEASE = 25
TARGETS = {
    "linux-x86_64": ("x86_64-unknown-linux-gnu", "liborbit_java.so", "x86-64", 62, (0, 3)),
    "linux-aarch64": ("aarch64-unknown-linux-gnu", "liborbit_java.so", "generic", 183, (0, 3)),
    "macos-aarch64": ("aarch64-apple-darwin", "liborbit_java.dylib", "generic", None, ()),
    "freebsd-x86_64": ("x86_64-unknown-freebsd", "liborbit_java.so", "x86-64", 62, (9,)),
    "freebsd-aarch64": ("aarch64-unknown-freebsd", "liborbit_java.so", "generic", 183, (9,)),
}
TARGET_ALIASES = {
    "linux-x64-gnu": "linux-x86_64",
    "linux-arm64-gnu": "linux-aarch64",
    "darwin-arm64": "macos-aarch64",
    "freebsd-x64": "freebsd-x86_64",
    "freebsd-arm64": "freebsd-aarch64",
}


def sha(data):
    return hashlib.sha256(data).hexdigest()


def file_sha(path):
    with Path(path).open("rb") as stream:
        return hashlib.file_digest(stream, "sha256").hexdigest()


def atomic(path, content):
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + "." + uuid.uuid4().hex + ".tmp")
    try:
        temporary.write_bytes(content)
        temporary.replace(path)  # Never truncate a library that another process has mapped.
    finally:
        temporary.unlink(missing_ok=True)


def write_json(path, value):
    atomic(path, (json.dumps(value, indent=2, sort_keys=True) + "\n").encode())


def run(command, cwd=ROOT, env=None):
    subprocess.run(list(map(str, command)), cwd=cwd, env=env, check=True)


def selected_target(value=None):
    value = TARGET_ALIASES.get(value, value)
    if value is None:
        os_name = {"Linux": "linux", "Darwin": "macos", "FreeBSD": "freebsd"}.get(platform.system())
        cpu = {"x86_64": "x86_64", "amd64": "x86_64", "aarch64": "aarch64", "arm64": "aarch64"}.get(platform.machine().lower())
        value = f"{os_name}-{cpu}"
    if value in TARGETS:
        return value
    for classifier, spec in TARGETS.items():
        if value == spec[0]:
            return classifier
    raise ValueError(f"Unsupported release target: {value}")


def source_files(root=ROOT):
    files = [root / "Cargo.toml", root / "release-config.json"]
    for name in ("native", "harness", "java/src/main", "scripts"):
        for path in (root / name).rglob("*"):
            relative = path.relative_to(root)
            if any(part in ("target", "__pycache__", ".git", "build") for part in relative.parts):
                continue
            if path.is_symlink():
                raise ValueError(f"Release source cannot be a symlink: {relative}")
            if path.is_file() and not (".local." in path.name or path.name.startswith("AGENTS")
                                       or path.name == "Cargo.lock" or path.suffix == ".pyc"):
                files.append(path)
    if (root / ".git").exists():
        names = [str(path.relative_to(root)) for path in files]
        ignored = subprocess.run(["git", "check-ignore", "-z", "--stdin"], cwd=root,
            input=b"\0".join(name.encode() for name in names) + b"\0", capture_output=True)
        if ignored.returncode not in (0, 1):
            raise ValueError("Cannot determine ignored release inputs")
        excluded = set(ignored.stdout.decode().split("\0"))
        files = [path for path in files if str(path.relative_to(root)) not in excluded]
    return sorted(files)


def context(root=ROOT):
    config = json.loads((root / "release-config.json").read_text())
    for key in ("name", "description", "url"):
        if not isinstance(config.get(key), str) or not config[key].strip():
            raise ValueError(f"Missing Maven metadata: {key}")
    for key in ("licenses", "developers"):
        if not isinstance(config.get(key), list) or not config[key]:
            raise ValueError(f"Missing Maven metadata: {key}")
    if any(not config.get("scm", {}).get(key) for key in ("url", "connection", "developerConnection")):
        raise ValueError("Missing Maven SCM metadata")
    if any(not item.get("name") or not item.get("url") for item in config["licenses"]):
        raise ValueError("Missing Maven license metadata")
    if any(not item.get("name") for item in config["developers"]):
        raise ValueError("Missing Maven developer name")
    version = tomllib.loads((root / "native/Cargo.toml").read_text())["package"]["version"]
    for value in (config["groupId"], config["artifactId"], version):
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", value) or ".." in value:
            raise ValueError("Invalid Maven coordinates")
    if version.endswith("SNAPSHOT"):
        raise ValueError("This release pipeline does not publish snapshots")
    digest = hashlib.sha256()
    # The native library is built from these alone: Java, scripts and harnesses
    # change without invalidating a prepared library.
    for path in source_files(root):
        relative = path.relative_to(root).as_posix()
        if relative != "Cargo.toml" and not relative.startswith("native/"):
            continue
        data = path.read_bytes()
        digest.update(str(path.relative_to(root)).encode() + b"\0" + str(len(data)).encode() + b"\0" + data)
    return {"groupId": config["groupId"], "artifactId": config["artifactId"], "version": version,
            "abi": ABI, "java": JAVA_RELEASE, "source": digest.hexdigest()}


def copy_sources(root, destination):
    for path in source_files(root):
        target = destination / path.relative_to(root)
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(path, target)


def ensure_lock(root=ROOT):
    """Resolve once without the development checkout's ignored Cargo patches."""
    lock = root / "prebuilds/Cargo.lock"
    if not lock.is_file():
        with tempfile.TemporaryDirectory(prefix="orbit-java-resolve-") as temporary:
            source = Path(temporary).resolve()
            copy_sources(root, source)
            run(["cargo", "generate-lockfile"], cwd=source)
            atomic(lock, (source / "Cargo.lock").read_bytes())
    return lock


def validate_binary(classifier, data):
    _, _, _, machine, osabis = TARGETS[classifier]
    if classifier == "macos-aarch64":
        valid = (len(data) >= 32 and int.from_bytes(data[:4], "little") == 0xfeedfacf
                 and int.from_bytes(data[4:8], "little") == 0x100000c
                 and int.from_bytes(data[12:16], "little") == 6)
    else:
        valid = (len(data) >= 64 and data[:6] == b"\x7fELF\x02\x01" and data[7] in osabis
                 and int.from_bytes(data[16:18], "little") == 3
                 and int.from_bytes(data[18:20], "little") == machine)
    if not valid:
        raise ValueError(f"Native artifact is not a shared library for {classifier}")


def stage_native(root, classifier, data, receipt):
    validate_binary(classifier, data)
    folder = root / "prebuilds" / classifier
    atomic(folder / TARGETS[classifier][1], data)
    write_json(folder / "release.json", receipt)


def read_native(root, classifier, expected=None, directory=None):
    expected = expected or context(root)
    directory = directory or root / "prebuilds" / classifier
    triple, library, cpu, _, _ = TARGETS[classifier]
    receipt = json.loads((directory / "release.json").read_text())
    data = (directory / library).read_bytes()
    validate_binary(classifier, data)
    lock = root / "prebuilds/Cargo.lock"
    required = {**expected, "format": 1, "classifier": classifier, "target": triple,
                "profile": "release", "cpu": cpu, "sha256": sha(data), "lock": file_sha(lock)}
    if any(receipt.get(key) != value for key, value in required.items()):
        raise ValueError(f"{classifier}: native receipt differs from current sources, dependency snapshot or binary")
    return data, receipt


def validate_local_dependencies(metadata, source):
    # Cargo canonicalizes manifests; macOS temporary paths can use /var symlinks.
    source = source.resolve()
    for package in metadata["packages"]:
        if package["source"] is None and not Path(package["manifest_path"]).resolve().is_relative_to(source):
            name = package.get("name", "unknown package")
            raise ValueError(f"Release dependency resolves to an external local checkout: {name}")


def prepare(root=ROOT, target=None):
    classifier = selected_target(target)
    triple, library, cpu, _, _ = TARGETS[classifier]
    expected = context(root)
    lock = ensure_lock(root)
    lock_hash = file_sha(lock)
    # Stage outside the checkout so parent .cargo/config.toml cannot inject local patches.
    with tempfile.TemporaryDirectory(prefix="orbit-java-native-") as temporary:
        source = Path(temporary).resolve()
        copy_sources(root, source)
        shutil.copyfile(lock, source / "Cargo.lock")
        env = os.environ.copy()
        output = Path(env.get("CARGO_TARGET_DIR", root / "target/prebuilds")).resolve()
        env["CARGO_TARGET_DIR"] = str(output)
        # A stable remap avoids invalidating every dependency for each random staging directory.
        cargo_home = Path(env.get("CARGO_HOME", Path.home() / ".cargo")).resolve()
        env["CARGO_ENCODED_RUSTFLAGS"] = (
            f"-C\x1ftarget-cpu={cpu}"
            f"\x1f--remap-path-prefix={Path.home()}=/build-home"
            f"\x1f--remap-path-prefix={cargo_home}=/cargo"
            f"\x1f--remap-path-prefix={Path(tempfile.gettempdir()).resolve()}=/build")
        metadata = subprocess.check_output(["cargo", "metadata", "--locked", "--all-features", "--format-version", "1"],
                                           cwd=source, env=env, text=True)
        validate_local_dependencies(json.loads(metadata), source)
        run(["cargo", "build", "--workspace", "--all-features", "--all-targets", "--locked", "--release", "--target", triple], cwd=source, env=env)
        data = (output / triple / "release" / library).read_bytes()
    if expected != context(root) or lock_hash != file_sha(lock):
        raise ValueError("Release inputs changed during native preparation")
    receipt = {**expected, "format": 1, "classifier": classifier, "target": triple,
               "profile": "release", "cpu": cpu, "sha256": sha(data), "lock": lock_hash}
    stage_native(root, classifier, data, receipt)
    print(f"Prepared prebuilds/{classifier}/{library}; no publication performed.")
    return receipt


def jdk_tool(name):
    home = os.environ.get("JAVA_HOME")
    if not home:
        raise ValueError("Set JAVA_HOME to JDK 25 or later")
    tool = Path(home) / "bin" / name
    if not tool.is_file():
        raise ValueError(f"Missing JDK tool: {name}")
    return tool


def properties(values):
    return "".join(f"{key}={value}\n" for key, value in values.items()).encode()


def jar(path, entries, version, module="dev.claviron.orbit"):
    manifest = ("Manifest-Version: 1.0\r\n" + (f"Automatic-Module-Name: {module}\r\n" if module else "")
                + f"Implementation-Version: {version}\r\n\r\n").encode()
    entries = {"META-INF/MANIFEST.MF": manifest, **entries}
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        for name, value in sorted(entries.items()):
            info = zipfile.ZipInfo(name, date_time=(1980, 1, 1, 0, 0, 0))  # ZIP's minimum timestamp.
            info.compress_type = zipfile.ZIP_DEFLATED
            info.external_attr = 0o100644 << 16
            archive.writestr(info, value)


def pom(config, expected):
    namespace = "http://maven.apache.org/POM/4.0.0"
    ET.register_namespace("", namespace)
    project = ET.Element(f"{{{namespace}}}project")
    def child(parent, name, value=None):
        node = ET.SubElement(parent, f"{{{namespace}}}{name}")
        node.text = value
        return node
    child(project, "modelVersion", "4.0.0")
    for key in ("groupId", "artifactId", "version"):
        child(project, key, expected[key])
    child(project, "packaging", "jar")
    for key in ("name", "description", "url"):
        child(project, key, config[key])
    licenses = child(project, "licenses")
    for value in config["licenses"]:
        node = child(licenses, "license")
        child(node, "name", value["name"])
        child(node, "url", value["url"])
        child(node, "distribution", "repo")
    developers = child(project, "developers")
    for value in config["developers"]:
        node = child(developers, "developer")
        for key, text in value.items():
            child(node, key, text)
    scm = child(project, "scm")
    for key, text in config["scm"].items():
        child(scm, key, text)
    props = child(project, "properties")
    child(props, "maven.compiler.release", str(JAVA_RELEASE))
    ET.indent(project)
    return ET.tostring(project, encoding="utf-8", xml_declaration=True)


def assemble(root=ROOT, targets=None):
    expected = context(root)
    config = json.loads((root / "release-config.json").read_text())
    targets = list(TARGETS) if targets is None else [selected_target(value) for value in targets]
    if len(set(targets)) != len(targets):
        raise ValueError("Duplicate native target")
    natives, errors = {}, []
    for classifier in targets:
        try:
            natives[classifier] = read_native(root, classifier, expected)
        except (OSError, ValueError, KeyError) as error:
            errors.append(f"{classifier}: {error}")
    if errors:
        raise ValueError("Native preparation incomplete:\n" + "\n".join(errors))
    folder = root / "target/central" / expected["version"]
    folder.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="orbit-java-package-", dir=folder.parent) as temporary:
        temporary = Path(temporary)
        output = temporary / "bundle"
        repository = output / "repository" / Path(*expected["groupId"].split(".")) / expected["artifactId"] / expected["version"]
        repository.mkdir(parents=True)
        classes, docs = temporary / "classes", temporary / "javadoc"
        classes.mkdir()
        java = sorted((root / "java/src/main/java").rglob("*.java"))
        run([jdk_tool("javac"), "--release", str(JAVA_RELEASE), "-Xlint:all", "-Werror", "-d", classes, *java])
        run([jdk_tool("javadoc"), "--release", str(JAVA_RELEASE), "-quiet", "-Xdoclint:all,-missing", "-notimestamp", "-d", docs, *java])
        stem = f"{expected['artifactId']}-{expected['version']}"
        api = {str(path.relative_to(classes)): path.read_bytes() for path in classes.rglob("*.class")}
        api["META-INF/orbit-java/api.properties"] = properties({"version": expected["version"], "abi": ABI})
        jar(repository / (stem + ".jar"), api, expected["version"])
        source_entries = {str(path.relative_to(root / "java/src/main/java")): path.read_bytes() for path in java}
        for path in source_files(root):
            if path.relative_to(root).parts[0] in ("native", "harness") or path == root / "Cargo.toml":
                source_entries["META-INF/orbit-java/rust/" + str(path.relative_to(root))] = path.read_bytes()
        jar(repository / (stem + "-sources.jar"), source_entries, expected["version"], module=None)
        jar(repository / (stem + "-javadoc.jar"), {str(path.relative_to(docs)): path.read_bytes() for path in docs.rglob("*") if path.is_file()}, expected["version"], module=None)
        for classifier, (data, receipt) in natives.items():
            base = f"META-INF/orbit-java/native/{classifier}/"
            entries = {base + TARGETS[classifier][1]: data,
                       base + "native.properties": properties({"version": expected["version"], "abi": ABI,
                                                              "classifier": classifier, "sha256": receipt["sha256"]})}
            jar(repository / f"{stem}-{classifier}.jar", entries, expected["version"], module="dev.claviron.orbit.native_" + classifier.replace("-", "_"))
        (repository / (stem + ".pom")).write_bytes(pom(config, expected))
        artifacts = {str(path.relative_to(output / "repository")): file_sha(path) for path in sorted(repository.iterdir())}
        if context(root) != expected:
            raise ValueError("Release sources changed during packaging")
        manifest = {**expected, "format": 1, "lock": file_sha(root / "prebuilds/Cargo.lock"), "targets": targets,
                    "complete": set(targets) == set(TARGETS), "artifacts": artifacts}
        write_json(output / "bundle.json", manifest)
        if folder.exists():
            shutil.rmtree(folder)
        output.replace(folder)
    print(f"Assembled {folder}; complete={manifest['complete']}; no publication performed.")
    return folder


def validate_bundle(folder, root=ROOT, complete=True, signed=False):
    manifest = json.loads((folder / "bundle.json").read_text())
    expected = context(root)
    if any(manifest.get(key) != value for key, value in expected.items()):
        raise ValueError("Maven bundle does not match current release sources")
    if complete and (not manifest.get("complete") or set(manifest["targets"]) != set(TARGETS)):
        raise ValueError("Publication requires all five native targets; a partial preview cannot be published")
    if file_sha(root / "prebuilds/Cargo.lock") != manifest["lock"]:
        raise ValueError("Maven bundle dependency snapshot has changed")
    coordinate = Path(*expected["groupId"].split(".")) / expected["artifactId"] / expected["version"]
    stem = f"{expected['artifactId']}-{expected['version']}"
    names = [stem + extension for extension in (".pom", ".jar", "-sources.jar", "-javadoc.jar")]
    targets = manifest.get("targets", [])
    if len(set(targets)) != len(targets) or set(targets) - set(TARGETS):
        raise ValueError("Invalid bundle native target list")
    names += [stem + "-" + target + ".jar" for target in targets]
    if set(manifest["artifacts"]) != {str(coordinate / name) for name in names}:
        raise ValueError("Bundle artifact list is incomplete or contains unexpected files")
    expected_paths = set(manifest["artifacts"])
    for name, checksum in manifest["artifacts"].items():
        relative = Path(name)
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("Unsafe bundle artifact path")
        path = folder / "repository" / relative
        if file_sha(path) != checksum:
            raise ValueError("Maven artifact changed after packaging")
        if signed:
            run(["gpg", "--batch", "--verify", str(path) + ".asc", path])
            for algorithm in ("md5", "sha1", "sha256"):
                digest = hashlib.new(algorithm, path.read_bytes()).hexdigest()
                if Path(str(path) + "." + algorithm).read_text().strip() != digest:
                    raise ValueError("Maven checksum does not match artifact")
                expected_paths.add(name + "." + algorithm)
            expected_paths.add(name + ".asc")
    actual = {str(path.relative_to(folder / "repository")) for path in (folder / "repository").rglob("*") if path.is_file()}
    if signed and actual != expected_paths:
        raise ValueError("Unexpected files in signed Maven repository")
    return manifest


def bundle_zip(folder, name, signed=False):
    destination = folder / name
    manifest = json.loads((folder / "bundle.json").read_text())
    try:
        with zipfile.ZipFile(destination, "w", compression=zipfile.ZIP_DEFLATED) as archive:
            # Whitelist the intended files; never include an unrelated operator file.
            for artifact, checksum in sorted(manifest["artifacts"].items()):
                suffixes = ("", ".asc", ".md5", ".sha1", ".sha256") if signed else ("",)
                for suffix in suffixes:
                    name = artifact + suffix
                    relative = Path(name)
                    if relative.is_absolute() or ".." in relative.parts:
                        raise ValueError("Unsafe bundle artifact path")
                    data = (folder / "repository" / relative).read_bytes()
                    if not suffix and sha(data) != checksum:
                        raise ValueError("Maven artifact changed while creating bundle")
                    archive.writestr(name, data)
    except BaseException:
        destination.unlink(missing_ok=True)
        raise
    return destination


def sign(folder, key, root=ROOT):
    if not key:
        raise ValueError("Supply --sign-key or ORBIT_JAVA_GPG_KEY explicitly")
    manifest = validate_bundle(folder, root)
    for name in manifest["artifacts"]:
        path = folder / "repository" / name
        run(["gpg", "--yes", "--armor", "--detach-sign", "--local-user", key, "--output", str(path) + ".asc", path])
        for algorithm in ("md5", "sha1", "sha256"):
            atomic(Path(str(path) + "." + algorithm), (hashlib.new(algorithm, path.read_bytes()).hexdigest() + "\n").encode())
    validate_bundle(folder, root, signed=True)
    archive = bundle_zip(folder, "central-bundle.zip", signed=True)
    print(f"Signed bundle: {archive}; upload has not been performed.")
    return archive


def publish(folder, root=ROOT):
    """One explicit, USER_MANAGED upload; no polling or automatic release."""
    validate_bundle(folder, root, signed=True)
    archive = bundle_zip(folder, "central-bundle.zip", signed=True)
    username = os.environ.get("CENTRAL_TOKEN_USERNAME")
    password = os.environ.get("CENTRAL_TOKEN_PASSWORD")
    if not username or not password:
        raise ValueError("Set CENTRAL_TOKEN_USERNAME and CENTRAL_TOKEN_PASSWORD from a Portal user token")
    token = base64.b64encode(f"{username}:{password}".encode()).decode()
    boundary = "orbit-java-" + uuid.uuid4().hex
    prefix = (f"--{boundary}\r\nContent-Disposition: form-data; name=\"bundle\"; filename=\"central-bundle.zip\"\r\n"
              "Content-Type: application/octet-stream\r\n\r\n").encode()
    suffix = f"\r\n--{boundary}--\r\n".encode()
    def body():
        yield prefix
        with archive.open("rb") as stream:
            while chunk := stream.read(shutil.COPY_BUFSIZE):
                yield chunk
        yield suffix
    request = urllib.request.Request("https://central.sonatype.com/api/v1/publisher/upload?publishingType=USER_MANAGED",
        data=body(), method="POST", headers={"Authorization": "Bearer " + token,
        "Content-Type": "multipart/form-data; boundary=" + boundary,
        "Content-Length": str(len(prefix) + archive.stat().st_size + len(suffix))})
    # No credentials are put in argv, logs or generated artifacts.
    with urllib.request.urlopen(request) as response:
        deployment = response.read().decode().strip()
    uuid.UUID(deployment)
    print(f"Uploaded deployment {deployment}. Finish validation/release in https://central.sonatype.com/publishing")
    print("USER_MANAGED upload does not automatically publish to Maven Central.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_mutually_exclusive_group(required=True)
    help_text = {
        "prepare": "build one portable native library into prebuilds; no upload",
        "package": "assemble Maven artifacts; --target permits an unpublishable partial preview",
        "dry-run": "require all five targets and create an unsigned local preview; no upload",
        "sign": "sign and verify a complete local bundle with GPG; no upload",
        "publish": "explicitly upload a signed bundle as USER_MANAGED; final release is a Portal action",
    }
    for name, text in help_text.items():
        actions.add_argument("--" + name, action="store_true", help=text)
    parser.add_argument("--target", action="append", help="classifier or Rust triple; one for native prepare, optional subset for package preview")
    parser.add_argument("--bundle", type=Path, help="existing bundle directory for signing/upload")
    parser.add_argument("--sign-key", default=os.environ.get("ORBIT_JAVA_GPG_KEY"))
    args = parser.parse_args()
    if args.bundle and not (args.sign or args.publish):
        parser.error("--bundle is only supported for signing/upload")
    if args.prepare:
        if args.target and len(args.target) != 1:
            parser.error("--prepare accepts one target")
        prepare(target=args.target[0] if args.target else None)
    elif args.package or args.dry_run:
        if args.dry_run and args.target:
            parser.error("--dry-run requires all five targets; use --package for a partial preview")
        folder = assemble(targets=args.target)
        if args.dry_run:
            validate_bundle(folder)
            print(f"Unsigned preview: {bundle_zip(folder, 'central-bundle-unsigned.zip')}")
    else:
        if args.target:
            parser.error("--target is only supported for prepare/package")
        folder = args.bundle or ROOT / "target/central" / context()["version"]
        if args.sign:
            sign(folder, args.sign_key)
        else:
            publish(folder)


if __name__ == "__main__":
    try:
        main()
    except (ValueError, OSError, KeyError, subprocess.CalledProcessError) as error:
        print(f"Release preparation failed: {error}", file=sys.stderr)
        sys.exit(1)
