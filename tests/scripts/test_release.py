"""Release correctness: synthetic non-host binaries never leave temporary fixtures."""
import base64
import contextlib
import io
import importlib.util
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch
import uuid
import zipfile

ROOT = Path(__file__).resolve().parents[2]
SCRIPTS = ROOT / "scripts"
sys.path.insert(0, str(SCRIPTS))
import release

spec = importlib.util.spec_from_file_location("remote_build", SCRIPTS / "remote-build.py")
remote = importlib.util.module_from_spec(spec)
spec.loader.exec_module(remote)


def binary(target):
    data = bytearray(64)
    if target == "macos-aarch64":
        data[:4] = (0xfeedfacf).to_bytes(4, "little")
        data[4:8] = (0x100000c).to_bytes(4, "little")
        data[12:16] = (6).to_bytes(4, "little")
    else:
        data[:6] = b"\x7fELF\x02\x01"
        data[7] = 9 if target.startswith("freebsd-") else 0
        data[16:18] = (3).to_bytes(2, "little")
        data[18:20] = release.TARGETS[target][3].to_bytes(2, "little")
    return bytes(data)


def snapshot(root):
    release.copy_sources(ROOT, root)
    config = json.loads((root / "release-config.json").read_text())
    config["groupId"] = "org.example"
    config["url"] = "https://example.com"
    config["scm"] = {"url": "https://example.com/claviron-java", "connection": "scm:git:https://example.com/claviron-java.git",
                     "developerConnection": "scm:git:https://example.com/claviron-java.git"}
    config["developers"] = [{"id": "example", "name": "Example Maintainer"}]
    (root / "release-config.json").write_text(json.dumps(config))
    release.atomic(root / "prebuilds/Cargo.lock", b"fixture dependency snapshot\n")


def prepared(root, target, data=None):
    data = binary(target) if data is None else data
    expected = release.context(root)
    triple, _, cpu, _, _ = release.TARGETS[target]
    receipt = {**expected, "format": 1, "classifier": target, "target": triple, "profile": "release", "cpu": cpu,
               "lock": release.file_sha(root / "prebuilds/Cargo.lock"), "sha256": release.sha(data)}
    release.stage_native(root, target, data, receipt)


class ReleaseChecks(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory(prefix="claviron-java-release-test-")
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "source"
        self.root.mkdir()
        snapshot(self.root)

    def test_target_matrix_rejects_architecture_and_freebsd_linux_confusion(self):
        self.assertEqual(len(release.TARGETS), 5)
        for target in release.TARGETS:
            release.validate_binary(target, binary(target))
            self.assertEqual(release.selected_target(release.TARGETS[target][0]), target)
        for alias, classifier in release.TARGET_ALIASES.items():
            self.assertEqual(release.selected_target(alias), classifier)
        with self.assertRaises(ValueError):
            release.validate_binary("freebsd-x86_64", binary("linux-x86_64"))
        with self.assertRaises(ValueError):
            release.validate_binary("linux-aarch64", binary("linux-x86_64"))
        with self.assertRaises(ValueError):
            release.selected_target("macos-x86_64")

    def test_cargo_canonical_manifests_stay_inside_symlinked_staging_root(self):
        alias = Path(self.temporary.name) / "source-alias"
        alias.symlink_to(self.root, target_is_directory=True)
        package = {"name": "workspace", "source": None,
                   "manifest_path": str((self.root / "native/Cargo.toml").resolve())}
        release.validate_local_dependencies({"packages": [package]}, alias)

    def test_external_dependency_symlink_cannot_bypass_release_isolation(self):
        external = Path(self.temporary.name) / "external"
        external.mkdir()
        (external / "Cargo.toml").write_text("fixture")
        alias = self.root / "external-alias"
        alias.symlink_to(external, target_is_directory=True)
        package = {"name": "external", "source": None,
                   "manifest_path": str(alias / "Cargo.toml")}
        with self.assertRaisesRegex(ValueError, "external local checkout: external"):
            release.validate_local_dependencies({"packages": [package]}, self.root)

    def test_receipt_binds_binary_lock_source_and_portable_cpu(self):
        target = "linux-x86_64"
        prepared(self.root, target)
        release.read_native(self.root, target)
        receipt_path = self.root / "prebuilds" / target / "release.json"
        receipt = json.loads(receipt_path.read_text())
        receipt["cpu"] = "native"
        release.write_json(receipt_path, receipt)
        with self.assertRaises(ValueError):
            release.read_native(self.root, target)
        prepared(self.root, target)
        (self.root / "prebuilds/Cargo.lock").write_text("other resolution")
        with self.assertRaises(ValueError):
            release.read_native(self.root, target)
        prepared(self.root, target)
        (self.root / "native/src/lib.rs").write_text("// changed fixture source\n")
        with self.assertRaises(ValueError):
            release.read_native(self.root, target)

    def test_hosts_validate_ssh_paths_and_one_builder_per_target(self):
        host = {"ssh": "builder.example.test", "repository": "/srv/source/claviron-java", "target": "linux-x86_64"}
        self.assertEqual(remote.hosts({"hosts": {"linux": host}}, [], release)[0]["target"], "linux-x86_64")
        for bad in ({**host, "ssh": "-oProxyCommand=command"}, {**host, "repository": "relative"}, {**host, "jobs": 0}):
            with self.assertRaises(ValueError):
                remote.hosts({"hosts": {"linux": bad}}, [], release)
        with self.assertRaises(ValueError):
            remote.hosts({"hosts": {"a": host, "b": host}}, [], release)

    def test_incomplete_central_metadata_is_refused_before_build(self):
        path = self.root / "release-config.json"
        config = json.loads(path.read_text())
        del config["scm"]["developerConnection"]
        path.write_text(json.dumps(config))
        with self.assertRaisesRegex(ValueError, "SCM metadata"):
            release.context(self.root)

    def test_sync_entrypoint_refuses_build_options(self):
        result = subprocess.run(["bash", str(ROOT / "scripts/sync.sh"), "--hosts", ".hosts"],
                                capture_output=True, text=True)
        self.assertEqual(result.returncode, 2)
        self.assertIn("--config", result.stderr)

    def test_remote_build_restores_untracked_lock_in_clean_clone_and_collects_only_artifacts(self):
        # Run real worker/clone orchestration locally; stub SSH, rsync and Cargo only.
        commands = Path(self.temporary.name) / "commands"
        commands.mkdir()
        (self.root / ".gitignore").write_text("/prebuilds/\n/target/\n__pycache__/\nCargo.lock\n")
        def git(*args):
            return subprocess.check_output(["git", *args], cwd=self.root, text=True)
        git("init", "--quiet")
        git("add", ".")
        git("-c", "commit.gpgsign=false", "-c", "user.name=Example", "-c", "user.email=build@example.test",
            "commit", "--quiet", "-m", "Prepare release test fixture")
        script = "#!" + sys.executable + "\n"
        (commands / "ssh").write_text(script + '''import subprocess,sys
if sys.argv[-2]=='refused':sys.exit(17)
sys.exit(subprocess.call(['bash','-c',sys.argv[-1].replace('bash -l -c ','bash -c ',1)]))
''')
        (commands / "rsync").write_text(script + '''import pathlib,shutil,sys
source=pathlib.Path(sys.argv[-2].split(':',1)[1]); destination=pathlib.Path(sys.argv[-1])
for name in ('libclaviron_java.so','libclaviron_java.dylib','release.json'):
 if (source/name).is_file():shutil.copyfile(source/name,destination/name)
''')
        (commands / "cargo").write_text(script + '''import json,os,pathlib,sys
if sys.argv[1]=='metadata':print(json.dumps({'packages':[]}))
elif sys.argv[1]=='build':
 target=sys.argv[sys.argv.index('--target')+1]
 folder=pathlib.Path(os.environ['CARGO_TARGET_DIR'])/target/'release';folder.mkdir(parents=True,exist_ok=True)
 data=bytearray(64);data[:6]=b'\\x7fELF\\x02\\x01';data[16:18]=(3).to_bytes(2,'little');data[18:20]=(62).to_bytes(2,'little')
 (folder/'libclaviron_java.so').write_bytes(data)
else:sys.exit(1)
''')
        for path in commands.iterdir():
            path.chmod(0o755)
        config = {"hosts": {
            "refused": {"ssh": "refused", "repository": str(self.root), "target": "freebsd-x86_64"},
            "accepted": {"ssh": "accepted", "repository": str(self.root), "target": "linux-x86_64", "cache": str(Path(self.temporary.name) / "cache")}}}
        before = (self.root / "prebuilds/Cargo.lock").read_bytes()
        with patch.dict(os.environ, {"PATH": str(commands) + os.pathsep + os.environ["PATH"]}):
            results = remote.build_all(self.root, config)
        self.assertEqual([result["success"] for result in results], [False, True])
        self.assertEqual((self.root / "prebuilds/Cargo.lock").read_bytes(), before)
        self.assertEqual(git("status", "--porcelain"), "")
        release.read_native(self.root, "linux-x86_64")
        jobs = list((Path(self.temporary.name) / "cache/jobs").iterdir())
        self.assertEqual((jobs[0] / "source/prebuilds/Cargo.lock").read_bytes(), before)


@unittest.skipUnless(os.environ.get("ORBIT_JAVA_TEST_NATIVE"), "supply an existing native library for JAR/FFM acceptance")
class PackagingChecks(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temporary = tempfile.TemporaryDirectory(prefix="claviron-java-package-test-")
        cls.root = Path(cls.temporary.name) / "source"
        cls.root.mkdir()
        snapshot(cls.root)
        for target in release.TARGETS:
            # Only the current-host payload is loadable; foreign payloads are header fixtures.
            data = Path(os.environ["ORBIT_JAVA_TEST_NATIVE"]).read_bytes() if target == release.selected_target() else binary(target)
            prepared(cls.root, target, data)
        cls.folder = release.assemble(cls.root)
        cls.manifest = release.validate_bundle(cls.folder, cls.root)
        cls.repository = cls.folder / "repository/org/example/orbit/0.1.0"
        cls.main_jar = cls.repository / "claviron-java-0.1.0.jar"
        cls.native_jar = cls.repository / ("claviron-java-0.1.0-" + release.selected_target() + ".jar")
        cls.classes = Path(cls.temporary.name) / "checks"
        cls.classes.mkdir()
        release.run([release.jdk_tool("javac"), "--release", "25", "-Xlint:all", "-Werror", "-cp", cls.main_jar,
                     "-d", cls.classes, ROOT / "java/src/test/java/dev/claviron/orbit/PackagedLibraryChecks.java"])

    @classmethod
    def tearDownClass(cls):
        cls.temporary.cleanup()

    def check_java(self, mode, jars, message="", grant=True):
        with tempfile.TemporaryDirectory(prefix="claviron-java-consumer-") as temporary:
            args = [release.jdk_tool("java"), "--illegal-native-access=deny", "-Djava.io.tmpdir=" + temporary]
            if grant:
                args += ["--enable-native-access=ALL-UNNAMED"]
            args += ["-cp", os.pathsep.join(map(str, [self.main_jar, self.classes, *jars])),
                     "dev.claviron.orbit.PackagedLibraryChecks", mode, message]
            release.run(args)

    def altered(self, kind):
        path = Path(self.temporary.name) / (kind + ".jar")
        with zipfile.ZipFile(self.native_jar) as archive:
            entries = {name: archive.read(name) for name in archive.namelist()}
        meta = next(name for name in entries if name.endswith("native.properties"))
        if kind == "version":
            entries[meta] = entries[meta].replace(b"version=0.1.0", b"version=9.0.0")
        elif kind == "abi":
            entries[meta] = entries[meta].replace(b"abi=3", b"abi=4")
        elif kind == "checksum":
            lib = next(name for name in entries if name.endswith((".so", ".dylib")))
            entries[lib] += b"corrupted fixture"
        release.jar(path, entries, "0.1.0")
        return path

    def test_packaged_load_cleanup_and_native_access_grant(self):
        self.check_java("ok", [self.native_jar])
        self.check_java("denied", [self.native_jar], "native access", grant=False)

    def test_missing_duplicate_wrong_version_and_corruption_are_rejected(self):
        self.check_java("missing", [], "Missing Orbit Java resource")
        self.check_java("duplicate", [self.native_jar, self.altered("duplicate")], "Ambiguous")
        self.check_java("version", [self.altered("version")], "mismatch")
        self.check_java("abi", [self.altered("abi")], "mismatch")
        self.check_java("checksum", [self.altered("checksum")], "checksum mismatch")

    def test_main_is_platform_neutral_and_bundle_has_all_five_classifiers(self):
        with zipfile.ZipFile(self.main_jar) as archive:
            self.assertFalse(any(name.endswith((".so", ".dylib")) or "Fixture" in name for name in archive.namelist()))
        self.assertEqual(len(self.manifest["artifacts"]), 9)
        self.assertEqual(set(self.manifest["targets"]), set(release.TARGETS))

    def test_partial_or_incomplete_bundle_cannot_be_signed_or_uploaded(self):
        manifest_path = self.folder / "bundle.json"
        original = manifest_path.read_bytes()
        self.addCleanup(manifest_path.write_bytes, original)
        altered = json.loads(original)
        altered["complete"] = False
        release.write_json(manifest_path, altered)
        with self.assertRaisesRegex(ValueError, "all five"):
            release.validate_bundle(self.folder, self.root)
        altered["complete"] = True
        altered["artifacts"].pop(next(iter(altered["artifacts"])))
        release.write_json(manifest_path, altered)
        with self.assertRaisesRegex(ValueError, "incomplete"):
            release.validate_bundle(self.folder, self.root)

    def test_signatures_checksums_and_user_managed_upload_without_network(self):
        if not shutil.which("gpg") or not shutil.which("gpgconf"):
            self.skipTest("gpg/gpgconf unavailable")
        temporary = tempfile.TemporaryDirectory(prefix="claviron-java-sign-test-")
        self.addCleanup(temporary.cleanup)
        home = Path(temporary.name)
        home.chmod(0o700)
        self.addCleanup(subprocess.run, ["gpgconf", "--homedir", str(home), "--kill", "gpg-agent"],
                        check=False, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        with patch.dict(os.environ, {"GNUPGHOME": str(home)}):
            subprocess.run(["gpg", "--batch", "--pinentry-mode", "loopback", "--passphrase", "", "--quick-generate-key",
                "Release Fixture <release@example.test>", "ed25519", "sign", "0"], check=True, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            archive = release.sign(self.folder, "release@example.test", self.root)
            self.assertTrue(archive.is_file())
            deployment = str(uuid.uuid4())
            class Response:
                def __enter__(self): return self
                def __exit__(self, *unused): return False
                def read(self): return deployment.encode()
            def upload(request):
                self.assertIn("publishingType=USER_MANAGED", request.full_url)
                content = b"".join(request.data)
                self.assertEqual(len(content), int(request.get_header("Content-length")))
                self.assertNotIn(b"fixture-secret", content)
                return Response()
            with patch.dict(os.environ, {"CENTRAL_TOKEN_USERNAME": "fixture", "CENTRAL_TOKEN_PASSWORD": "fixture-secret"}), \
                 patch.object(release.urllib.request, "urlopen", side_effect=upload) as request:
                output = io.StringIO()
                with contextlib.redirect_stdout(output):
                    release.publish(self.folder, self.root)
                self.assertIn("USER_MANAGED", output.getvalue())
                self.assertEqual(request.call_count, 1)


if __name__ == "__main__":
    unittest.main()
