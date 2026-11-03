package io.claviron.orbit;

import java.nio.file.Files;
import java.nio.file.Path;

/** Run from the assembled main/native JAR classpath, without a library path. */
public final class PackagedLibraryChecks {
    private PackagedLibraryChecks() {}
    @SuppressWarnings("try") // Deliberately check Busy when closing a library with a live runtime.
    public static void main(String[] args) throws Exception {
        check(NativeLoader.classifier("Linux", "amd64").equals("linux-x86_64"));
        check(NativeLoader.classifier("Linux", "aarch64").equals("linux-aarch64"));
        check(NativeLoader.classifier("Mac OS X", "arm64").equals("macos-aarch64"));
        check(NativeLoader.classifier("FreeBSD", "amd64").equals("freebsd-x86_64"));
        check(NativeLoader.classifier("FreeBSD", "arm64").equals("freebsd-aarch64"));
        try {
            NativeLoader.classifier("Mac OS X", "x86_64");
            throw new AssertionError("Unsupported macOS architecture accepted");
        } catch (UnsupportedOperationException expected) { /* Explicit matrix. */ }
        try {
            if (args[0].equals("ok")) {
                try (NativeLibrary library = NativeLibrary.load()) {
                    check(library.ffi.symbols.find("oj_fixture_submit").isEmpty());
                    NativeRuntime runtime = library.openRuntime(2, 32, 1);
                    try {
                        library.close();
                        throw new AssertionError("Library unloaded with a live runtime");
                    } catch (NativeException busy) {
                        check(busy.status() == 5);
                    }
                    runtime.close();
                }
            } else {
                try (NativeLibrary unexpected = NativeLibrary.load()) {
                    throw new AssertionError("Invalid packaged library loaded: " + unexpected);
                }
            }
        } catch (IllegalStateException | IllegalCallerException expected) {
            check(!args[0].equals("ok"));
            check(expected.getMessage().contains(args[1]));
        }
        try (var files = Files.list(Path.of(System.getProperty("java.io.tmpdir")))) {
            check(files.noneMatch(path -> path.getFileName().toString().startsWith("orbit-java-")));
        }
        System.out.println("Packaged native check passed: " + args[0]);
    }
    private static void check(boolean condition) {
        if (!condition) throw new AssertionError("Packaged native invariant failed");
    }
}
