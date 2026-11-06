package dev.claviron.orbit;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.Locale;
import java.util.Properties;

/** Class-path native resources only. There is no download or system-library fallback. */
final class NativeLoader {
    private NativeLoader() {}

    static String classifier(String os, String arch) {
        String cpu = switch (arch.toLowerCase(Locale.ROOT)) {
            case "amd64", "x86_64" -> "x86_64";
            case "aarch64", "arm64" -> "aarch64";
            default -> throw new UnsupportedOperationException("Unsupported Orbit Java architecture: " + arch);
        };
        String system = switch (os.toLowerCase(Locale.ROOT)) {
            case "linux" -> "linux";
            case "mac os x", "macos", "darwin" -> "macos";
            case "freebsd" -> "freebsd";
            default -> throw new UnsupportedOperationException("Unsupported Orbit Java operating system: " + os);
        };
        if (system.equals("macos") && !cpu.equals("aarch64")) {
            throw new UnsupportedOperationException("Orbit Java provides macOS natives for arm64 only");
        }
        return system + "-" + cpu;
    }

    static Extracted extract() {
        String platform = classifier(System.getProperty("os.name"), System.getProperty("os.arch"));
        ClassLoader loader = NativeLibrary.class.getClassLoader();
        String base = "META-INF/orbit-java/";
        String resource = base + "native/" + platform + "/";
        try {
            Properties api = properties(unique(loader, base + "api.properties"));
            Properties nativeInfo = properties(unique(loader, resource + "native.properties"));
            if (!api.getProperty("version", "").equals(nativeInfo.getProperty("version"))
                    || !Integer.toString(Ffm.ABI).equals(api.getProperty("abi"))
                    || !api.getProperty("abi").equals(nativeInfo.getProperty("abi"))
                    || !platform.equals(nativeInfo.getProperty("classifier"))) {
                throw new IllegalStateException("Orbit Java API/native version, ABI or platform mismatch");
            }
            String expected = nativeInfo.getProperty("sha256", "");
            if (!expected.matches("[0-9a-f]{64}")) throw new IllegalStateException("Invalid Orbit Java native checksum metadata");
            String name = platform.startsWith("macos-") ? "liborbit_java.dylib" : "liborbit_java.so";
            URL binary = unique(loader, resource + name);
            Path directory = Files.createTempDirectory("orbit-java-");
            Extracted extracted = new Extracted(directory.resolve(name));
            try {
                MessageDigest digest = MessageDigest.getInstance("SHA-256");
                try (InputStream input = new java.security.DigestInputStream(binary.openStream(), digest)) {
                    Files.copy(input, extracted.path());
                }
                if (!expected.equals(HexFormat.of().formatHex(digest.digest()))) {
                    throw new IllegalStateException("Orbit Java native checksum mismatch");
                }
                return extracted;
            } catch (IOException | NoSuchAlgorithmException | RuntimeException | Error failure) {
                try { extracted.close(); } catch (RuntimeException cleanup) { failure.addSuppressed(cleanup); }
                throw failure;
            }
        } catch (IOException error) {
            throw new UncheckedIOException("Cannot extract the packaged Orbit Java native library", error);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("SHA-256 is unavailable", error);
        }
    }

    private static URL unique(ClassLoader loader, String resource) throws IOException {
        var resources = loader.getResources(resource);
        if (!resources.hasMoreElements()) {
            throw new IllegalStateException("Missing Orbit Java resource " + resource
                    + "; add the matching native classifier dependency or use NativeLibrary.load(Path)");
        }
        URL result = resources.nextElement();
        if (resources.hasMoreElements()) throw new IllegalStateException("Ambiguous Orbit Java resource: " + resource);
        return result;
    }

    private static Properties properties(URL resource) throws IOException {
        Properties result = new Properties();
        try (InputStream input = resource.openStream()) { result.load(input); }
        return result;
    }

    record Extracted(Path path) implements AutoCloseable {
        @Override public void close() {
            try {
                Files.deleteIfExists(path);
                Files.deleteIfExists(path.getParent());
            } catch (IOException error) {
                throw new UncheckedIOException("Cannot remove extracted Orbit Java library", error);
            }
        }
    }
}
