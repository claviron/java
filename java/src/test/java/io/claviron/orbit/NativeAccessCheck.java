package io.claviron.orbit;

import java.nio.file.Path;

/** Run without a native-access grant, with illegal access denied. */
public final class NativeAccessCheck {
    private NativeAccessCheck() {}

    public static void main(String[] args) {
        try (NativeLibrary unexpected = NativeLibrary.load(Path.of(args[0]))) {
            throw new AssertionError("Native library loaded without native access: " + unexpected.getClass().getName());
        } catch (IllegalCallerException expected) {
            System.out.println("Native access without an explicit grant: correctly rejected");
        }
    }
}
