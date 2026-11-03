package io.claviron.orbit;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/** Ordered header entry. Values retain their bytes, including repeated/non-UTF-8 fields. */
public final class Header {
    private final String name;
    private final byte[] value;

    private Header(String name, byte[] owned) {
        this.name = Objects.requireNonNull(name);
        this.value = owned;
    }

    public static Header copyOf(String name, byte[] value) { return new Header(name, value.clone()); }

    public static Header text(String name, String value) {
        if (value.chars().anyMatch(c -> c > 255)) throw new IllegalArgumentException("Header text must be Latin-1");
        return new Header(name, value.getBytes(StandardCharsets.ISO_8859_1));
    }

    static Header owned(String name, byte[] value) { return new Header(name, value); }
    byte[] bytes() { return value; }
    public String name() { return name; }
    public ByteBuffer value() { return ByteBuffer.wrap(value).asReadOnlyBuffer(); }
    public String latin1() { return new String(value, StandardCharsets.ISO_8859_1); }
}
