package cn.gm.light.rtable.utils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * @author jiachun.fjc
 */
public final class StackTraceUtil {

    private static final String NULL_STRING = "null";

    public static String stackTrace(final Throwable t) {
        if (t == null) {
            return NULL_STRING;
        }

        try (final ByteArrayOutputStream out = new ByteArrayOutputStream(); final PrintStream ps = new PrintStream(out)) {
            t.printStackTrace(ps);
            ps.flush();
            return new String(out.toByteArray());
        } catch (final IOException ignored) {
            // ignored
        }
        return NULL_STRING;
    }

    private StackTraceUtil() {
    }
}
