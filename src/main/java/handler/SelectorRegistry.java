package handler;

import java.nio.channels.Selector;

public class SelectorRegistry {
    private static Selector selector;

    public static void setSelector(Selector sel) {
        selector = sel;
    }

    public static Selector getSelector() {
        return selector;
    }
}