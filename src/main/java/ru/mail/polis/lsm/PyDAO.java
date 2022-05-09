package ru.mail.polis.lsm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Iterator;

public final class PyDAO implements DAO {
    public Path dir;

    public PyDAO(Path dir) throws IOException, InterruptedException {
        this.dir = dir;
    }

    @Override
    public void close() throws IOException {
        // Empty
    }

    @Override
    public Iterator<Record> range(ByteBuffer fromKey, ByteBuffer toKey) {
        return null;
    }

    @Override
    public void upsert(Record record) {
        // Empty
    }

    @Override
    public void closeAndCompact() {
        // Empty
    }
}
