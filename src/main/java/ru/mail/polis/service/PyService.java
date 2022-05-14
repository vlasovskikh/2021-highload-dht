package ru.mail.polis.service;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import com.google.gson.Gson;

import one.nio.http.HttpClient;
import one.nio.http.HttpException;
import one.nio.net.ConnectionString;
import one.nio.pool.PoolException;

public class PyService implements Service {
    private Process process;
    private final int port;
    private Path dir;
    private Set<String> topology;

    public PyService(int port, Path dir, Set<String> topology) {
        this.port = port;
        this.dir = dir;
        this.topology = topology;
    }

    @Override
    public void start() {
        var gson = new Gson();
        try {
            var cmd = new String[] {
                "poetry",
                "run",
                "pydht",
                "serve",
                "--directory",
                dir.toString(),
                "--port",
                Integer.toString(port),
                "--cluster-urls",
                gson.toJson(this.topology),
                "--debug",
                "--access-log",
            };
            process = Runtime.getRuntime().exec(cmd);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        if (waitUntilReady(port, 5000)) {
            return;
        }
        boolean finished;
        try {
            finished = process.waitFor(0, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (finished) {
            var s = new BufferedReader(
                new InputStreamReader(process.getErrorStream())
            ).lines().collect(Collectors.joining("\n"));
            throw new RuntimeException(s);
        }
        else {
            process.destroyForcibly();
        }
    }

    @Override
    public void stop() {
        if (process == null) {
            return;
        }
        process.destroy();
        boolean finished;
        try {
            finished = process.waitFor(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (!finished) {
            process.destroyForcibly();
        }
    }

    private boolean waitUntilReady(int port, int timeout) {
        final int inc = 100;
        for (int spent = 0; spent < timeout; spent += inc) {
            try {
                if (isReady(port)) {
                    return true;
                }
                Thread.sleep(inc);
            } catch (InterruptedException e) {
                return false;
            }
        }
        return false;
    }

    private boolean isReady(int port) throws InterruptedException {
        var c = new HttpClient(new ConnectionString("http://localhost:" + port));
        try {
            var r = c.get("/v0/status");
            return r.getStatus() / 100 == 2;
        } catch (PoolException | IOException | HttpException e) {
            return false;
        }
    }
}
