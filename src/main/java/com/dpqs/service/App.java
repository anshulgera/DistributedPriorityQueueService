package com.dpqs.service;

import io.javalin.Javalin;

public class App {

    public static void main(String[] args) {
        Javalin.create()
                .get("/health", ctx -> ctx.json(new HealthStatus("ok")))
                .start(resolvePort());
    }

    static int resolvePort() {
        String port = System.getenv("PORT");
        return port != null ? Integer.parseInt(port) : 8080;
    }
}
