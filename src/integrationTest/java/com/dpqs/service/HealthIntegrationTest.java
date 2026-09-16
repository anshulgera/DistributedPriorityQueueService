package com.dpqs.service;

import org.junit.jupiter.api.Test;

import static io.restassured.RestAssured.given;
import static org.hamcrest.Matchers.equalTo;

/**
 * Smoke test that hits a running, deployed instance of the service over HTTP.
 * This is intentionally not an in-process test: it exercises the real network
 * path (container, port mapping, JSON serialization) the same way a client would.
 */
class HealthIntegrationTest {

    private static String baseUrl() {
        String url = System.getenv("SERVICE_BASE_URL");
        return url != null ? url : "http://localhost:8080";
    }

    @Test
    void healthEndpointReturns200Ok() {
        given()
                .baseUri(baseUrl())
        .when()
                .get("/health")
        .then()
                .statusCode(200)
                .body("status", equalTo("ok"));
    }
}
