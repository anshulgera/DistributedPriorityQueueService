package com.dpqs.service;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AppTest {

    @Test
    void defaultsToPort8080WhenPortEnvVarUnset() {
        // PORT is not set in this test process, so resolvePort() must fall back to 8080.
        assertEquals(8080, App.resolvePort());
    }

    @Test
    void healthStatusReportsOk() {
        assertEquals("ok", new HealthStatus("ok").status());
    }
}
