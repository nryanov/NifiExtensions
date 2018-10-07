package com.github.gr1f0n6x.nifi.service.tarantool;

import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

public class TarantoolConnectionServiceTest {
    private TestRunner runner;

    @Before
    public void init() {
        TestProcessor processor = new TestProcessor();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void validateSuccess() throws InitializationException {
        TarantoolConnection connection = new TarantoolConnectionService();
        runner.addControllerService("connection", connection);
        runner.enableControllerService(connection);
        runner.assertValid();
    }

    @Test
    public void validateError() throws InitializationException {
        TarantoolConnection connection = new TarantoolConnectionService();
        runner.addControllerService("connection", connection);
        runner.setProperty(TarantoolConnectionService.PORT, "-1");
        runner.enableControllerService(connection);
        runner.assertNotValid();
    }
}
