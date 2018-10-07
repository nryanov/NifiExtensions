package com.github.gr1f0n6x.nifi.service.memcached;

import com.github.grf0n6x.nifi.service.memcached.MemcachedConnectionService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class MemcachedConnectionServiceTest {
    private TestRunner runner;

    @Before
    public void init() {
        TestProcessor processor = new TestProcessor();
        runner = TestRunners.newTestRunner(processor);
    }

    @Test
    public void validateSuccess() throws InitializationException {
        MemcachedConnection connection = new MemcachedConnectionService();
        runner.addControllerService("test", connection);
        runner.enableControllerService(connection);
        runner.assertValid();
    }

    @Test
    public void validateError() throws InitializationException {
        MemcachedConnection connection = new MemcachedConnectionService();
        runner.addControllerService("test", connection);
        runner.setProperty(MemcachedConnectionService.HOST, "");
        runner.enableControllerService(connection);
        runner.assertNotValid();
    }

    @Test
    public void validateErrorIncorrectPort() throws InitializationException {
        MemcachedConnection connection = new MemcachedConnectionService();
        runner.addControllerService("test", connection);
        runner.setProperty(MemcachedConnectionService.PORT, "-1");
        runner.enableControllerService(connection);
        runner.assertNotValid();
    }

    @Test
    public void getClient() throws InitializationException {
        MemcachedConnection connection = new MemcachedConnectionService();
        runner.addControllerService("test", connection);
        runner.enableControllerService(connection);
        assertNotNull(connection.getClient());
    }
}
