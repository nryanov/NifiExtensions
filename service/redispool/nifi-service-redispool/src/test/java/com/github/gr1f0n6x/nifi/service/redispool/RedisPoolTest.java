package com.github.gr1f0n6x.nifi.service.redispool;

import com.github.gr1f0n6x.nifi.service.redispool.service.RedisPoolService;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.util.TestRunner;
import org.apache.nifi.util.TestRunners;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

public class RedisPoolTest {
    private TestRunner runner;

    @Before
    public void init() {
        runner = TestRunners.newTestRunner(TestProcessor.class);
    }

    @Test
    public void validateDefaults() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);
        runner.assertValid(service);
    }

    @Test
    public void validateCustom() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);

        runner.setProperty(service, RedisPoolService.HOST, "192.168.99.100");
        runner.setProperty(service, RedisPoolService.PORT, "6379");
        runner.setProperty(service, RedisPoolService.MAX_TOTAL_CONNECTIONS, "4");
        runner.assertValid(service);
    }

    @Test(expected = AssertionError.class)
    public void validateBlankHostError() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);

        runner.setProperty(service, RedisPoolService.HOST, "");
        runner.assertValid(service);
    }

    @Test(expected = AssertionError.class)
    public void validateIncorrectPortError() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);

        runner.setProperty(service, RedisPoolService.PORT, "-1");
        runner.assertValid(service);
    }

    @Test(expected = AssertionError.class)
    public void validateIncorrectTotalConnectionsError() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);

        runner.setProperty(service, RedisPoolService.MAX_TOTAL_CONNECTIONS, "-1");
        runner.assertValid(service);
    }

    @Test
    public void enable() throws InitializationException {
        final RedisPoolService service = new RedisPoolService();
        runner.addControllerService("test", service);
        runner.enableControllerService(service);

        assertTrue(service.isEnabled());
    }
}
