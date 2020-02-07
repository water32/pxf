package org.greenplum.pxf.api.model;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class RequestContextTest {

    private RequestContext context;

    @BeforeEach
    public void before() {
        context = new RequestContext();
    }

    @Test
    public void testDefaultValues() {
        assertEquals("default", context.getServerName());
        assertEquals(0, context.getStatsMaxFragments());
        assertEquals(0, context.getStatsSampleRatio(), 0.1);
    }

    @Test
    public void testSettingBlankToDefaultServerName() {
        context.setServerName("      ");
        assertEquals("default", context.getServerName());
    }

    @Test
    public void testInvalidServerName() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setServerName("foo,bar"));
        assertEquals("Invalid server name 'foo,bar'", ex.getMessage());
    }

    @Test
    public void testStatsMaxFragmentsFailsOnZero() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setStatsMaxFragments(0));
        assertEquals("Wrong value '0'. STATS-MAX-FRAGMENTS must be a positive integer", ex.getMessage());
    }

    @Test
    public void testStatsMaxFragmentsFailsOnNegative() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setStatsMaxFragments(-1));
        assertEquals("Wrong value '-1'. STATS-MAX-FRAGMENTS must be a positive integer", ex.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnOver1() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setStatsSampleRatio(1.1f));
        assertEquals("Wrong value '1.1'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0", ex.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnZero() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setStatsSampleRatio(0));
        assertEquals("Wrong value '0.0'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0", ex.getMessage());
    }

    @Test
    public void testStatsSampleRatioFailsOnLessThanOneTenThousand() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.setStatsSampleRatio(0.00005f));
        assertEquals("Wrong value '5.0E-5'. STATS-SAMPLE-RATIO must be a value between 0.0001 and 1.0", ex.getMessage());
    }

    @Test
    public void testValidateFailsWhenStatsSampleRatioIsNotSet() {
        context.setStatsMaxFragments(5);
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.validate());
        assertEquals("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together", ex.getMessage());
    }

    @Test
    public void testValidateFailsWhenStatsMaxFragmentsIsNotSet() {
        context.setStatsSampleRatio(0.1f);

        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.validate());
        assertEquals("Missing parameter: STATS-SAMPLE-RATIO and STATS-MAX-FRAGMENTS must be set together", ex.getMessage());
    }

    @Test
    public void testValidateFailsWhenProtocolIsNotSet() {
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.validate());
        assertEquals("Property PROTOCOL has no value in the current request", ex.getMessage());
    }

    @Test
    public void testSuccessfulValidationWhenStatsAreUnset() {
        context.setProtocol("Dummy");
        context.validate();
    }

    @Test
    public void testSuccessfulValidationWhenStatsAreSet() {
        context.setProtocol("Dummy");
        context.setStatsMaxFragments(4);
        context.setStatsSampleRatio(0.5f);
        context.validate();
    }

    @Test
    public void testServerNameIsLowerCased() {
        context.setServerName("DUMMY");
        assertEquals("dummy", context.getServerName());
    }

    @Test
    public void testReturnDefaultOptionValue() {
        assertEquals("bar", context.getOption("foo", "bar"));
    }

    @Test
    public void testReturnDefaultIntOptionValue() {
        assertEquals(77, context.getOption("foo", 77));
    }

    @Test
    public void testReturnDefaultNaturalIntOptionValue() {
        assertEquals(77, context.getOption("foo", 77, true));
    }

    @Test
    public void testFailsOnInvalidIntegerOptionWhenRequestedInteger() {
        context.addOption("foo", "junk");
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.getOption("foo", 77));
        assertEquals("Property foo has incorrect value junk : must be an integer", ex.getMessage());
    }

    @Test
    public void testFailsOnInvalidIntegerOptionWhenRequestedNaturalInteger() {
        context.addOption("foo", "junk");
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.getOption("foo", 77, true));
        assertEquals("Property foo has incorrect value junk : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testFailsOnInvalidNaturalIntegerOptionWhenRequestedNaturalInteger() {
        context.addOption("foo", "-5");
        Exception ex = assertThrows(
            IllegalArgumentException.class,
            () -> context.getOption("foo", 77, true));
        assertEquals("Property foo has incorrect value -5 : must be a non-negative integer", ex.getMessage());
    }

    @Test
    public void testReturnsUnmodifiableOptionsMap() {
        Map<String, String> unmodifiableMap = context.getOptions();
        assertThrows(
            UnsupportedOperationException.class,
            () -> unmodifiableMap.put("foo", "bar"));
    }

    @Test
    public void testConfigOptionIsSetWhenProvided() {
        context.setConfig("foobar");
        assertEquals("foobar", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsARelativeDirectoryName() {
        context.setConfig("../../relative");
        assertEquals("../../relative", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsAnAbsoluteDirectoryName() {
        context.setConfig("/etc/hadoop/conf");
        assertEquals("/etc/hadoop/conf", context.getConfig());
    }

    @Test
    public void testSucceedsWhenConfigOptionIsTwoDirectories() {
        context.setConfig("foo/bar");
        assertEquals("foo/bar", context.getConfig());
    }
}
