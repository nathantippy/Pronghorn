package com.javanut.json.encode;

import org.junit.Before;
import org.junit.Test;

import com.javanut.json.encode.JSONRenderer;
import com.javanut.pronghorn.util.StringBuilderWriter;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONRootEnumTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testRootEnum_name() {
        JSONRenderer<StackEnum> json = new JSONRenderer<StackEnum>()
                .enumName(o -> o);
        assertTrue(json.isLocked());
        json.render(out, StackEnum.pronghornPipes);
        assertEquals("\"pronghornPipes\"", out.toString());
    }

    @Test
    public void testRootEnum_ordinal() {
        JSONRenderer<StackEnum> json = new JSONRenderer<StackEnum>()
                .enumOrdinal(o -> o);
        assertTrue(json.isLocked());
        json.render(out, StackEnum.pronghornPipes);
        assertEquals("1", out.toString());
    }

    @Test
    public void testRootEnumNull_name() {
        JSONRenderer<StackEnum> json = new JSONRenderer<StackEnum>()
                .enumName(o -> o);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testRootEnumNull_ordinal() {
        JSONRenderer<StackEnum> json = new JSONRenderer<StackEnum>()
                .enumOrdinal(o -> o);
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }
}
