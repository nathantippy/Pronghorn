package com.javanut.json.encode;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.javanut.json.encode.JSONRenderer;
import com.javanut.pronghorn.util.StringBuilderWriter;

import java.time.DayOfWeek;
import java.util.Arrays;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertTrue;

public class JSONObjectTests {
    private StringBuilderWriter out;

    @Before
    public void init() {
        out = new StringBuilderWriter();
    }

    @Test
    public void testObjectEmpty() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject().endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{}", out.toString());
    }

    @Test
    public void testObjectNull_Yes() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject().integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testObjectNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject().integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{\"i\":9}", out.toString());
    }

    @Test
    public void testObjectCompund() {
        JSONRenderer<Integer> json1 = new JSONRenderer<Integer>()
                .integer(o->o);
        JSONRenderer<BasicObj> json2 = new JSONRenderer<BasicObj>()
                .startObject()
                    .beginSelect("no comma")
                        .tryCase(o->false).constantNull()
                    .endSelect()
                    .integer("y", o->o.i+6)
                    .basicArray("bob", o-> new Integer[] {332}).string((o, i, t) -> t.append(o[i].toString()))
                    .listArray("bob", o-> Arrays.asList(224, 213)).string((o, i, t) -> t.append(o.get(i).toString()))
                    .iterArray("bob", o-> Arrays.asList(224, 213)).string((o, i, t) -> t.append(o.next().toString()))
                .endObject();
        JSONRenderer<BasicObj> json3 = new JSONRenderer<BasicObj>()
                .startObject()
                    .renderer("v", json1, o->o.i+5)
                    .renderer("x", json2, o->o)
                    .renderer("z", json2, o->null)
                    .beginSelect("cond")
                        .tryCase(o->false).integer(o->42)
                        .tryCase(o->true).integer(o->43)
                    .endSelect()
                    .startObject("always", o->null)
                        .empty()
                    .endObject()
                .endObject();
        assertTrue(json3.isLocked());
        json3.render(out, new BasicObj());
        assertEquals("{\"v\":14,\"x\":{\"y\":15,\"bob\":[\"332\"],\"bob\":[\"224\",\"213\"],\"bob\":[\"224\",\"213\"]},\"z\":null,\"cond\":43,\"always\":null}", out.toString());
    }

    @Test
    public void testObjectPrimitives() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject()
                    .bool("b", o->o.b)
                    .integer("i", o->o.i)
                    .decimal("d", 2, o->o.d)
                    .string("s", (o,t)-> t.append(o.s))
                    .array("empty", null)
                        .empty()
                    .startObject("m")
                        .empty()
                    .endObject()
                    .enumName("en", o->DayOfWeek.TUESDAY)
                    .enumOrdinal("eo", o->DayOfWeek.WEDNESDAY)
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj()));
        assertEquals("{\"b\":true,\"i\":9,\"d\":123.40,\"s\":\"fum\",\"empty\":[],\"m\":{},\"en\":\"TUESDAY\",\"eo\":2}", out.toString());
    }

    @Test
    public void testObjectPrimitivesNull_Yes() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject()
                    .nullableBool("b", o->true, o->o.b)
                    .nullableInteger("i", o->true, o->o.i)
                    .nullableDecimal("d", 2, o->true, o->o.d)
                    .nullableString("s", (o,t)-> t.append(null))
                    .startObject("m", o->o.m)
                    .endObject()
                    .constantNull("always")
                .enumName("en", o->null)
                .enumOrdinal("eo", o->null)
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        
        assertEquals("{\"b\":null,\"i\":null,\"d\":null,\"s\":null,\"m\":null,\"always\":null,\"en\":null,\"eo\":null}", out.toString());
    }

    @Test
    public void testObjectPrimitivesNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .startObject()
                    .nullableBool("b", o->false, o->o.b)
                    .nullableInteger("i", o->false, o->o.i)
                    .nullableDecimal("d", 2, o->false, o->o.d)
                    .nullableString("s", (o,t)->t.append(o.s))
                    .startObject("m", o->o.m)
                        .startObject("c", o->o.m).endObject()
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj()));
        assertEquals("{\"b\":true,\"i\":9,\"d\":123.40,\"s\":\"fum\",\"m\":{\"c\":null}}", out.toString());
    }
}
