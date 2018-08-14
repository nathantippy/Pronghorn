package com.ociweb.json.encode;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import com.ociweb.pronghorn.util.StringBuilderWriter;

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
                .beginObject().endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{}", out.toString());
    }

    @Test
    public void testObjectNull_Yes() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject().integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, null);
        assertEquals("null", out.toString());
    }

    @Test
    public void testObjectNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject().integer("i", o->o.i).endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        assertEquals("{\"i\":9}", out.toString());
    }

    @Test
    public void testObjectCompund() {
        JSONRenderer<Integer> json1 = new JSONRenderer<Integer>()
                .integer(o->o);
        JSONRenderer<BasicObj> json2 = new JSONRenderer<BasicObj>()
                .beginObject()
                    .beginSelect("no comma")
                        .tryCase(o->false).constantNull()
                    .endSelect()
                    .integer("y", o->o.i+6)
                    .basicArray("bob", o-> new Integer[] {332}).string((o, i) -> o[i].toString())
                    .listArray("bob", o-> Arrays.asList(224, 213)).string((o, i) -> o.get(i).toString())
                    .iterArray("bob", o-> Arrays.asList(224, 213)).string((o, i) -> o.next().toString())
                .endObject();
        JSONRenderer<BasicObj> json3 = new JSONRenderer<BasicObj>()
                .beginObject()
                    .renderer("v", json1, o->o.i+5)
                    .renderer("x", json2, o->o)
                    .renderer("z", json2, o->null)
                    .beginSelect("cond")
                        .tryCase(o->false).integer(o->42)
                        .tryCase(o->true).integer(o->43)
                    .endSelect()
                    .beginObject("always", o->null)
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
                .beginObject()
                    .bool("b", o->o.b)
                    .integer("i", o->o.i)
                    .decimal("d", 2, o->o.d)
                    .string("s", (o,t)-> t.append(o.s))
                    .array("empty", null)
                        .empty()
                    .beginObject("m")
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
                .beginObject()
                    .nullableBool("b", o->true, o->o.b)
                    .nullableInteger("i", o->true, o->o.i)
                    .nullableDecimal("d", 2, o->true, o->o.d)
                    .nullableString("s", (o,t)-> t.append(null))
                    .beginObject("m", o->o.m)
                    .endObject()
                    .constantNull("always")
                .enumName("en", o->null)
                .enumOrdinal("eo", o->null)
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj());
        System.out.println(out);
        assertEquals("{\"b\":null,\"i\":null,\"d\":null,\"s\":null,\"m\":null,\"always\":null,\"en\":null,\"eo\":null}", out.toString());
    }

    @Test
    public void testObjectPrimitivesNull_No() {
        JSONRenderer<BasicObj> json = new JSONRenderer<BasicObj>()
                .beginObject()
                    .nullableBool("b", o->false, o->o.b)
                    .nullableInteger("i", o->false, o->o.i)
                    .nullableDecimal("d", 2, o->false, o->o.d)
                    .nullableString("s", (o,t)->t.append(o.s))
                    .beginObject("m", o->o.m)
                        .beginObject("c", o->o.m).endObject()
                    .endObject()
                .endObject();
        assertTrue(json.isLocked());
        json.render(out, new BasicObj(new BasicObj()));
        assertEquals("{\"b\":true,\"i\":9,\"d\":123.40,\"s\":\"fum\",\"m\":{\"c\":null}}", out.toString());
    }
}
