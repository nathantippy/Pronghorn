package com.ociweb.json.encode;

import com.ociweb.json.encode.function.*;
import com.ociweb.json.JSONType;
import com.ociweb.json.template.StringTemplateBuilder;

public class JSONArray<T, P extends JSONCompositeOwner, N> implements JSONCompositeOwner {
    private final JSONBuilder<T> builder;
    private final ArrayIteratorFunction<T, N> iterator;
    private final P owner;
    private final int depth;

    JSONArray(StringTemplateBuilder<T> scripts, JSONKeywords keywords, ArrayIteratorFunction<T, N> iterator, P owner, int depth) {
        this.iterator = iterator;
        this.depth = depth;
        this.builder = new JSONBuilder<>(scripts, keywords, depth);
        this.owner = owner;
    }

    @Override
    public void childCompleted() {
        // Single child...
        builder.endArray();
        owner.childCompleted();
    }

    // Object

    public <M> JSONObject<M, P> beginObject(IterMemberFunction<T, N, M> accessor) {
        return new JSONObject<M, P>(
                builder.beginObject(iterator, accessor),
                builder.getKeywords(), owner, depth + 1) {

            public P endObject() {
                builder.endArray();
                owner.childCompleted();
                return owner;
            }
        };
    }

    // Array

    // TODO

    // Renderer

    public <M> P renderer(JSONRenderer<M> renderer, IterMemberFunction<T, N, M> accessor) {
        builder.addRenderer(iterator, renderer, accessor);
        this.childCompleted();
        return owner;
    }

    @Deprecated
    public <M> P renderer(JSONRenderer<M> renderer, ToMemberFunction<T, M> accessor) {
        builder.addRenderer(renderer, accessor);
        this.childCompleted();
        return owner;
    }

    // Null

    public P constantNull() {
        builder.addNull(iterator);
        this.childCompleted();
        return owner;
    }

    // TODO: nullable array elements for primitives

    // Bool

    public P bool(IterBoolFunction<T, N> func) {
        builder.addBool(iterator, func);
        this.childCompleted();
        return owner;
    }

    public P bool(IterBoolFunction<T, N> func, JSONType encode) {
        builder.addBool(iterator, func, encode);
        this.childCompleted();
        return owner;
    }

    // Integer

    public P integer(IterLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        this.childCompleted();
        return owner;
    }

    public P integer(IterLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        this.childCompleted();
        return owner;
    }

    @Deprecated
    public P integerNull(IterNullableLongFunction<T, N> func) {
        builder.addInteger(iterator, func);
        this.childCompleted();
        return owner;
    }

    @Deprecated
    public P integerNull(IterNullableLongFunction<T, N> func, JSONType encode) {
        builder.addInteger(iterator, func, encode);
        this.childCompleted();
        return owner;
    }

    // Decimal

    public P decimal(int precision, IterDoubleFunction<T, N> func) {
        builder.addDecimal(iterator, precision, func);
        this.childCompleted();
        return owner;
    }

    public P decimal(int precision, IterDoubleFunction<T, N> func, JSONType encode) {
        builder.addDecimal(iterator, precision, func, encode);
        this.childCompleted();
        return owner;
    }

    // String

    public P string(IterStringFunction<T, N> func) {
        builder.addString(iterator, func);
        this.childCompleted();
        return owner;
    }

    public P string(IterStringFunction<T, N> func, JSONType encode) {
        builder.addString(iterator, func, encode);
        this.childCompleted();
        return owner;
    }
}
