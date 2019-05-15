package com.javanut.pronghorn.util.template;

public interface CharTemplateData<T> {

	void fetch(Appendable target, T source);

}
