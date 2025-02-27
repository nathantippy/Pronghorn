package com.javanut.pronghorn.util.template;

import com.javanut.pronghorn.util.AppendableByteWriter;
import com.javanut.pronghorn.util.ByteWriter;

public class StringTemplateBuilder<T> extends StringTemplateRenderer<T> implements ByteWriter {
	private StringTemplateScript<T>[] script;
	private int count;

	public StringTemplateBuilder() {
		this.script = new StringTemplateScript[8];
	}

	public StringTemplateBuilder<T> add(String text) {
		return addBytes(text.getBytes());
	}

	public StringTemplateBuilder<T> add(final byte[] byteData) {
		return add(byteData, 0, byteData.length);
	}

	public StringTemplateBuilder<T> add(final byte[] byteData, int pos, int len) {
		if (byteData != null && len > 0) {
			final byte[] localData = new byte[len];
			System.arraycopy(byteData, pos, localData, 0, len);
			addBytes(localData);
		}
		return this;
	}

	public StringTemplateBuilder<T> add(StringTemplateScript<T> script) {
		return append(script);
	}

	public <N> StringTemplateBuilder<T> add(final StringTemplateIterScript<T> script) {
		return append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {					
						for(int i = 0; (script.render(writer, source, i)); i++) {
						}
					}
				});
	}

	public StringTemplateBuilder<T> add(final StringTemplateScript<T>[] branches, final StringTemplateBranching<T> select) {
		final StringTemplateScript<T>[] localData = new StringTemplateScript[branches.length];
		System.arraycopy(branches, 0, localData, 0, branches.length);

		return append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {
						int s = select.branch(source);
						if (s != -1) {
							assert (s < localData.length) : "String template builder selected invalid branch.";
							localData[s].render(writer, source);
						}
					}
				});
	}

	public StringTemplateRenderer<T> finish() {
		return this;
	}
	
	@Override
	public void render(final AppendableByteWriter<?> writer, final T source) {
		render(this,writer,source);
	}

	public static <T> void render(StringTemplateBuilder<T> builder, AppendableByteWriter<?> writer, T source) {
		//assert(immutable) : "String template builder can only be rendered after lock.";
				StringTemplateScript<T>[] localScript = builder.script;
				for(int i=0;i<builder.count;i++) {
					localScript[i].render(writer, source);
				}
	}

	private StringTemplateBuilder<T> addBytes(final byte[] byteData) {
		return append(
				new StringTemplateScript<T>() {					
					@Override
					public void render(AppendableByteWriter writer, T source) {
						writer.write(byteData);
						
					}
				});
	}

	private StringTemplateBuilder<T> append(StringTemplateScript<T> fetchData) {
		//assert(!immutable) : "String template builder cannot be modified after lock.";
		if (count==script.length) {
			StringTemplateScript<T>[] newScript = new StringTemplateScript[script.length*2];
			System.arraycopy(script, 0, newScript, 0, script.length);
			script = newScript;
		}
		script[count++] = fetchData;
		return this;
	}


	@Override
	public void write(byte[] encodedBlock) {
		add(encodedBlock);
	}


	@Override
	public void write(byte[] encodedBlock, int pos, int len) {
		add(encodedBlock, pos, len);
	}


	@Override
	public void writeByte(final int asciiChar) {
		append(
				new StringTemplateScript<T>() {
					@Override
					public void render(AppendableByteWriter writer, T source) {
						writer.writeByte(asciiChar);
						
					}
				});
	}




	
	
}
