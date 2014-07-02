package io.reflection.app.logging;

import java.util.logging.Level;

public final class GaeLevel extends Level {

	private static final long serialVersionUID = 1L;

	protected GaeLevel(String name, int value) {
		super(name, value);
	}

	public static final Level DEBUG = Level.FINER;

	public static final Level TRACE = Level.FINEST;

}
