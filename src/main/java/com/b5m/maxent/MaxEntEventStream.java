package com.b5m.maxent;

import java.io.IOException;
import opennlp.maxent.ContextGenerator;
import opennlp.maxent.DataStream;
import opennlp.model.AbstractEventStream;
import opennlp.model.Event;

public class MaxEntEventStream extends AbstractEventStream {
	ContextGenerator cg;
	DataStream ds;
	Event next;
	private String separator = " ";

	public MaxEntEventStream(DataStream ds, String sep) {
		separator = sep;
		cg = new MaxEntContextGenerator();
		this.ds = ds;
		if (this.ds.hasNext())
			next = createEvent((String) this.ds.nextToken());
	}

	public MaxEntEventStream(DataStream ds) {
		this(ds, " ");
	}

	public boolean hasNext() throws IOException {
		while (next == null && ds.hasNext())
			next = createEvent((String) ds.nextToken());
		return next != null;
	}

	public Event next() throws IOException {
		while (next == null && this.ds.hasNext())
			next = createEvent((String) this.ds.nextToken());

		Event current = next;
		if (this.ds.hasNext()) {
			next = createEvent((String) this.ds.nextToken());
		} else {
			next = null;
		}
		return current;
	}

	private Event createEvent(String obs) {
		int lastSpace = obs.lastIndexOf(separator);
		if (lastSpace == -1)
			return null;
		else
			return new Event(obs.substring(lastSpace + 1), cg.getContext(obs
					.substring(0, lastSpace)));
	}

}
