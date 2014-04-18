package com.b5m.pig.udf;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PriceToRange extends EvalFunc<String> {
	private static final Pattern PRICE_PATTERN = Pattern
			.compile("[\\d,]+\\.?[\\d]*");

	public PriceToRange() {

	}

	@Override
	public String exec(Tuple tuple) throws IOException {
		String args = (String) tuple.get(0);
		Matcher machter = PRICE_PATTERN.matcher(args);
		if (machter.find()) {
			try {
				Double price = Double.parseDouble(machter.group());
				Integer lower = 20 * ((int) Math.ceil(price) / 20);
				String range = lower.toString();
				Integer upper = lower + 20;
				range += " - " + upper.toString();
				return range;
			} catch (Exception e) {
				return "";
			}
		}
		return "";
	}
}
