package com.b5m.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PriceToRange extends EvalFunc<String>{

	public PriceToRange() {
		
	}
	
	@Override
	public String exec(Tuple tuple) throws IOException {
		//TODO
		Double price = Double.parseDouble((String) tuple.get(0));
		Integer lower = 20 *( (int)Math.ceil(price) / 20);
		String range = lower.toString();
		Integer upper = lower + 20;
		range += " - " + upper.toString();
		return range;
	}

}
