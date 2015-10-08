import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class STEM extends EvalFunc<String> {
	public String exec(Tuple input) throws  IOException {	
		String word = (String) input.get(0);
		String stemmed = "";
		if (!word.isEmpty()){
			Porter p = new Porter();
			stemmed = p.stripAffixes(word);
		}
		return stemmed;
	}
}
