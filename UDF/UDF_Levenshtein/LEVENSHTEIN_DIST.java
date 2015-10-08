import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class LEVENSHTEIN_DIST extends EvalFunc<Integer> {
	public Integer exec(Tuple input) throws  IOException {	
		String misspelled = (String) input.get(0);
		String dictWord = (String) input.get(1);
		int dist = Integer.MAX_VALUE;
		
		Levenshtein lev = new Levenshtein();
		dist = lev.getLevenshteinDistance(misspelled, dictWord);
		
		return dist;
	}
}

