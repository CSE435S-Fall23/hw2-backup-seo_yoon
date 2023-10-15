package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;

/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {
	
	private AggregateOperator ao;
	private boolean groupBy;
	private TupleDesc td;
	private ArrayList<Tuple> tuples = new ArrayList<>();
	
	private HashMap<Object, Integer> countMap = new HashMap<>();
    private HashMap<Object, Integer> sumMap = new HashMap<>();
	
	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.ao = o;
		this.groupBy = groupBy;
		this.td = td;
	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		if (groupBy) {
			mergeGroup(t);
		} else {
			switch(ao) {
				case MAX:
					break;
				case MIN:
					break;
				case AVG:
					break;
				case COUNT:
//					countMap.put(groupKey, countMap.getOrDefault(groupKey, 0) + 1);
					break;
				case SUM:
					break;
			}
		} 
	}
	
	// if groupBy
	public void mergeGroup(Tuple t) {
		
	}
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		return null;
	}

}
