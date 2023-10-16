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
	
	private AggregateOperator o;
	private Boolean groupBy;
	private TupleDesc td;
	private IntField vanilla; 
	private int avgCount;
	private HashMap<Field, Field> theMap;
	private HashMap<Field, Integer> occuranceCount;

	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here
		this.o = o;
		this.groupBy = groupBy;
		this.td = td;
		
		this.theMap = new HashMap<Field, Field>();
		this.occuranceCount = new HashMap<Field, Integer>();


		switch(this.o){
			case MAX:
				this.vanilla = new IntField(Integer.MIN_VALUE);
				break;
			case MIN:
				this.vanilla = new IntField(Integer.MAX_VALUE);
				break;
			case AVG:
				this.vanilla = new IntField(0);
				this.avgCount = 0;
				break;
			case COUNT:
				this.vanilla = new IntField(0);
				break;
			case SUM:
				this.vanilla = new IntField(0);
				break;
		}



	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {
		//your code here
		if (this.groupBy) {
			groupByMerge(t);
		}
		
		IntField tupleIntField = (IntField) t.getField(0);
		
		switch(this.o){
			case MAX:
				if (this.vanilla.compare(RelationalOperator.LT, t.getField(0))) {
					this.vanilla = new IntField(tupleIntField.getValue());
				}
				break;
			case MIN:
				if (this.vanilla.compare(RelationalOperator.GT, t.getField(0))) {
					this.vanilla = new IntField(tupleIntField.getValue());
				}
				break;
			case AVG:
				this.vanilla = new IntField(this.vanilla.getValue() + tupleIntField.getValue());
				this.avgCount += 1;
				break;
			case COUNT:
				this.vanilla = new IntField(this.vanilla.getValue() + 1);
				break;
			case SUM:
				this.vanilla = new IntField(this.vanilla.getValue() + tupleIntField.getValue());
				break;
		}
	}
	
	public void groupByMerge(Tuple t) {
		Field key = t.getField(0);
		IntField oldValueForMin = (IntField) this.theMap.getOrDefault(key, new IntField(Integer.MAX_VALUE));
		IntField oldValueForMax = (IntField) this.theMap.getOrDefault(key, new IntField(Integer.MIN_VALUE));
		IntField oldValue = (IntField) this.theMap.getOrDefault(key, new IntField(0));
		int oldCount = this.occuranceCount.getOrDefault(key, 0);

		switch(this.o){
			case MAX:
				if (oldValueForMax.compare(RelationalOperator.LT, t.getField(1))) {
					this.theMap.put(key, t.getField(1));
				}
				break;
			case MIN:
				if (oldValueForMin.compare(RelationalOperator.GT, t.getField(1))) {
					this.theMap.put(key, t.getField(1));
				}
				break;
			case AVG:
				this.theMap.put(key, new IntField(oldValue.getValue() + ((IntField) t.getField(1)).getValue()));
				this.occuranceCount.put(key, oldCount + 1);
				break;
			case COUNT:
				this.theMap.put(key, new IntField(oldValue.getValue() + 1));
				break;
			case SUM:
				this.theMap.put(key, new IntField(oldValue.getValue() + ((IntField) t.getField(1)).getValue()));
				break;
		}
	}
	
	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {
		//your code here
		if (!this.groupBy) {
			return this.getResultsVanilla();
		}else {
			return this.getResultsGroupBy();
		}
	}
	
	private ArrayList<Tuple> getResultsVanilla(){
		Tuple resTuple = new Tuple(td);
		ArrayList<Tuple> res = new ArrayList<Tuple>();
		
		if (this.o == AggregateOperator.AVG) {
			resTuple.setField(0,new IntField(this.vanilla.getValue()/this.avgCount));
		}else {
			resTuple.setField(0, vanilla);
		}
		res.add(resTuple);

		return res;
		
	}
	
	private ArrayList<Tuple> getResultsGroupBy(){
		ArrayList<Tuple> res = new ArrayList<Tuple>();
		
		for (Field key : this.theMap.keySet()) {
			Tuple resTuple = new Tuple(td);
			resTuple.setField(0, key);
			if (this.o == AggregateOperator.AVG) {
				resTuple.setField(1, new IntField(((IntField)this.theMap.get(key)).getValue()/this.occuranceCount.get(key)));
			}else {
				resTuple.setField(1, this.theMap.get(key));

			}
			res.add(resTuple);
		} 
		return res;
		
	}

}
