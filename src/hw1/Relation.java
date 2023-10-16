package hw1;

import java.util.ArrayList;

/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;
	
	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}
	
	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here
		
		Relation r = new Relation(new ArrayList<>(), getDesc());
		for (Tuple t : getTuples()) {
			if (t.getField(field).compare(op, operand)) {
				r.tuples.add(t);
			}
		}
		
		return r;
	}
	
	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here
		String[] fieldsArr = this.td.getFields();
		ArrayList<Tuple> tuplesArr = this.tuples;
		
		for (int i = 0; i < fields.size(); i++) {
			if (fields.get(i) != null) {
				fieldsArr[fields.get(i)] = names.get(i);
			}
		}
		
		TupleDesc newTd = new TupleDesc(td.getTypes(), fieldsArr);
		for (Tuple t:tuplesArr) {
			t.setDesc(newTd);
		}
		Relation r1 = new Relation(tuplesArr, newTd);
		return r1;
	}
	
	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here
		String[] newFields = new String[fields.size()];
		Type[] newTypes = new Type[fields.size()];
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		// get the info of the fields to be projected
		for (int i = 0; i < fields.size(); i++) {
			newTypes[i] = td.getType(fields.get(i));
			newFields[i] = td.getFieldName(fields.get(i));
		}
		
		// create a new TupleDesc with projected fields
		TupleDesc newTd = new TupleDesc(newTypes, newFields);
		
		for (Tuple t: tuples) {
			Tuple tup = new Tuple(newTd);
			for (int i = 0; i < fields.size(); i++) {
				tup.setField(i, t.getField(fields.get(i)));
			}
			newTuples.add(tup);
		}
		
		return new Relation(newTuples, newTd);
	}
	
	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		//your code here
		
		ArrayList<Type> newTypes = new ArrayList<Type>();
		ArrayList<String> newFields = new ArrayList<String>();
		
		
		Type[] ogTypes = td.getTypes();
		String[] ogFields = td.getFields();
		
		for (int i = 0; i < ogTypes.length; i++) {
			newTypes.add(ogTypes[i]);
			newFields.add(ogFields[i]);
		}
		
		Type[] otherTypes = other.td.getTypes();
		String[] otherFields = other.td.getFields();
		for (int i = 0; i < otherTypes.length; i++) {
			newTypes.add(otherTypes[i]);
			newFields.add(otherFields[i]);
		}
		
		
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		TupleDesc newTd = new TupleDesc(newTypes.toArray(new Type[0]), newFields.toArray(new String[0]));
		
		for (Tuple tpOg: tuples) {
			for (Tuple tpOther: other.tuples) {
				if (tpOg.getField(field1).compare(RelationalOperator.EQ, tpOther.getField(field2))) {
					Tuple nt = new Tuple(newTd);
					for (int i = 0; i < ogTypes.length; i++) {
						nt.setField(i, tpOg.getField(i));
					}
					int fieldIndex = ogTypes.length;
					for (int i = 0; i < otherTypes.length; i++) {
						nt.setField(fieldIndex, tpOther.getField(i));
						fieldIndex += 1;
					}
					newTuples.add(nt);
					
				}
				
			}
		}
		
		Relation res = new Relation(newTuples, newTd);
		
		return res;
	}
	
	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		Aggregator aggregator = new Aggregator(op, groupBy,this.td);
		
		for (Tuple t : tuples) {
			aggregator.merge(t);
		}
		Relation res = new Relation(aggregator.getResults(), this.td);
		return res;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}
	
	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}
	
	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		System.out.println(td.numFields());
		String res = td.toString();
		res += "\n";
		
		for (Tuple t : tuples) {
			res += t.toString();
			res += " ";
		}
		return res;
	}
}
