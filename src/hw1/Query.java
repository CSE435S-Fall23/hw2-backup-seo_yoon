package hw1;

import java.util.ArrayList;
import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();

		//your code here
		
		List<SelectItem> selectItems = sb.getSelectItems();
		FromItem fromItem = sb.getFromItem();
		List<Join> joins = sb.getJoins();
		Expression where = sb.getWhere();
		

		Catalog catalog = Database.getCatalog();
		
		
		// SET UP RELATION
		int tableId = catalog.getTableId(fromItem.toString());
		ArrayList<Tuple> tuples = catalog.getDbFile(tableId).getAllTuples();
		TupleDesc tupleDesc = catalog.getDbFile(tableId).getTupleDesc();
		
		Relation relation = new Relation(tuples, tupleDesc);
		
		System.out.println(sb.toString());
		
		
		//JOIN
		if (joins != null) {
			for (Join j : joins) {
				Expression onExp = j.getOnExpression();
				FromItem rightItem = j.getRightItem();
				
				// SET UP OTHER RELATION 
				int otherTableId = catalog.getTableId(rightItem.toString());
				ArrayList<Tuple> otherTuples = catalog.getDbFile(otherTableId).getAllTuples();
				TupleDesc otherTupleDesc = catalog.getDbFile(otherTableId).getTupleDesc();
				Relation other = new Relation(otherTuples, otherTupleDesc);
				
				
				String[] split = onExp.toString().split(" = ");
				String[] leftOn = split[0].split("\\.");
				String[] rightOn = split[1].split("\\.");
				
				
				int field1 = relation.getDesc().nameToId(leftOn[1]);
				int field2 = other.getDesc().nameToId(rightOn[1]);

				
				relation = relation.join(other, field1, field2);
				
			}
		}
		
		
		//WHERE
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		if (where != null) {
			where.accept(whereVisitor);
			
			// whereVisitor.getLeft() returns String
			// select() params are int, RelationalOperator, Field
			relation = relation.select(
					relation.getDesc().nameToId(whereVisitor.getLeft()), whereVisitor.getOp(), whereVisitor.getRight());
		}
		
		
		
		//SELECT
		ArrayList<Integer> fieldsArr = new ArrayList<Integer>();
		boolean isGroupBy = false;
		
		if (sb.getGroupByColumnReferences() != null) {
			isGroupBy = true;
		}
		
		ColumnVisitor cv = new ColumnVisitor();

		
		if (selectItems != null && !selectItems.get(0).toString().equals("*")) {
			
			
			for (SelectItem item : selectItems) {

				item.accept(cv);
				
				String selectCol = cv.isAggregate() ? cv.getColumn() : item.toString();
				fieldsArr.add(relation.getDesc().nameToId(selectCol));
				System.out.println(selectCol);
			}
			
			relation = relation.project(fieldsArr);
			
			if (cv.isAggregate()) {
				relation = relation.aggregate(cv.getOp(), isGroupBy);
			} 			
		}

		return relation;
		
	}
}
