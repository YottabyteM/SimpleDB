package simpledb;

import java.util.ArrayList;
import java.util.HashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private int gbField;
    private Type gbFieldType;
    private int aggregateField;
    private Op op;
    private boolean noGrouping = false;
    private HashMap<Field, Integer> groups;
    private HashMap<Field, Integer> counts;
    private String fieldName="", groupFieldName="";
    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        if (gbfield==Aggregator.NO_GROUPING) {
            noGrouping=true;
        }
        gbField = gbfield;
        gbFieldType = gbfieldtype;
        aggregateField = afield;
        op = what;
        groups = new HashMap<Field, Integer>();
        counts = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field key;
        int value;
        int currentAggregateValue;
        int currentCount;
        fieldName=tup.getTupleDesc().getFieldName(aggregateField);
        if (noGrouping) {
            key = new IntField(Aggregator.NO_GROUPING);
        } else {
            key = tup.getField(gbField);
            groupFieldName=tup.getTupleDesc().getFieldName(gbField);

        }
        value = ((IntField)tup.getField(aggregateField)).getValue();

        if (counts.containsKey(key)) {
            currentCount = counts.get(key);
        }
        if (groups.containsKey(key)) {
            currentAggregateValue = groups.get(key);
        } else {
            if (op==Op.COUNT || op==Op.SUM || op==Op.AVG){
                groups.put(key, 0);
                counts.put(key, 0);
            }
            if (op==Op.MAX) {
                groups.put(key, -99999);
                counts.put(key, 0);
            }
            if (op==Op.MIN) {
                groups.put(key, 99999);
                counts.put(key, 0);
            }
        }
        currentAggregateValue = groups.get(key);
        currentCount = counts.get(key);


        if (op==Op.MIN){
            if (value < currentAggregateValue){
                currentAggregateValue = value;
                groups.put(key, currentAggregateValue);
            }
        }

        if (op==Op.MAX) {
            if (value > currentAggregateValue) {
                currentAggregateValue=value;
                groups.put(key, currentAggregateValue);
            }
        }

        if (op==Op.COUNT){
            currentAggregateValue++;
            groups.put(key, currentAggregateValue);
        }

        if (op==Op.SUM){
            currentAggregateValue += value;
            groups.put(key, currentAggregateValue);
        }

        if (op==Op.AVG){
            currentCount++;
            counts.put(key, currentCount);
            currentAggregateValue += value;
            groups.put(key, currentAggregateValue);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     * 
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
        ArrayList<Tuple> tuples = new ArrayList<Tuple>();
        TupleDesc td = this.getTupleDesc();

        if (noGrouping) {
            for (Field key : groups.keySet()){
                int value = groups.get(key);
                if (op == Op.AVG) {
                    value = value/counts.get(key);
                }
                Tuple tuple = new Tuple(td);
                tuple.setField(0, new IntField(value));
                tuples.add(tuple);
            }
        } else {
            for (Field key : groups.keySet()){
                int value = groups.get(key);
                if (op==Op.AVG){
                    value=value/counts.get(key);
                }

                Tuple tuple = new Tuple(td);
                tuple.setField(0, key);
                tuple.setField(1, new IntField(value));
                tuples.add(tuple);
            }
        }
        return new TupleIterator(td, tuples);
    }
    public TupleDesc getTupleDesc() {
        Type[] typeAr;
        String[] stringAr;
        TupleDesc td;

        if (noGrouping) {
            typeAr = new Type[1];
            stringAr = new String[1];
            typeAr[0] = Type.INT_TYPE;
            stringAr[0] = fieldName;
        } else {
            typeAr = new Type[2];
            stringAr = new String[2];
            typeAr[0] = gbFieldType;
            typeAr[1] = Type.INT_TYPE;
            stringAr[0] = groupFieldName;
            stringAr[1] = fieldName;
        }
        td = new TupleDesc(typeAr, stringAr);
        return td;
    }
}
