package simpledb;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private Field[] tupleFields;
    private TupleDesc tupleSchema;
    private RecordId recordId;
    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td
     *            the schema of this tuple. It must be a valid TupleDesc
     *            instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleFields=new Field[td.numFields()];
        tupleSchema = td;
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return this.tupleSchema;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     *         be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return this.recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid
     *            the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i
     *            index of the field to change. It must be a valid index.
     * @param f
     *            new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        tupleFields[i] = f;
    }

    /**
     * @return the value of the ith field, or null if it has not been set.
     *
     * @param i
     *            field index to return. Must be a valid index.
     */
    public Field getField(int i) {
        // some code goes here
        return tupleFields[i];
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     *
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     *
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
        String descrip = "";
        for (int i =0; i< tupleFields.length;i++){
            if (tupleFields[i]==null){
                descrip += "null"+"\t";
            }else {
                descrip += tupleFields[i].toString()+"\t";
            }

        }
        descrip = descrip.substring(0, descrip.length() - 1) + "\n";
        return descrip;
    }

    /**
     * @return
     *        An iterator which iterates over all the fields of this tuple
     * */
    public Iterator<Field> fields()
    {
        // some code goes here
        return Arrays.asList(tupleFields).iterator();
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     * */
    public void resetTupleDesc(TupleDesc td)
    {
        // some code goes here
        tupleSchema = td;
    }
    public static Tuple merge(Tuple t1, Tuple t2) {
        // some code goes here
        TupleDesc td1=t1.getTupleDesc(), td2= t2.getTupleDesc();

        TupleDesc mergedTd = TupleDesc.merge(td1,td2);


        Tuple mergedTp = new Tuple(mergedTd);
        int i;
        //Clever use of i and j.
        for(i = 0; i < td1.numFields(); i++){
            mergedTp.setField(i, t1.getField(i));
        }

        for(int j = 0; j < td2.numFields(); j++){
            mergedTp.setField(i, t2.getField(j));
            i++;
        }

        //create a merged Tuple.
        return mergedTp;

    }
}
