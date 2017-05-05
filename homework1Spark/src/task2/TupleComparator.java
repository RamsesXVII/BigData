package task2;

import java.util.Comparator;

import scala.Serializable;
import scala.Tuple3;

public class TupleComparator implements Comparator<Tuple3<String,String,String>>, Serializable{
    @Override
    public int compare(Tuple3<String, String,String> tuple1, Tuple3<String,String,String> tuple2) {
        return tuple1._1().compareTo(tuple2._1());
    }
}