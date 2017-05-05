import java.util.LinkedList;

public class main {

	public static void main(String[] args) {
		LinkedList<Integer>a=new LinkedList<>();
		
		for(int i=0;i<50;i++)
			a.add(new Integer(i));
		
		LinkedList<Integer>b=new LinkedList<>();
		
		for(Integer m:a)
			b.add(m);
		
		
		for(Integer  h: b)
			System.out.println(h);

	}

}
