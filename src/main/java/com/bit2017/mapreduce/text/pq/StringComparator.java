package com.bit2017.mapreduce.text.pq;

import java.util.Comparator;

public class StringComparator implements Comparator<String> {

	@Override
	public int compare(String o1, String o2) {
		if( o1.length() == o2.length() ){
//			System.out.println("       " +o1 +"  =="+ o2);
			return 0;
		} else if( o1.length() > o2.length() ){
//			System.out.println("       " +o1 +"  >"+ o2);
			return 1;
		} else {
//			System.out.println("       " +o1 +" else"+ o2);
			return -1;
		}
		
	}

}
