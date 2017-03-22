package com.bit2017.mapreduce.text.pq;

import java.util.PriorityQueue;

public class TestMain {

	public static void main(String[] args) {
		PriorityQueue<String> pq = new PriorityQueue<String>(10, new StringComparator() );
		pq.add("hello");
		pq.add("hello World");
		pq.add("hi");
		pq.add("how are you");
		pq.add("fine");
		pq.add("thx");
		
		while(pq.isEmpty() == false) {
			String output = pq.remove();
			System.out.println(output);
		}
		
		
	}

}
