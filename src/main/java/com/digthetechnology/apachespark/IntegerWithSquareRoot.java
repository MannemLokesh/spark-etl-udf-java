package com.digthetechnology.apachespark;

public class IntegerWithSquareRoot {
	
	private int originalValue;
	
	private double squarerootValue;
	
	public int getOriginalValue() {
		return originalValue;
	}

	public double getSquarerootValue() {
		return squarerootValue;
	}

	public IntegerWithSquareRoot(int i) {
		this.originalValue = i;
		this.squarerootValue = Math.sqrt(i);
	}

	@Override
	public String toString() {
		return "IntegerWithSquareRoot [originalValue=" + originalValue + ", squarerootValue=" + squarerootValue + "]";
	}

}
