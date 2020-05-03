package com.virtualpairprogrammers.model;

import java.util.List;

public class Line {
	
	private String type;
	private List<Field> fields;
	
	public Line() {
		
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public List<Field> getFields() {
		return fields;
	}

	public void setFields(List<Field> fields) {
		this.fields = fields;
	}

	@Override
	public String toString() {
		return "Line [type=" + type + ", fields=" + fields + "]";
	}
	
}
