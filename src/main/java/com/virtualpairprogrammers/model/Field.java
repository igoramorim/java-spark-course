package com.virtualpairprogrammers.model;

public class Field {
	
	private Integer position;
	private String description;
	private Integer size;
	private String pad;
	private String fill;
	
	public Field() {
		
	}
	
	public Integer getPosition() {
		return position;
	}
	public void setPosition(Integer position) {
		this.position = position;
	}
	public Integer getSize() {
		return size;
	}
	public void setSize(Integer size) {
		this.size = size;
	}
	public String getPad() {
		return pad;
	}
	public void setPad(String pad) {
		this.pad = pad;
	}
	public String getFill() {
		return fill;
	}
	public void setFill(String fill) {
		this.fill = fill;
	}

	public String getDescription() {
		return description;
	}

	public void setDescription(String description) {
		this.description = description;
	}
	
}
