package com.virtualpairprogrammers.model;

import java.util.List;

public class Layout {
	
	private String report;
	private List<Line> lines;
	
	public Layout() {
		
	}

	public String getReport() {
		return report;
	}

	public void setReport(String report) {
		this.report = report;
	}

	public List<Line> getLines() {
		return lines;
	}

	public void setLines(List<Line> lines) {
		this.lines = lines;
	}

	@Override
	public String toString() {
		return "Layout [report=" + report + ", lines=" + lines + "]";
	}
}
