package com.study.beam;

import java.io.Serializable;

public class CustomerEntity implements Serializable{
	
	private String id;
	
	public CustomerEntity(String id, String name) {
		this.id = id;
		this.name = name;
	}
	

	public CustomerEntity() {
		super();
		// TODO Auto-generated constructor stub
	}


	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	private String name;
	
	

}
