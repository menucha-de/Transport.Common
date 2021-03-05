package havis.transport.common;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Dummy {

	private String lname;
	private String fname;
	private int age;
	private List<Dummy> contacts = new ArrayList<Dummy>();
	private Map<String, String> complexType = new HashMap<String, String>();
	private Dummy subDummy;
	private Date born;
	private List<ArrayList<Integer>> lists = new ArrayList<ArrayList<Integer>>();

	public Dummy() {
	}

	public Dummy(String lastName, String firstName, int age, Dummy[] contacts, Dummy subDummy) {
		this.lname = lastName;
		this.fname = firstName;
		this.age = age;
		this.born = new Date(0);
		ArrayList<Integer> listOne = new ArrayList<Integer>();
		listOne.add(Integer.valueOf(0));
		listOne.add(Integer.valueOf(1));
		ArrayList<Integer> listTwo = new ArrayList<Integer>();
		listTwo.add(Integer.valueOf(0));
		listTwo.add(Integer.valueOf(1));
		listTwo.add(Integer.valueOf(2));
		lists.add(listOne);
		lists.add(listTwo);
		if (contacts != null) {
			this.contacts = Arrays.asList(contacts);
		}
		if (subDummy != null)
			this.subDummy = subDummy;
	}

	public String getLName() {
		return lname;
	}

	public void setLName(String lname) {
		this.lname = lname;
	}

	public String getFName() {
		return fname;
	}

	public List<ArrayList<Integer>> getLists() {
		return lists;
	}

	public Date getBorn() {
		return born;
	}

	public void setFName(String fname) {
		this.fname = fname;
	}

	public int getAge() {
		return age;
	}

	public void setAge(int age) {
		this.age = age;
	}

	public List<Dummy> getContacts() {
		return contacts;
	}

	public void setContacts(List<Dummy> contacts) {
		this.contacts = contacts;
	}

	public Map<String, String> getComplexType() {
		return complexType;
	}

	public void setComplexType(Map<String, String> complexType) {
		this.complexType = complexType;
	}

	public Dummy getSubDummy() {
		return subDummy;
	}

	public void setSubDummy(Dummy subDummy) {
		this.subDummy = subDummy;
	}
}
