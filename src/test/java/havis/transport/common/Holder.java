package havis.transport.common;

import javax.xml.bind.annotation.XmlRootElement;

@XmlRootElement(name = "Holder")
public class Holder<T> {

	private T value;

	public Holder() {
	}

	public Holder(T value) {
		this.value = value;
	}

	public T getValue() {
		return value;
	}

	public void setValue(T value) {
		this.value = value;
	}
}