package havis.transport.common;

import java.util.ArrayList;
import java.util.List;

import havis.transport.Subscriber;

public class SubscriberConfiguration {

	private List<Subscriber> subscribers;

	public SubscriberConfiguration() {
	}

	public List<Subscriber> getSubscribers() {
		if (subscribers == null) {
			subscribers = new ArrayList<Subscriber>();
		}
		return subscribers;
	}

	public void setSubscribers(List<Subscriber> subscribers) {
		this.subscribers = subscribers;
	}
}
