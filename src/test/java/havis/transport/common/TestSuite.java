package havis.transport.common;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({ CommonSubscriptorManagerTest.class, CommonSubscriberManagerTest.class, PlainDataConverterTest.class, TransportTest.class })
public class TestSuite {
}