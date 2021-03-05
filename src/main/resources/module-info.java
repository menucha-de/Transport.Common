module havis.transport.common {
    requires havis.transform.api;
    requires iot.device.client;
    requires jackson.core;
    requires java.logging;
    requires java.sql;
    requires jaxb.api;
    requires paho.client;
    requires resteasy.jaxrs;
    requires supercsv;
    requires havis.net.rest.shared;
    
    requires havis.transport.api;
    requires transitive havis.util.monitor;
    requires transitive jackson.databind;
    requires transitive javax.ws.rs.api;

    exports havis.transport.common;
    exports havis.transport.common.rest;
    exports havis.transport.common.rest.provider;

    uses havis.transform.TransformerFactory;
}
