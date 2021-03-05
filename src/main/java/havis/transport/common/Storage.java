package havis.transport.common;

import havis.transport.TransportException;

import java.io.StringWriter;
import java.util.HashMap;
import java.util.Map;

import javax.ws.rs.DELETE;
import javax.ws.rs.DefaultValue;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("storage")
public class Storage {

	private Map<String, JdbcTransporter<?>> map = new HashMap<>();

	public final static Storage INSTANCE = new Storage();

	@Produces(MediaType.APPLICATION_OCTET_STREAM)
	@GET
	@Path("{name}")
	public Response download(@PathParam("name") String name, @QueryParam("limit") @DefaultValue("1000") int limit,
			@QueryParam("offset") @DefaultValue("0") int offset) {
		JdbcTransporter<?> connector = map.get(name);

		if (connector == null)
			return Response.noContent().build();

		StringWriter writer = new StringWriter();
		try {
			connector.marshal(writer, limit, offset);
			return Response.ok(writer.toString()).build();
		} catch (TransportException e) {
			return Response.serverError().build();
		}
	}

	@DELETE
	@Path("{name}")
	public Response clear(@PathParam("name") String name) {
		JdbcTransporter<?> connector = map.get(name);

		if (connector == null)
			return Response.noContent().build();

		try {
			int result = connector.clear();
			if (result > 0)
				return Response.ok().build();
			return Response.notModified().build();
		} catch (TransportException e) {
			return Response.serverError().build();
		}
	}

	public void put(String name, JdbcTransporter<?> connector) {
		map.put(name, connector);
	}

	public void remove(String name) {
		map.remove(name);
	}
}