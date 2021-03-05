package havis.transport.common.rest.provider;

import havis.transport.TransportException;

import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.ext.ExceptionMapper;
import javax.ws.rs.ext.Provider;

@Provider
public class TransportExceptionMapper implements ExceptionMapper<TransportException> {

	@Override
	public Response toResponse(TransportException e) {
		return Response.status(Response.Status.BAD_GATEWAY).entity(e.getMessage()).type(MediaType.TEXT_PLAIN).build();
	}
}