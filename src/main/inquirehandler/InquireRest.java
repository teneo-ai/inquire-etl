package main.inquirehandler;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.core.StreamReadConstraints;
import com.fasterxml.jackson.databind.AnnotationIntrospector;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.introspect.JacksonAnnotationIntrospector;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.fasterxml.jackson.jaxrs.json.JacksonJsonProvider;
import com.fasterxml.jackson.module.jaxb.JaxbAnnotationIntrospector;
import org.glassfish.jersey.media.multipart.MultiPartFeature;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.io.InputStream;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.TimeZone;

public class InquireRest {
    protected final AuthorizationFilter authorizationFilter;
    private static final Client client;
    public final WebTarget webTarget;

    static {
        // init object mapper
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");
        sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
        ObjectMapper objectMapper = (new ObjectMapper()).configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false).setSerializationInclusion(JsonInclude.Include.NON_NULL).setAnnotationIntrospector(AnnotationIntrospector.pair(new JaxbAnnotationIntrospector(), new JacksonAnnotationIntrospector())).setDateFormat(sdf);
        objectMapper.getFactory().setStreamReadConstraints(StreamReadConstraints.builder().maxStringLength(Integer.MAX_VALUE).build());
        objectMapper.registerModule(new JodaModule());

        // init client
        client = ClientBuilder.newClient();

        final JacksonJsonProvider jsonProvider = new JacksonJsonProvider(objectMapper);
        client.register(jsonProvider);
        jsonProvider.removeUntouchable(String.class);
        client.property("jersey.config.workers.legacyOrdering", true);


        // Disable other JSON providers
        client.property("jersey.config.client.jsonFeature", "Teneo");
        // Disable WARN about unavailable default providers
        client.property("jersey.config.disableDefaultProvider", "ALL");
        client.register(MultiPartFeature.class);
    }

    public InquireRest(final URL serverUrl, final String accessToken) throws IllegalArgumentException {

        if (serverUrl == null) {
            throw new IllegalArgumentException("Server URL parameter cannot be null.");
        }

        // webTarget
        webTarget = client.target(serverUrl.toString()).path("rest");

        authorizationFilter = new AuthorizationFilter(accessToken);
        webTarget.register(authorizationFilter);
    }

    public InquireRest(final WebTarget webTarget, final AuthorizationFilter authorizationFilter) throws IllegalArgumentException {
        this. webTarget = webTarget;
        this.authorizationFilter = authorizationFilter;
        webTarget.register(authorizationFilter);
    }

    protected <T> T doPost(WebTarget webTarget, Entity<?> entity, Class<T> returnType) {
        return doPost(webTarget, entity, determineMediaType(returnType)).readEntity(returnType);
    }

    protected Response doPost(WebTarget webTarget, Entity<?> entity, MediaType acceptedMediaType) {
        Response response = webTarget.request(acceptedMediaType).post(entity);
        return response;
    }

    protected Response doPost(WebTarget webTarget, Entity<?> entity) {
        return doPost(webTarget, entity, MediaType.APPLICATION_JSON_TYPE);
    }

    protected <T> T doGet(WebTarget webTarget, GenericType<T> returnType) {
        return doGet(webTarget).readEntity(returnType);
    }

    public Response doGet(WebTarget webTarget) {
        return doGet(webTarget, MediaType.APPLICATION_JSON_TYPE);
    }

    protected Response doGet(WebTarget webTarget, MediaType acceptedMediaType) {
        Response response = webTarget.request(acceptedMediaType).get();
        return response;
    }

    private static <T> MediaType determineMediaType(Class<T> returnType) {
        return InputStream.class.isAssignableFrom(returnType) || byte[].class.equals(returnType)
                ? MediaType.APPLICATION_OCTET_STREAM_TYPE
                : MediaType.APPLICATION_JSON_TYPE;
    }

}
