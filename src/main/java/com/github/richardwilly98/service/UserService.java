package com.github.richardwilly98.service;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.richardwilly98.User;

@Path("/users")
public class UserService {

	private static Logger log = Logger.getLogger(UserService.class);
	private final static String index = "test-users";
	private final static String type = "user";
	private static ObjectMapper mapper = new ObjectMapper();

	private List<User> users = Arrays.asList(new User("dsandron", "Danilo",
			"Bankok"), new User("rlouapre", "Richard", "Jersey City"));

	private Client client;

	private Client getClient() {
		if (client == null) {
			client = new TransportClient()
					.addTransportAddress(new InetSocketTransportAddress(
							"localhost", 9300));
		}
		return client;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/{id}")
	public User get(@PathParam("id") String id) {
		User user = null;
		for (User u : users) {
			if (u.getId().equalsIgnoreCase(id)) {
				user = u;
				break;
			}
		}
		return user;
	}

	@GET
	@Produces({ MediaType.APPLICATION_JSON })
	@Path("/find/{name}")
	public List<User> find(@PathParam("name") String name) {
		return getUser(name);
	}

	@POST
	@Path("/")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response create(User user) {
		if (user == null) {
			throw new IllegalArgumentException("user");
		}
		String id = createUser(user);
		user.setId(id);
		return Response.status(201).entity(user).build();
	}

	private List<User> getUser(String name) {
		List<User> users = new ArrayList<User>();
		if (!getClient().admin().indices().prepareExists(index).execute()
				.actionGet().exists()) {
			getClient().admin().indices().prepareCreate(index).execute()
					.actionGet();
			// Force index to be refreshed.
			getClient().admin().indices().refresh(new RefreshRequest(index))
					.actionGet();
			for (User u : this.users) {
				String json;
				try {
					json = mapper.writeValueAsString(u);
					IndexResponse response = getClient()
							.prepareIndex(index, type).setId(u.getId())
							.setSource(json).execute().actionGet();
					log.trace(String.format("Index: %s  - Type: %s - Id: %s",
							response.getIndex(), response.getType(),
							response.getId()));
				} catch (JsonProcessingException e) {
					log.error("Json processing exception.", e);
				}
			}
		}
		SearchResponse searchResponse = getClient().prepareSearch(index)
				.setTypes(type).setQuery(QueryBuilders.queryString(name))
				.execute().actionGet();
		log.debug("totalHits: " + searchResponse.getHits().totalHits());
		for (SearchHit hit : searchResponse.getHits().hits()) {
			String json = hit.getSourceAsString();
			try {
				User user = mapper.readValue(json, User.class);
				users.add(user);
			} catch (Throwable t) {
				log.error("Json processing exception.", t);
			}
		}

		return users;
	}

	private String createUser(User user) {
		if (user.getId() == null) {
			user.setId(generateUniqueId(user));
		}
		String json;
		try {
			json = mapper.writeValueAsString(user);
			log.trace(json);
			IndexResponse response = getClient().prepareIndex(index, type)
					.setId(user.getId()).setSource(json).execute().actionGet();
			log.trace(String.format("Index: %s  - Type: %s - Id: %s",
					response.getIndex(), response.getType(), response.getId()));
			return response.getId();
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}

	private String generateUniqueId(User user) {
		return UUID.randomUUID().toString();
	}
}
