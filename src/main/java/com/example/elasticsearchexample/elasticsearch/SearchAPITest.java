package com.example.elasticsearchexample.elasticsearch;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.util.EntityUtils;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.ResponseException;
import org.elasticsearch.client.RestClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.util.ObjectUtils;
import org.springframework.web.bind.annotation.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/v1")
public class SearchAPITest {

    @Autowired
    private RestClient restClient;

    // Get all index
    @GetMapping("/index")
    public List<String> getIndex() {
        Request request = new Request("GET", "/_all");
        try {
            Response client = restClient.performRequest(request);
            ObjectMapper objectMapper = new ObjectMapper();
            JsonNode node = objectMapper.readTree(EntityUtils.toString(client.getEntity()));
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> result = mapper.convertValue(node, new TypeReference<Map<String, Object>>() {});
            List<String> listIndex = new ArrayList<>();
            result.forEach((s, o) -> listIndex.add(s));
            return listIndex;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //Create Index
    @PostMapping("/index")
    public String addIndex(@RequestParam(name = "index") String index) {
        Request request = new Request("PUT", "/" + index);
        try {
            Response client = restClient.performRequest(request);
            System.out.println(client);
        } catch (ResponseException e) {
            return "Add Index " + index + " failed";
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return "Add Index:" + index + " Success";
    }

    //Delete Index
    @DeleteMapping("/index")
    public String deleteIndex(@RequestParam(name = "index") String index) {
        {
            Request request = new Request("DELETE", "/" + index);
            try {
                Response client = restClient.performRequest(request);
                System.out.println(client);
            } catch (ResponseException e) {
                return "Delete Index " + index + " failed";
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            return "Delete Index:" + index + " Success";
        }
    }

    //Search document
    @GetMapping("/document/search")
    public JsonNode getAllDocumentOnIndex(@RequestParam(name = "index", required = false) String index, @RequestParam(value = "list-field") String listField, @RequestParam(value = "list-value") String valueField) {
        StringBuilder str = new StringBuilder();
        if (!ObjectUtils.isEmpty(index)) str.append("/").append(index);
        str.append("/_search");
        Request request = new Request("GET", str.toString());

        StringBuilder strBody = new StringBuilder("{\"query\":{\"bool\":{\"must\":[");
        List<String> fields = Arrays.asList(listField.split(","));
        List<String> valueFields = Arrays.asList(valueField.split(","));
        for (int i = 0; i < fields.size(); i++) {
            if (i > 0) strBody.append(",");
            strBody.append("{\"match\":{\"");
            strBody.append(fields.get(i));
            strBody.append("\":\"");
            try {
                strBody.append(valueFields.get(i));
            } catch (ArrayIndexOutOfBoundsException e) {
                try {
                    StringBuilder messageError = new StringBuilder();
                    messageError.append("{\"Error\":\"No value found ");
                    for (int j = i; j < fields.size(); j++) {
                        if (j > i) messageError.append(",");
                        messageError.append(fields.get(j));
                    }
                    messageError.append("\"}");
                    return new ObjectMapper().readTree(messageError.toString());
                } catch (JsonProcessingException ex) {
                    throw new RuntimeException(ex);
                }
            }
            strBody.append("\"}}");
        }
        strBody.append("]}}}");
        request.setJsonEntity(strBody.toString());
        try {
            Response client = restClient.performRequest(request);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(EntityUtils.toString(client.getEntity())).get("hits").get("hits");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    //Get all document
    @GetMapping("/document/get-all")
    public JsonNode getAll(@RequestParam(name = "index", required = false) String index) {
        StringBuilder str = new StringBuilder();
        if (!ObjectUtils.isEmpty(index)) str.append("/").append(index);
        str.append("/_search");
        Request request = new Request("GET", str.toString());
        try {
            Response client = restClient.performRequest(request);
            ObjectMapper objectMapper = new ObjectMapper();
            return objectMapper.readTree(EntityUtils.toString(client.getEntity())).get("hits").get("hits");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Add or update document
    @PostMapping("/document")
    public String createOrUpdate(@RequestParam(name = "index") String index, @RequestParam(name = "id", required = false) String id, @RequestBody String jsonBody) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append("/");
        endpoint.append(index).append("/_doc");
        if (!ObjectUtils.isEmpty(id)) endpoint.append("/").append(id);
        Request request = new Request("POST", endpoint.toString());
        request.setJsonEntity(jsonBody);
        try {
            Response client = restClient.performRequest(request);
            return client.getStatusLine().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // Delete document
    @DeleteMapping("/document")
    public String deleteDocument(@RequestParam(name = "index") String index, @RequestParam(name = "id") String id) {
        StringBuilder endpoint = new StringBuilder();
        endpoint.append("/");
        endpoint.append(index).append("/_doc");
        endpoint.append("/").append(id);
        Request request = new Request("DELETE", endpoint.toString());
        try {
            Response client = restClient.performRequest(request);
            return client.getStatusLine().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}