package forklift.replay;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestClientFactory;
import io.searchbox.client.config.HttpClientConfig;
import io.searchbox.core.Delete;
import io.searchbox.core.Index;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;
import io.searchbox.params.Parameters;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;

public class ReplayESWriter extends ReplayStoreThread<ReplayESWriterMsg> {
    private static final Logger log = LoggerFactory.getLogger(ReplayES.class);

    private final JestClient restClient;

    public ReplayESWriter(String hostname) {
        this(hostname, 9300, "elasticsearch");
    }

    public ReplayESWriter(String hostname, int port, String clusterName) {
        final JestClientFactory jestFactory = new JestClientFactory();
        jestFactory.setHttpClientConfig(
                 new HttpClientConfig.Builder("http://" + hostname + ":" + port)
                     .multiThreaded(true)
                     .build());
        restClient = jestFactory.getObject();
    }

    @Override
    protected void poll(ReplayESWriterMsg t) {
        final String index = "forklift-replay-" + LocalDate.now().format(DateTimeFormatter.BASIC_ISO_DATE);

        deleteOldLogs(t.getId(), index);
        writeMessage(t, index);
    }


    public void deleteOldLogs(String id, String index) {
        // In order to ensure there is only one replay msg for a given id we have to clean the msg id from
        // any previously created indexes.
        try {
            final int from = 0;
            final int size = 50;
            final String query = "{\"term\": {\"_id\": \"" + id + "\"}}";
            final SearchResult result = restClient.execute(
                new Search.Builder("{" +
                                   "\"from\":" + from + "," +
                                   "\"size\":" + size + "," +
                                   "\"query\":" + query +
                                   "}")
                .addIndex("forklift-replay*")
                .build());

            if (result != null) {
                final List<SearchResult.Hit<Map, Void>> hits = result.getHits(Map.class);
                if (hits != null) {
                    for (SearchResult.Hit<Map, Void> hit : hits) {
                        if (hit.index == null || hit.index.equals(index)) {
                            continue;
                        }

                        restClient.execute(new Delete.Builder(id)
                                           .index(hit.index)
                                           .type("log").build());
                    }
                }
            }
        } catch (Exception e) {
            log.error("Error deleting old replay logs {}", id, e);
        }
    }

    public void writeMessage(ReplayESWriterMsg t, String index) {
        try {
            restClient.execute(new Index.Builder(t.getFields())
                               .id(t.getId())
                               .type("log")
                               .index(index)
                               .setParameter(Parameters.VERSION, t.getVersion())
                               .setParameter(Parameters.VERSION_TYPE, "external_gte")
                               .build());
        } catch (IOException e) {
            log.error("Unable to write fields to replay log: {}", t.getFields(), e);
        }
    }
}
