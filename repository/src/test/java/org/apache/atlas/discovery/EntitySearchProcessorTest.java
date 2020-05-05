package org.apache.atlas.discovery;

import com.google.common.collect.Sets;
import org.apache.atlas.BasicTestSetup;
import org.apache.atlas.SortOrder;
import org.apache.atlas.TestModules;
import org.apache.atlas.exception.AtlasBaseException;
import org.apache.atlas.model.discovery.SearchParameters;
import org.apache.atlas.repository.graphdb.AtlasGraph;
import org.apache.atlas.repository.graphdb.AtlasVertex;
import org.apache.atlas.repository.store.graph.v2.EntityGraphRetriever;
import org.apache.atlas.type.AtlasTypeRegistry;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Guice;
import org.testng.annotations.Test;

import javax.inject.Inject;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

@Guice(modules = TestModules.TestOnlyModule.class)
public class EntitySearchProcessorTest extends BasicTestSetup {

    @Inject
    private AtlasGraph graph;

    @Inject
    private AtlasTypeRegistry typeRegistry;

    @Inject
    private EntityGraphRetriever entityRetriever;

    @BeforeClass
    public void setup() {
        setupTestData();
    }


    @Test
    public void searchTablesByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_column");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        assertEquals(processor.getResultCount(), 4);
        assertEquals(processor.execute().size(), 4);
    }

    @Test
    public void searchByClassificationSortBy() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_table");
        params.setClassification("Metric");
        params.setLimit(10);
        params.setSortBy("createTime");
        params.setSortOrder(SortOrder.ASCENDING);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());
        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        List<AtlasVertex> vertices = processor.execute();

        assertEquals(processor.getResultCount(), 4);
        assertEquals(vertices.size(), 4);


        AtlasVertex firstVertex = vertices.get(0);

        Date firstDate = (Date) entityRetriever.toAtlasEntityHeader(firstVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        AtlasVertex secondVertex = vertices.get(1);
        Date secondDate = (Date) entityRetriever.toAtlasEntityHeader(secondVertex, Sets.newHashSet(params.getSortBy())).getAttribute(params.getSortBy());

        assertTrue(firstDate.before(secondDate));
    }

    @Test
    public void emptySearchByClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_table");
        params.setClassification("PII");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());

        EntitySearchProcessor processor = new EntitySearchProcessor(context);

        assertEquals(processor.getResultCount(), 0);
        assertEquals(processor.execute().size(), 0);
    }

    @Test(expectedExceptions = AtlasBaseException.class, expectedExceptionsMessageRegExp = "NotExisting: Unknown/invalid classification")
    public void searchByNonExistingClassification() throws AtlasBaseException {
        SearchParameters params = new SearchParameters();
        params.setTypeName("hive_process");
        params.setClassification("NotExisting");
        params.setLimit(10);

        SearchContext context = new SearchContext(params, typeRegistry, graph, Collections.<String>emptySet());
        new EntitySearchProcessor(context);
    }

}
