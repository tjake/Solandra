package solandra;

import static org.junit.Assert.assertEquals;

import java.net.MalformedURLException;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.impl.CommonsHttpSolrServer;
import org.apache.solr.client.solrj.response.QueryResponse;
import org.apache.solr.common.SolrDocument;
import org.apache.solr.common.SolrDocumentList;
import org.apache.solr.common.SolrInputDocument;
import org.junit.BeforeClass;
import org.junit.Test;

public class FacetTests extends SolandraTestRunner
{

    static String indexName = String.valueOf(System.nanoTime());

    // Set test schema
    static String schemaXml = "<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n"
            + "<schema name=\"wikipedia\" version=\"1.1\">\n"
            + "<types>\n"
            + "<fieldType name=\"tint\" class=\"solr.TrieIntField\" precisionStep=\"8\" omitNorms=\"true\" positionIncrementGap=\"0\"/>\n"
            + "<fieldType name=\"text\" class=\"solr.TextField\">\n"
            + "<analyzer><tokenizer class=\"solr.StandardTokenizerFactory\"/></analyzer>\n"
            + "</fieldType>\n"
            + "<fieldType name=\"string\" class=\"solr.StrField\"/>\n"
            + "<fieldType name=\"sint\" class=\"solr.SortableIntField\" omitNorms=\"true\"/>\n"
            + "</types>\n"
            + "<fields>\n"
            + "<field name=\"url\" type=\"string\" indexed=\"true\" stored=\"true\"/>\n"
            + "<field name=\"text\"  type=\"text\" indexed=\"true\"  stored=\"true\" termVectors=\"true\" termPositions=\"true\" termOffsets=\"true\"/>\n"
            + "<field name=\"title\" type=\"text\" indexed=\"true\"  stored=\"true\"/>\n"
            + "<field name=\"price\" type=\"tint\" indexed=\"true\"  stored=\"true\"/>\n"
            + "<dynamicField name=\"*_i\" stored=\"false\" type=\"sint\" multiValued=\"false\" indexed=\"true\"/>"
            + "</fields>\n" + "<uniqueKey>url</uniqueKey>\n" + "<defaultSearchField>title</defaultSearchField>\n"
            + "</schema>\n";

    @BeforeClass
    public static void init() throws Exception
    {
        addSchema(indexName, schemaXml);
        getSolrClient(indexName);
    }

    @Test
    public void testFacetCounts() throws Exception
    {
        CommonsHttpSolrServer client = getSolrClient(indexName);


        for (int i = 1; i < 64; i++)
        {
            SolrInputDocument doc = createDoc(i % 4 == 0);
            client.add(doc);
            client.commit(true, true);
            
            SolrQuery q = new SolrQuery().setQuery("text:bar");
            
            client.query(q);
            
            q = new SolrQuery().setQuery("*:*").addFacetField("title");

            QueryResponse r = client.query(q);
            
            SolrDocumentList resultList = r.getResults();

            assertEquals(i, resultList.getNumFound());

            assertEquals(i, r.getFacetField("title").getValues().get(0).getCount());
        }

    }

    private SolrInputDocument createDoc(boolean extra)
    {
        SolrInputDocument doc = new SolrInputDocument();

        doc.addField("url", "" + System.nanoTime());
        doc.addField("title", "foo");

        if(extra)
            doc.addField("text", "bar");

        return doc;
    }

}
