package gov.nasa.jpl.federated.query;

import org.eclipse.rdf4j.federated.FedXFactory;
import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.TupleQuery;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;


public class FederatedQueryNasaWikidata {

    public static void main(String[] args) throws Exception {

        Repository repository = FedXFactory.newFederation()
                .withSparqlEndpoint("https://query.wikidata.org/sparql")
                .withSparqlEndpoint("http://localhost:8080/rdf4j-server/repositories/nasa")
                .create();

        File file = new File("src/main/resources/fq_Q42889_vehicle_transitive_results.csv");
        String csvFile = file.getAbsolutePath();


        try (PrintWriter writer = new PrintWriter(csvFile)) {

        try (RepositoryConnection conn = repository.getConnection()) {

//          String query = query("wdt:P31","wd:Q634");
//          String query = query("wdt:P31","wd:Q844911");
//          String query = query("wdt:P31", "wd:Q3504248");

           /*veliche*/
           String query = query("wdt:P279*", "wd:Q42889");

//            String query = query("wdt:P279", "wd:Q48797819");
//            String query = query("wdt:P279", "wd:Q2252759");
//            String query = query("wdt:P279*", "wd:Q2098169");

            TupleQuery tq = conn.prepareTupleQuery(query);

            try (TupleQueryResult tqRes = tq.evaluate()) {

                int count = 0;

//              writer.write("Select all AIPs that hold instances (wdt:P31) of wd:Q3504248 in Wikidata, ");
//                writer.write("Select all AIPs about vehicles (wd:Q42889), ");
//                writer.write("\n");

                while (tqRes.hasNext()) {

                    BindingSet b = tqRes.next();
                    System.out.println(" result: "+ b.getValue("s"));
                    writer.write(b.getValue("s")+", ");
                    writer.write("\n");
                    count++;

                }

            System.out.println("Number of results: " + count);

            }

            writer.close();
        }

        }catch (IOException e) {

            e.printStackTrace();
        }

        repository.shutDown();

    }

    public static String query(String predicate, String object){

        String query =
                "PREFIX wd: <http://www.wikidata.org/entity/> "
                        + "PREFIX wdt: <http://www.wikidata.org/prop/direct/> "
                        + "PREFIX rdf:  <http://www.w3.org/1999/02/22-rdf-syntax-ns#>"
                        + "PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#> "
                        + "SELECT DISTINCT ?s ?o ?r ?type "
                        + "WHERE { "
                        + "?s a <http://www.oais.ip#OWLClass_2104e04d_3fd5_416c_b94c_ec9803a257ca>."
                        + "<http://www.oais.ip#OWLClass_2104e04d_3fd5_416c_b94c_ec9803a257ca> rdfs:label ?label."
                        + "?o <http://www.oais.ip#OWLObjectProperty_054a9cee_f7a9_44d8_9793_65458644ce93> ?s ."
                        + "?o <http://www.oais.ip#OWLObjectProperty_63fcd189_fc4f_4cb0_a486_a40c55e38aad> ?r."
                        + "?r "+ predicate +" "+object+"."
                        +"}";

        return query ;
    }
}