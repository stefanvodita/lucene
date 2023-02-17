package org.apache.lucene.facet;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetFloatAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.NumericUtils;

public class MultipleAggregationsPerfTestDriver {
  private static void indexDocs(
      TaxonomyWriter tw,
      IndexWriter iw,
      FacetsConfig facetsConfig,
      LineNumberReader reader,
      int docLimit)
      throws Exception {
    String line;
    while ((line = reader.readLine()) != null) {
      if (reader.getLineNumber() % 10000 == 0) {
        System.err.println("doc: " + reader.getLineNumber());
      }
      if (docLimit != -1 && reader.getLineNumber() == docLimit) {
        break;
      }
      String[] values = line.split("\t");
      try {
        Document doc = new Document();
        KeywordField id = new KeywordField("id", values[0], Field.Store.NO);
        doc.add(id);
        TextField name = new TextField("name", values[1], Field.Store.NO);
        doc.add(name);
        addDoubleField(doc, "lat", values[4]);
        addDoubleField(doc, "lon", values[5]);
        addFacetField(doc, "country_code", values[8]);
        addLongField(doc, "population", values[14]);
        addLongField(doc, "elevation", values[15]);
        KeywordField tz = new KeywordField("tz", values[17], Field.Store.NO);
        doc.add(tz);
        // addFacetField(doc, "tz", values[17]);
        iw.addDocument(facetsConfig.build(tw, doc));
      } catch (NumberFormatException e) {
        // continue
      }
    }
  }

  private static void addDoubleField(Document doc, String fieldName, String val) {
    try {
      double d = Double.parseDouble(val);
      DoublePoint p = new DoublePoint(fieldName, d);
      doc.add(p);
      NumericDocValuesField n =
          new NumericDocValuesField(fieldName, NumericUtils.doubleToSortableLong(d));
      doc.add(n);
    } catch (NumberFormatException e) {
      // nothing
    }
  }

  private static void addLongField(Document doc, String fieldName, String val) {
    try {
      long l = Long.parseLong(val);
      LongPoint p = new LongPoint(fieldName, l);
      doc.add(p);
      NumericDocValuesField n = new NumericDocValuesField(fieldName, l);
      doc.add(n);
    } catch (NumberFormatException e) {
      // nothing
    }
  }

  private static void addFacetField(Document doc, String dimName, String val) {
    if (val != null && "".equals(val) == false) {
      FacetField f = new FacetField(dimName, val);
      doc.add(f);
    }
  }

  private static double timeit(Runnable runnable) {
    long startTime = System.nanoTime();
    runnable.run();
    long endTime = System.nanoTime();

    return (endTime - startTime) / 1000000.D;
  }

  public static void main(String[] args) throws Exception {
    String geonamesDataPath = args[0];
    String taxoPath = args[1];
    String indexPath = args[2];
    int docLimit = Integer.parseInt(args[3]);
    int nAggs = Integer.parseInt(args[4]);

    Path tp = Paths.get(taxoPath);
    Path path = Paths.get(indexPath);
    // IOUtils.rm(tp);
    // IOUtils.rm(path);

    FacetsConfig facetsConfig = new FacetsConfig();

    try (FSDirectory dir = FSDirectory.open(path);
        FSDirectory taxoDir = FSDirectory.open(tp)) {
      System.err.println("Now run indexing");
      IndexWriterConfig config = new IndexWriterConfig();
      try (IndexWriter iw = new IndexWriter(dir, config);
          TaxonomyWriter tw = new DirectoryTaxonomyWriter(taxoDir);
          LineNumberReader reader =
              new LineNumberReader(
                  new InputStreamReader(Files.newInputStream(Paths.get(geonamesDataPath))))) {
        long t0 = System.nanoTime();
        indexDocs(tw, iw, facetsConfig, reader, docLimit);
        System.out.printf(
            Locale.ROOT, "Indexing time: %d msec%n", (System.nanoTime() - t0) / 1_000_000);
      }
      System.err.println("Index files: " + Arrays.toString(dir.listAll()));

      FacetsCollector facetsCollector;
      try (DirectoryReader indexReader = DirectoryReader.open(dir);
          TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {

        IndexSearcher indexSearcher = new IndexSearcher(indexReader);
        FacetsCollectorManager facetsCollectorManager = new FacetsCollectorManager();
        facetsCollector = indexSearcher.search(new MatchAllDocsQuery(), facetsCollectorManager);

         AssociationAggregationFunction aggregationFunction = new NoOpAssocAggF();
//        AssociationAggregationFunction aggregationFunction = AssociationAggregationFunction.MAX;
         DoubleValuesSource valuesSource = DoubleValuesSource.constant(0);
//        DoubleValuesSource valuesSource = DoubleValuesSource.fromDoubleField("lat");
        List<AssociationAggregationFunction> aggregationFunctions =
            Collections.nCopies(nAggs, aggregationFunction);
        List<DoubleValuesSource> valuesSources = Collections.nCopies(nAggs, valuesSource);

        System.err.println("Now run performance test");

        double tSequential =
            timeit(
                () -> {
                  try {
                    for (int i = 0; i < nAggs; i++) {
                      new TaxonomyFacetFloatAssociations(
                          FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
                          taxoReader,
                          facetsConfig,
                          facetsCollector,
                          aggregationFunction,
                          valuesSource);
                    }
                  } catch (IOException ignored) {
                  }
                });

        double tParallel =
            timeit(
                () -> {
                  try {
                    new TaxonomyFacetFloatAssociations(
                        FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
                        taxoReader,
                        facetsConfig,
                        facetsCollector,
                        aggregationFunctions,
                        valuesSources);
                  } catch (IOException ignore) {
                  }
                });

        System.out.println("tSeq = " + tSequential + " tPar = " + tParallel);
      }
    }
  }

  private static class NoOpAssocAggF extends AssociationAggregationFunction {
    @Override
    public int aggregate(int existingVal, int newVal) {
      return 0;
    }

    @Override
    public float aggregate(float existingVal, float newVal) {
      return 0;
    }
  }
}
