/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.lucene.demo.facet.pickerranking;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import org.apache.lucene.document.BinaryDocValuesField;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.DoublePoint;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KeywordField;
import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.SimpleBindings;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetField;
import org.apache.lucene.facet.FacetResult;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsCollectorManager;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.LabelAndValue;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.facet.taxonomy.TaxonomyWriter;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyReader;
import org.apache.lucene.facet.taxonomy.directory.DirectoryTaxonomyWriter;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.ConjunctionUtils;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.FixedBitSet;
import org.apache.lucene.util.IOUtils;
import org.apache.lucene.util.NumericUtils;

public class PickerRanking {
  private static final SimpleBindings globalBindings = new SimpleBindings();
  private static final FacetsConfig facetsConfig = new FacetsConfig();

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.err.println(
          "Usage: PickerRanking /path/to/geonames.txt /path/to/taxo/index/dir /path/to/index/dir doc_limit(or -1 means index all lines)");
      System.exit(2);
    }

    String geonamesDataPath = args[0];
    String taxoPath = args[1];
    String indexPath = args[2];
    int docLimit = Integer.parseInt(args[3]);

    Path tp = Paths.get(taxoPath);
    Path path = Paths.get(indexPath);
    IOUtils.rm(tp);
    IOUtils.rm(path);
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
        indexDocs(tw, iw, reader, docLimit);
        System.out.printf(
            Locale.ROOT, "Indexing time: %d msec%n", (System.nanoTime() - t0) / 1_000_000);
      }
      System.err.println("Index files: " + Arrays.toString(dir.listAll()));

      try (DirectoryReader reader = DirectoryReader.open(dir);
          TaxonomyReader taxoReader = new DirectoryTaxonomyReader(taxoDir)) {

        // [1] Mimic parsing metadata config. This creates an entry in `globalBindings` for every
        // "field" defined in XBM/XQM. Some fields are physically in the index, while some are
        // virtual, but all of these definitions are for matchset-level fields. They provide a
        // single value for each document in the index.
        //
        // Available document fields:
        // * lat: document latitude (physically in the index)
        // * lon: document longitude (physically in the index)
        // * elevation: document elevation (physically in the index)
        // * population: document population (physically in the index)
        // * tourism_factor: country document tourism_factor (physically in the index)
        // * ONE: virtual function that always returns the value 1.0 (can be used for counting)
        // * distance_from_seattle: virtual function that returns the distance of the document
        //   from Seattle, WA
        // * is_westcoast_tz: virtual function that returns 1.0 if the document is located in the
        //   US westcoast timezone (i.e., America/Los_Angeles), and 0.0 otherwise
        setupGlobalBindings();

        // [2] Run some query and collect hits for faceting:
        IndexSearcher searcher = new IndexSearcher(reader);
        FacetsCollectorManager fcm = new FacetsCollectorManager();

        Query q = new MatchAllDocsQuery();
        FacetsCollector poiFc = searcher.search(q, fcm);

        // [3] Grab the picker ranking expressions from the request. Imagine these strings come in
        // through our binning sort- param. Here we have a different expression for binning on
        // country code and timezone. A string that begins with "msa_" indicates a "match set
        // aggregation." The syntax is `msa_<fieldname>_<aggregation_function>` where fieldname
        // is a typical fieldname (something defined in XBM/XQM, and simulated in `globalBindings`),
        // and aggregation_function is one of MAX/SUM, describing how to aggregate a single value
        // over all the hits for a particular faceting ordinal.
        //
        // Lucene expression syntax:
        // https://lucene.apache.org/core/8_0_0/expressions/org/apache/lucene/expressions/js/package-summary.html
        // Functions can be added:
        // https://lucene.apache.org/core/8_0_0/expressions/org/apache/lucene/expressions/js/JavascriptCompiler.html#compile-java.lang.String-
        String countryCodeRankingExpression2 = "tourism_factor * msa_population_sum";

        // [4] Imagine we now parse all "msa" components from the picker ranking expressions,
        // and we build a set of all the unique components. A component is simply (field, function):
        Set<PickerRankingExpressionAggregationComponent> pickerRankingComponents =
            getAllPickerRankingComponents();

        // [5] Create bindings specific to picker ranking. These bindings will produce a single
        // value for any given faceting ordinal. They're specific to the request. The bindings
        // will provide values for all the picker ranking components across all picker ranking
        // expressions in the request:
        SimpleBindings pickerRankingBindings = new SimpleBindings();
        for (PickerRankingExpressionAggregationComponent c : pickerRankingComponents) {
          // A MatchsetAggregatedVirtualField is like a virtual field for a faceting ordinal, where
          // the value is an aggregate over the match set. These can be re-used across different
          // picker ranking expressions. Note that the values are computed up-front and are
          // re-used if they're referenced in multiple picker ranking expressions:
          MatchsetAggregatedVirtualField pickerVirtualField =
              new MatchsetAggregatedVirtualField(taxoReader, facetsConfig, globalBindings, c, poiFc);
          pickerRankingBindings.add(
              c.componentString(), pickerVirtualField.getDoubleValuesSource());
        }

        // Add data from the taxonomy. This is where we get "tourism_factor".
        DoubleValuesSource ordinalData = new DoubleValuesSource() {
          final NumericDocValues ordinalData = ((DirectoryTaxonomyReader) taxoReader).getInternalIndexReader()
              .leaves().get(1).reader().getNumericDocValues("ordinal-data");
          final BinaryDocValues fullPath = ((DirectoryTaxonomyReader) taxoReader).getInternalIndexReader()
              .leaves().get(1).reader().getBinaryDocValues("$full_path$");

          @Override
          public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
            return new DoubleValues() {

              @Override
              public double doubleValue() throws IOException {
                return Double.longBitsToDouble(ordinalData.longValue());
              }

              @Override
              public boolean advanceExact(int ord) throws IOException {
                boolean ret = ordinalData.advanceExact(ord - 1) && fullPath.advanceExact(ord - 1);
                System.out.println(ord + " " + fullPath.binaryValue().utf8ToString()
                    + " "  + Double.longBitsToDouble(ordinalData.longValue()));
                return ret;
              }
            };
          }

          @Override
          public boolean needsScores() {
            return false;
          }

          @Override
          public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
            return null;
          }

          @Override
          public int hashCode() {
            return 0;
          }

          @Override
          public boolean equals(Object obj) {
            return false;
          }

          @Override
          public String toString() {
            return null;
          }

          @Override
          public boolean isCacheable(LeafReaderContext ctx) {
            return false;
          }
        };
        pickerRankingBindings.add("tourism_factor", ordinalData);

        // [6] Get all the unique ordinals present in the matchset (across all dimensions):
        BitSet ordinalsInMatchset = getOrdinalsForHits(taxoReader, poiFc);

        // [7] Facet oand print some of the individual components that went into the
        // picker ranking expression (simulating return fields):
        doSingleDimFaceting(
            pickerRankingBindings,
            countryCodeRankingExpression2,
            ordinalsInMatchset,
            "country_code",
            pickerRankingComponents,
            taxoReader);
      }
    }
  }

  /**
   * Simulate parsing XBM/XQM. Creates a binding definition for every typical field. Nothing to do
   * with picker ranking, just defining available document fields.
   */
  private static void setupGlobalBindings() throws Exception {
    // Physical fields in the index:
    globalBindings.add("lat", DoubleValuesSource.fromDoubleField("lat"));
    globalBindings.add("lon", DoubleValuesSource.fromDoubleField("lon"));
    globalBindings.add("elevation", DoubleValuesSource.fromLongField("elevation"));
    globalBindings.add("population", DoubleValuesSource.fromLongField("population"));

    // Virtual field that always returns the value 1.0:
    globalBindings.add("ONE", DoubleValuesSource.constant(1.0));

    // Distance virtual field that computes the distance from Seattle (in reality, this would
    // retrieve a qs-param location instead of hardcoding to Seattle):
    Expression distanceFromSeattle =
        JavascriptCompiler.compile(String.format("haversin(%f,%f,lat,lon)", 47.6062, 122.3321));
    globalBindings.add(
        "distance_from_seattle", distanceFromSeattle.getDoubleValuesSource(globalBindings));

    // Virtual field that produces the value 1.0 if the timezone is US West Coast, otherwise 0.0:
    globalBindings.add(
        "is_westcoast_tz",
        new StringCompareDoubleValuesSource("tz", "America/Los_Angeles", 1.0, 0.0));
  }

  /**
   * Simulate parsing all the picker ranking expression components across the entire request. I'm
   * not very good at writing regex's and doing string parsing, so I'm just hardcoding this. In
   * reality, we would parse the picker ranking expression strings to do this.
   */
  private static Set<PickerRankingExpressionAggregationComponent> getAllPickerRankingComponents() {
    Set<PickerRankingExpressionAggregationComponent> components = new HashSet<>();
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "ONE", PickerRankingExpressionAggregationComponent.AggregationFunction.SUM));
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "elevation", PickerRankingExpressionAggregationComponent.AggregationFunction.MAX));
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "population", PickerRankingExpressionAggregationComponent.AggregationFunction.SUM));
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "population", PickerRankingExpressionAggregationComponent.AggregationFunction.MAX));
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "distance_from_seattle",
            PickerRankingExpressionAggregationComponent.AggregationFunction.MAX));
    components.add(
        new PickerRankingExpressionAggregationComponent(
            "msa", "is_westcoast_tz",
            PickerRankingExpressionAggregationComponent.AggregationFunction.MAX));
    return components;
  }

  /**
   * Get all unique ordinals present in the match set, regardless of what dimension they rollup to.
   */
  private static BitSet getOrdinalsForHits(TaxonomyReader taxonomyReader, FacetsCollector fc)
      throws IOException {
    BitSet ords = new FixedBitSet(taxonomyReader.getSize());
    for (FacetsCollector.MatchingDocs hits : fc.getMatchingDocs()) {
      SortedNumericDocValues dv =
          DocValues.getSortedNumeric(hits.context.reader(), FacetsConfig.DEFAULT_INDEX_FIELD_NAME);
      DocIdSetIterator it =
          ConjunctionUtils.intersectIterators(Arrays.asList(hits.bits.iterator(), dv));
      while (it.nextDoc() != DocIdSetIterator.NO_MORE_DOCS) {
        for (int i = 0; i < dv.docValueCount(); i++) {
          ords.set((int) dv.nextValue());
        }
      }
    }

    return ords;
  }

  /** Facet on a single dimension, ranking the pickers by the specified expression. */
  private static void doSingleDimFaceting(
      Bindings pickerExpressionBindings,
      String pickerRankingExpression,
      BitSet ordinalsInMatchset,
      String dim,
      Set<PickerRankingExpressionAggregationComponent> allComponents,
      TaxonomyReader taxoReader)
      throws Exception {
    System.out.println("Facet " + dim + " by: " + pickerRankingExpression);
    RescoringFacets facets =
        new RescoringFacets(
            taxoReader,
            facetsConfig,
            ordinalsInMatchset,
            pickerRankingExpression,
            pickerExpressionBindings);
    FacetResult facetResult = facets.getTopChildren(10, dim);
    if (facetResult != null) {
      for (LabelAndValue e : facetResult.labelValues) {
        System.out.printf("%s: %.2f\n", e.label, e.value.floatValue());
        for (PickerRankingExpressionAggregationComponent c : allComponents) {
          String msaField = c.componentString();
          System.out.printf(
              "  -- %s: %.2f\n",
              msaField,
              getMSAReturnFieldForPicker(
                  pickerExpressionBindings, dim, e.label, msaField, taxoReader));
        }
      }
    }
    System.out.println();
  }

  /** Simulates getting a return field for a picker. */
  private static float getMSAReturnFieldForPicker(
      Bindings pickerExpressionBindings,
      String dim,
      String value,
      String msaField,
      TaxonomyReader taxonomyReader)
      throws Exception {
    // Get ordinal for value string:
    int ord = taxonomyReader.getOrdinal(dim, value);

    // Picker bindings are not segment specific:
    DoubleValues dv =
        pickerExpressionBindings.getDoubleValuesSource(msaField).getValues(null, null);
    boolean advanced = dv.advanceExact(ord);
    assert advanced;
    return (float) dv.doubleValue();
  }

  ////////////////////////////////////////
  // Indexing stuff
  ////////////////////////////////////////

  private static void indexDocs(
      TaxonomyWriter tw, IndexWriter iw, LineNumberReader reader, int docLimit) throws Exception {
    Map<String, Double> ordinalDataPerCountryCode = new HashMap<>();
    ordinalDataPerCountryCode.put("AD", 0.3);
    ordinalDataPerCountryCode.put("AE", 0.6);
    ordinalDataPerCountryCode.put("AF", 0.9);
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
        TextField type = new TextField("type", "point_of_interest", Field.Store.NO);
        doc.add(type);
        addDoubleField(doc, "lat", values[4]);
        addDoubleField(doc, "lon", values[5]);
        addFacetField(doc, "country_code", values[8]);
        addLongField(doc, "population", values[14]);
        addLongField(doc, "elevation", values[15]);
        KeywordField tz = new KeywordField("tz", values[17], Field.Store.NO);
        doc.add(tz);
//        addFacetField(doc, "tz", values[17]);
        iw.addDocument(facetsConfig.build(tw, doc, String.valueOf(ordinalDataPerCountryCode.get(values[8]))));
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

  /*
   * term counts (name field):
   * "la" -> 181425
   * "de" -> 171095
   * "saint" -> 62831
   * "canyon" -> 27503
   *
   * "hotel" -> 64349
   * "del" -> 37469
   * "les" -> 13111
   * "plaza" -> 10605
   *
   * "channel" -> 4186
   * "centre" -> 4615
   * "st" -> 6616
   * "imperial" -> 663
   */
}
