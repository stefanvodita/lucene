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
package org.apache.lucene.sandbox.facet.plain.histograms;

import java.io.IOException;
import org.apache.lucene.codecs.lucene90.Lucene90DocValuesFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.NumericDocValuesField;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.sandbox.facet.FacetFieldCollectorManager;
import org.apache.lucene.sandbox.facet.recorders.CountFacetRecorder;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestUtil;

public class TestHistogramCollectorManager extends LuceneTestCase {

  public void testSingleValuedNoSkipIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new NumericDocValuesField("f", 3));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("f", 4));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new NumericDocValuesField("f", 6));
    w.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    CountFacetRecorder recorder = new CountFacetRecorder();
    HistogramFacetCutter cutter = new HistogramFacetCutter("f", 4);
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);

    searcher.search(new MatchAllDocsQuery(), collectorManager);
    assertEquals(2, recorder.recordedOrds().toArray().length);
    assertEquals(1, recorder.getCount(0));
    assertEquals(2, recorder.getCount(1));

    recorder = new CountFacetRecorder();
    cutter = new HistogramFacetCutter("f", 4, 1);
    FacetFieldCollectorManager<CountFacetRecorder> finalCollectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), finalCollectorManager));

    reader.close();
    dir.close();
  }

  public void testMultiValuedNoSkipIndex() throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, newIndexWriterConfig());
    Document doc = new Document();
    doc.add(new SortedNumericDocValuesField("f", 3));
    doc.add(new SortedNumericDocValuesField("f", 8));
    w.addDocument(doc);
    doc = new Document();
    doc.add(new SortedNumericDocValuesField("f", 4));
    doc.add(new SortedNumericDocValuesField("f", 6));
    doc.add(new SortedNumericDocValuesField("f", 8));
    w.addDocument(doc);
    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    CountFacetRecorder recorder = new CountFacetRecorder();
    HistogramFacetCutter cutter = new HistogramFacetCutter("f", 4);
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);

    searcher.search(new MatchAllDocsQuery(), collectorManager);
    assertEquals(3, recorder.recordedOrds().toArray().length);
    assertEquals(1, recorder.getCount(0));
    assertEquals(1, recorder.getCount(1));
    assertEquals(2, recorder.getCount(2));

    recorder = new CountFacetRecorder();
    cutter = new HistogramFacetCutter("f", 4, 1);
    FacetFieldCollectorManager<CountFacetRecorder> finalCollectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), finalCollectorManager));

    reader.close();
    dir.close();
  }

  public void testSkipIndex() throws IOException {
    doTestSkipIndex(newIndexWriterConfig());
  }

  public void testSkipIndexWithSort() throws IOException {
    doTestSkipIndex(
        newIndexWriterConfig().setIndexSort(new Sort(new SortField("f", SortField.Type.LONG))));
  }

  public void testSkipIndexWithSortAndLowInterval() throws IOException {
    doTestSkipIndex(
        newIndexWriterConfig()
            .setIndexSort(new Sort(new SortField("f", SortField.Type.LONG)))
            .setCodec(TestUtil.alwaysDocValuesFormat(new Lucene90DocValuesFormat(3))));
  }

  private void doTestSkipIndex(IndexWriterConfig cfg) throws IOException {
    Directory dir = newDirectory();
    IndexWriter w = new IndexWriter(dir, cfg);
    long[] values = new long[] {3, 6, 0, 4, 6, 12, 8, 8, 7, 8, 0, 4, 3, 6, 11};
    for (long value : values) {
      Document doc = new Document();
      doc.add(NumericDocValuesField.indexedField("f", value));
      w.addDocument(doc);
    }

    DirectoryReader reader = DirectoryReader.open(w);
    w.close();
    IndexSearcher searcher = newSearcher(reader);

    CountFacetRecorder recorder = new CountFacetRecorder();
    HistogramFacetCutter cutter = new HistogramFacetCutter("f", 4);
    FacetFieldCollectorManager<CountFacetRecorder> collectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);

    searcher.search(new MatchAllDocsQuery(), collectorManager);
    LongIntHashMap expectedCounts = new LongIntHashMap();
    for (long value : values) {
      expectedCounts.addTo(Math.floorDiv(value, 4), 1);
    }
    assertEquals(expectedCounts.size(), recorder.recordedOrds().toArray().length);
    for (LongIntHashMap.LongIntCursor cursor : expectedCounts) {
      assertEquals(cursor.value, recorder.getCount((int) cursor.key));
    }

    recorder = new CountFacetRecorder();
    cutter = new HistogramFacetCutter("f", 4, 1);
    FacetFieldCollectorManager<CountFacetRecorder> finalCollectorManager =
        new FacetFieldCollectorManager<>(cutter, recorder);
    expectThrows(
        IllegalStateException.class,
        () -> searcher.search(new MatchAllDocsQuery(), finalCollectorManager));

    reader.close();
    dir.close();
  }
}
