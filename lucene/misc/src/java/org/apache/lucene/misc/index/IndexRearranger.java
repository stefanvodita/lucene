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

package org.apache.lucene.misc.index;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.lucene.index.CodecReader;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.FilterCodecReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.util.BitSet;
import org.apache.lucene.util.Bits;
import org.apache.lucene.util.NamedThreadFactory;

/**
 * Copy and rearrange index according to document selectors, from input dir to output dir. Length of
 * documentSelectors determines how many segments there will be
 *
 * <p>TODO: another possible (faster) approach to do this is to manipulate FlushPolicy and
 * MergePolicy at indexing time to create small desired segments first and merge them accordingly
 * for details please see: https://markmail.org/message/lbtdntclpnocmfuf
 *
 * @lucene.experimental
 */
public class IndexRearranger {
  protected final Directory input, output;
  protected final IndexWriterConfig config;
  protected final List<DocumentSelector> documentSelectors;

  /**
   * Constructor
   *
   * @param input input dir
   * @param output output dir
   * @param config index writer config
   * @param documentSelectors specify what document is desired in the rearranged index segments,
   *     each selector correspond to one segment
   */
  public IndexRearranger(
      Directory input,
      Directory output,
      IndexWriterConfig config,
      List<DocumentSelector> documentSelectors) {
    this.input = input;
    this.output = output;
    this.config = config;
    this.documentSelectors = documentSelectors;
  }

  public void execute() throws Exception {
    execute(null, null);
  }

  public void execute(List<Directory> segmentDirs, List<IndexWriterConfig> iwcs) throws Exception {
    config.setMergePolicy(
        NoMergePolicy.INSTANCE); // do not merge since one addIndexes call create one segment
    try (IndexWriter writer = new IndexWriter(output, config);
        IndexReader reader = DirectoryReader.open(input)) {
      ExecutorService executor =
          Executors.newFixedThreadPool(
              Math.min(Runtime.getRuntime().availableProcessors(), documentSelectors.size()),
              new NamedThreadFactory("rearranger"));

      List<Future<Void>> futures = new ArrayList<>();
      for (DocumentSelector selector : documentSelectors) {
//      for (int i = 0; i < documentSelectors.size(); ++i) {
//        final DocumentSelector selector = documentSelectors.get(i);
//        final IndexWriterConfig iwc = iwcs.get(i);
//        final Directory directory = segmentDirs.get(i);

//        Callable<Void> addSegment = () -> {addOneSegment(writer, reader, selector); return null;};
//        futures.add(executor.submit(addSegment));
        addOneSegment(writer, reader, selector);
      }
//      for (Future<Void> future : futures) {
//        future.get();
//      }
      executor.shutdown();

//      writer.addIndexes(segmentDirs.toArray(new Directory[0]));

//      for (Directory dir : segmentDirs) {
//        dir.close();
//      }
    }

    List<SegmentCommitInfo> ordered = new ArrayList<>();
    try (IndexReader reader = DirectoryReader.open(output)) {
      for (DocumentSelector ds : documentSelectors) {
        int foundLeaf = -1;
        for (LeafReaderContext context : reader.leaves()) {
          SegmentReader sr = (SegmentReader) context.reader();
          int docFound = ds.getAllFilteredDocs(sr).nextSetBit(0);
          if (docFound != DocIdSetIterator.NO_MORE_DOCS) {
            if (foundLeaf != -1) {
              throw new IllegalStateException(
                  "Document selector "
                      + ds
                      + " has matched more than 1 segments. Matched segments order: "
                      + foundLeaf
                      + ", "
                      + context.ord);
            }
            foundLeaf = context.ord;
            ordered.add(sr.getSegmentInfo());
          }
        }
//        assert foundLeaf != -1;
      }
    }
    SegmentInfos sis = SegmentInfos.readLatestCommit(output);
    sis.clear();
    sis.addAll(ordered);
    sis.commit(output);
  }

  private static Path getTmpDirPath() {
    return Paths.get("/", "tmp", "rearrange_indexes", UUID.randomUUID().toString());
  }

  private static void addOneSegment(
          IndexWriter writer, IndexReader reader, DocumentSelector selector) throws IOException {
    CodecReader[] readers = new CodecReader[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      readers[context.ord] =
              new DocSelectorFilteredCodecReader((CodecReader) context.reader(), selector);
    }
    writer.addIndexes(readers);
//    writer.commit();

    DirectoryReader directoryReader = DirectoryReader.open(writer);
    int n = directoryReader.leaves().size();
    LeafReader segmentReader = directoryReader.leaves().get(n - 1).reader();
    applyDeletes(writer, segmentReader, selector);
//    segmentReader.close();
    directoryReader.close();
  }

  private static void addOneSegment(IndexReader reader, DocumentSelector selector,
                                         IndexWriterConfig config, Directory dir) throws IOException {
    IndexWriter segmentWriter = new IndexWriter(dir, config);
    CodecReader[] readers = new CodecReader[reader.leaves().size()];
    for (LeafReaderContext context : reader.leaves()) {
      readers[context.ord] =
          new DocSelectorFilteredCodecReader((CodecReader) context.reader(), selector);
    }
    segmentWriter.addIndexes(readers);
//    segmentWriter.commit();

    DirectoryReader directoryReader = DirectoryReader.open(segmentWriter);
    LeafReader segmentReader = directoryReader.leaves().get(0).reader();
    applyDeletes(segmentWriter, segmentReader, selector);
//    segmentReader.close();
    directoryReader.close();

    segmentWriter.close();
  }

  private static void applyDeletes(IndexWriter segmentWriter, LeafReader segmentReader,
                                   DocumentSelector selector) throws IOException {
    for (int i = 0; i < segmentReader.maxDoc(); ++i) {
      if (selector.isDeleted(segmentReader, i)) {
        if (segmentWriter.tryDeleteDocument(segmentReader, i) == -1) {
          System.out.println("tryDeleteDocument failed and there's no plan B");
        }
      }
    }
//    segmentWriter.commit();
  }

  private static class DocSelectorFilteredCodecReader extends FilterCodecReader {

    BitSet filteredLiveDocs;
    int numDocs;

    public DocSelectorFilteredCodecReader(CodecReader in, DocumentSelector selector)
        throws IOException {
      super(in);
      filteredLiveDocs = selector.getAllFilteredDocs(in);
      numDocs = filteredLiveDocs.cardinality();
    }

    @Override
    public int numDocs() {
      return numDocs;
    }

    @Override
    public Bits getLiveDocs() {
      return filteredLiveDocs;
    }

    @Override
    public CacheHelper getCoreCacheHelper() {
      return in.getCoreCacheHelper();
    }

    @Override
    public CacheHelper getReaderCacheHelper() {
      return null;
    }
  }

  /** Select document within a CodecReader */
  public interface DocumentSelector {
    BitSet getAllFilteredDocs(CodecReader reader) throws IOException;

    boolean isDeleted(LeafReader reader, int idx) throws IOException;
  }
}
