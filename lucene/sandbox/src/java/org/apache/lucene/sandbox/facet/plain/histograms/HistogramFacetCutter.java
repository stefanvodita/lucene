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
import java.util.Objects;
import org.apache.lucene.document.FieldType;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.DocValuesSkipper;
import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.SortedNumericDocValues;
import org.apache.lucene.internal.hppc.LongIntHashMap;
import org.apache.lucene.sandbox.facet.cutters.FacetCutter;
import org.apache.lucene.sandbox.facet.cutters.LeafFacetCutter;
import org.apache.lucene.sandbox.facet.iterators.OrdinalIterator;
import org.apache.lucene.search.CollectionTerminatedException;
import org.apache.lucene.search.CollectorManager;
import org.apache.lucene.search.LeafCollector;

/**
 * {@link CollectorManager} that computes a histogram of the distribution of the values of a field.
 *
 * <p>It takes an {@code bucketWidth} as a parameter and counts the number of documents that fall
 * into intervals [0, bucketWidth), [bucketWidth, 2*bucketWidth), etc. The keys of the returned
 * {@link LongIntHashMap} identify these intervals as the quotient of the integer division by {@code
 * bucketWidth}. Said otherwise, a key equal to {@code k} maps to values in the interval {@code [k *
 * bucketWidth, (k+1) * bucketWidth)}.
 *
 * <p>This implementation is optimized for the case when {@code field} is part of the index sort and
 * has a {@link FieldType#setDocValuesSkipIndexType skip index}.
 *
 * <p>Note: this collector is inspired from "YU, Muzhi, LIN, Zhaoxiang, SUN, Jinan, et al.
 * TencentCLS: the cloud log service with high query performances. Proceedings of the VLDB
 * Endowment, 2022, vol. 15, no 12, p. 3472-3482.", where the authors describe how they run
 * "histogram queries" by sorting the index by timestamp and pre-computing ranges of doc IDs for
 * every possible bucket.
 */
public final class HistogramFacetCutter implements FacetCutter {

  private static final int DEFAULT_MAX_BUCKETS = 1024;

  private final String field;
  private final long bucketWidth;
  private final int maxBuckets;

  /**
   * Compute a histogram of the distribution of the values of the given {@code field} according to
   * the given {@code bucketWidth}. This configures a maximum number of buckets equal to the default
   * of 1024.
   */
  public HistogramFacetCutter(String field, long bucketWidth) {
    this(field, bucketWidth, DEFAULT_MAX_BUCKETS);
  }

  /**
   * Expert constructor.
   *
   * @param maxBuckets Max allowed number of buckets. Note that this is checked at runtime and on a
   *     best-effort basis.
   */
  public HistogramFacetCutter(String field, long bucketWidth, int maxBuckets) {
    this.field = Objects.requireNonNull(field);
    if (bucketWidth < 2) {
      throw new IllegalArgumentException("bucketWidth must be at least 2, got: " + bucketWidth);
    }
    this.bucketWidth = bucketWidth;
    if (maxBuckets < 1) {
      throw new IllegalArgumentException("maxBuckets must be at least 1, got: " + maxBuckets);
    }
    this.maxBuckets = maxBuckets;
  }

  @Override
  public LeafFacetCutter createLeafCutter(LeafReaderContext context) throws IOException {
    FieldInfo fi = context.reader().getFieldInfos().fieldInfo(field);
    if (fi == null) {
      // The segment has no values, nothing to do.
      throw new CollectionTerminatedException();
    }
    if (fi.getDocValuesType() != DocValuesType.NUMERIC
        && fi.getDocValuesType() != DocValuesType.SORTED_NUMERIC) {
      throw new IllegalStateException(
          "Expected numeric field, but got doc-value type: " + fi.getDocValuesType());
    }
    SortedNumericDocValues values = DocValues.getSortedNumeric(context.reader(), field);
    NumericDocValues singleton = DocValues.unwrapSingleton(values);
    if (singleton == null) {
      return new HistogramNaiveLeafFacetCutter(values, bucketWidth, maxBuckets);
    } else {
      DocValuesSkipper skipper = context.reader().getDocValuesSkipper(field);
      if (skipper != null) {
        long leafMinBucket = Math.floorDiv(skipper.minValue(), bucketWidth);
        long leafMaxBucket = Math.floorDiv(skipper.maxValue(), bucketWidth);
        if (leafMaxBucket - leafMinBucket <= 1024) {
          // Only use the optimized implementation if there is a small number of unique buckets,
          // so that we can count them using a dense array instead of a hash table. This helps save
          // the overhead of hashing and collision resolution.
          return new HistogramLeafFacetCutter(singleton, skipper, bucketWidth, maxBuckets);
        }
      }
      return new HistogramNaiveSingleValuedLeafFacetCutter(singleton, bucketWidth, maxBuckets);
    }
  }

  /**
   * Naive implementation of a histogram {@link LeafCollector}, which iterates all maches and looks
   * up the value to determine the corresponding bucket.
   */
  private static class HistogramNaiveLeafFacetCutter implements LeafFacetCutter {

    private final SortedNumericDocValues values;
    private final long bucketWidth;
    private final int maxBuckets;

    HistogramNaiveLeafFacetCutter(SortedNumericDocValues values, long bucketWidth, int maxBuckets) {
      this.values = values;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;
    }

    private int valueCount;
    private int currentValueIdx = 0;
    private long prevBucket = Long.MIN_VALUE;

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        valueCount = values.docValueCount();
        currentValueIdx = 0;
        prevBucket = Long.MIN_VALUE;
        return true;
      }
      return false;
    }

    @Override
    public int nextOrd() throws IOException {
      int bucket;
      do {
        if (currentValueIdx++ == valueCount) {
          return OrdinalIterator.NO_MORE_ORDS;
        }

        final long value = values.nextValue();
        bucket = (int) Math.floorDiv(value, bucketWidth);
      } while (bucket == prevBucket);
      // We must not double-count values that map to the same bucket since this returns doc
      // counts as opposed to value counts.
      checkMaxBuckets(bucket, maxBuckets);
      prevBucket = bucket;
      return bucket;
    }
  }

  /**
   * Naive implementation of a histogram {@link LeafCollector}, which iterates all maches and looks
   * up the value to determine the corresponding bucket.
   */
  private static class HistogramNaiveSingleValuedLeafFacetCutter implements LeafFacetCutter {

    private final NumericDocValues values;
    private final long bucketWidth;
    private final int maxBuckets;
    private int bucket;
    private boolean isConsumed;

    HistogramNaiveSingleValuedLeafFacetCutter(
        NumericDocValues values, long bucketWidth, int maxBuckets) {
      this.values = values;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (values.advanceExact(doc)) {
        final long value = values.longValue();
        bucket = (int) Math.floorDiv(value, bucketWidth);
        isConsumed = false;
        checkMaxBuckets(bucket, maxBuckets);
        return true;
      }
      return false;
    }

    @Override
    public int nextOrd() {
      if (isConsumed) {
        return NO_MORE_ORDS;
      }
      isConsumed = true;
      return bucket;
    }
  }

  /**
   * Optimized histogram {@link LeafCollector}, that takes advantage of the doc-values index to
   * speed up collection.
   */
  private static class HistogramLeafFacetCutter implements LeafFacetCutter {

    private final NumericDocValues values;
    private final DocValuesSkipper skipper;
    private final long bucketWidth;
    private final int maxBuckets;
    private final int[] counts;
    private final long leafMinBucket;

    /** Max doc ID (inclusive) up to which all docs values may map to the same bucket. */
    private int upToInclusive = -1;

    /** Whether all docs up to {@link #upToInclusive} values map to the same bucket. */
    private boolean upToSameBucket;

    /** Index in {@link #counts} for docs up to {@link #upToInclusive}. */
    private int upToBucketIndex;

    private boolean isConsumed;
    private int bucket;

    HistogramLeafFacetCutter(
        NumericDocValues values, DocValuesSkipper skipper, long bucketWidth, int maxBuckets) {
      this.values = values;
      this.skipper = skipper;
      this.bucketWidth = bucketWidth;
      this.maxBuckets = maxBuckets;

      leafMinBucket = Math.floorDiv(skipper.minValue(), bucketWidth);
      long leafMaxBucket = Math.floorDiv(skipper.maxValue(), bucketWidth);
      counts = new int[Math.toIntExact(leafMaxBucket - leafMinBucket + 1)];
    }

    private void advanceSkipper(int doc) throws IOException {
      if (doc > skipper.maxDocID(0)) {
        skipper.advance(doc);
      }
      upToSameBucket = false;

      if (skipper.minDocID(0) > doc) {
        // Corner case which happens if `doc` doesn't have a value and is between two intervals of
        // the doc-value skip index.
        upToInclusive = skipper.minDocID(0) - 1;
        return;
      }

      upToInclusive = skipper.maxDocID(0);

      // Now find the highest level where all docs map to the same bucket.
      for (int level = 0; level < skipper.numLevels(); ++level) {
        int totalDocsAtLevel = skipper.maxDocID(level) - skipper.minDocID(level) + 1;
        long minBucket = Math.floorDiv(skipper.minValue(level), bucketWidth);
        long maxBucket = Math.floorDiv(skipper.maxValue(level), bucketWidth);

        if (skipper.docCount(level) == totalDocsAtLevel && minBucket == maxBucket) {
          // All docs at this level have a value, and all values map to the same bucket.
          upToInclusive = skipper.maxDocID(level);
          upToSameBucket = true;
          upToBucketIndex = (int) (minBucket - this.leafMinBucket);
        } else {
          break;
        }
      }
    }

    @Override
    public boolean advanceExact(int doc) throws IOException {
      if (doc > upToInclusive) {
        advanceSkipper(doc);
      }

      if (upToSameBucket) {
        bucket = upToBucketIndex;
        isConsumed = false;
        return true;
      } else if (values.advanceExact(doc)) {
        final long value = values.longValue();
        bucket = (int) Math.floorDiv(value, bucketWidth);
        checkMaxBuckets(bucket, maxBuckets);
        isConsumed = false;
        return true;
      }
      return false;
    }

    @Override
    public int nextOrd() {
      if (isConsumed) {
        return OrdinalIterator.NO_MORE_ORDS;
      }
      isConsumed = true;
      return bucket;
    }
  }

  private static void checkMaxBuckets(int bucket, int maxBuckets) {
    if (bucket >= maxBuckets) {
      throw new IllegalStateException(
          "Collected "
              + bucket
              + " buckets, which is more than the configured max number of buckets: "
              + maxBuckets);
    }
  }
}
