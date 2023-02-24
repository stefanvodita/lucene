package org.apache.lucene.demo.facet.pickerranking;

import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.BinaryDocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

import java.io.IOException;

/**
 * This class maps DoubleValues keyed by ordinal to docids.
 * It uses a label that is indexed together with the facet field that produced the ordinals.
 */
public class NestedAggregationDoubleValuesSource extends DoubleValuesSource {
  DoubleValues values;
  TaxonomyReader taxoReader;
  String facetFieldName;
  String labelFieldName;

  public NestedAggregationDoubleValuesSource(DoubleValues values, TaxonomyReader taxoReader,
                                             String facetFieldName, String labelFieldName) {
    this.values = values;
    this.taxoReader = taxoReader;
    this.facetFieldName = facetFieldName;
    this.labelFieldName = labelFieldName;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    return new DoubleValues() {
      final BinaryDocValues labels = ctx.reader().getBinaryDocValues(labelFieldName);

      @Override
      public double doubleValue() throws IOException {
        return values.doubleValue();
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        boolean retcode = labels.advanceExact(doc);
        assert retcode;
        String label = labels.binaryValue().utf8ToString();
        int ord = taxoReader.getOrdinal(facetFieldName, label);
        return values.advanceExact(ord);
      }
    };
  }

  @Override
  public boolean needsScores() {
    return false;
  }

  @Override
  public DoubleValuesSource rewrite(IndexSearcher reader) {
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
}
