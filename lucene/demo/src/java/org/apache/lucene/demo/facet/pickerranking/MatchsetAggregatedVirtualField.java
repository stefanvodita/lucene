package org.apache.lucene.demo.facet.pickerranking;

import java.io.IOException;
import java.util.Arrays;
import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.facet.FacetsCollector;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;
import org.apache.lucene.facet.taxonomy.TaxonomyFacetFloatAssociations;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;

/**
 * Exposes a {@link DoubleValuesSource} interface over taxonomy ordinals instead of documents. For
 * each taxonomy ordinal, the {@code DoubleValuesSource} provides a single value computed as an
 * aggregate over the matchset hits containing the ordinal.
 *
 * <p>Note that this computes values for all unique ordinals in a single index field. Because many
 * dimensions are likely indexed in one field (in this demo they all are), we are computing data
 * that can be reused for faceting on any dimension in the field.
 */
class MatchsetAggregatedVirtualField {
  // values indexed by ordinal (not particularly memory-efficient...)
  private final float[] values;

  MatchsetAggregatedVirtualField(
      TaxonomyReader taxoReader,
      FacetsConfig facetsConfig,
      Bindings bindings,
      PickerRankingExpressionAggregationComponent component,
      FacetsCollector fc)
      throws IOException {
    // Setup a DVS for getting a document value. This provides a single value for each hit in the
    // matchset. It could be a physically-indexed field, or it could be a "virtual field":
    DoubleValuesSource matchsetValues =
        bindings.getDoubleValuesSource(component.matchsetFieldName());

    // Aggregation function that "reduces" all matchset values for the same ordinal to one value:
    AssociationAggregationFunction aggregationFunction =
        component.aggregationFunction().getFunction();

    // Here we piggyback on `TaxonomyFacetFloatAssociations` to do our ordinal-level aggregation
    // over the matchset. The end result is that we create a single value per ordinal, where that
    // value is the result of applying our aggregation function over the underlying matchset value
    // source. We could roll our own implementation for this, but for prototyping purposes, it
    // seemed to make sense to just reuse this "under the hood":
    TaxonomyFacetFloatAssociations facets =
        new TaxonomyFacetFloatAssociations(
            taxoReader, facetsConfig, fc, aggregationFunction, matchsetValues);

    // Grab a reference to the raw ordinal values:
    values = facets.values;
  }

  /**
   * Return a {@link DoubleValuesSource} interface over the aggregated value data. While a typical
   * DVS provides a value for a document, this provides a value for a taxonomy ordinal.
   */
  DoubleValuesSource getDoubleValuesSource() {
    return new PickerDoubleValuesSource(values);
  }

  /**
   * Provides a DVS interface over pre-computed values, keyed by taxonomy ordinal instead of docID.
   * Context is completely ignored when pulling DVs. This data is global.
   */
  private static class PickerDoubleValuesSource extends DoubleValuesSource {
    private final float[] values;

    PickerDoubleValuesSource(float[] values) {
      this.values = values;
    }

    @Override
    public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
      // Note: we completely ignore context as the data is global.

      return new DoubleValues() {
        int ord = -1;

        @Override
        public double doubleValue() throws IOException {
          return values[ord];
        }

        @Override
        public boolean advanceExact(int ord) throws IOException {
          this.ord = ord;
          return true;
        }
      };
    }

    @Override
    public boolean needsScores() {
      return false;
    }

    @Override
    public DoubleValuesSource rewrite(IndexSearcher reader) throws IOException {
      return this;
    }

    @Override
    public int hashCode() {
      return Arrays.hashCode(values);
    }

    @Override
    public boolean equals(Object other) {
      if (other == null) {
        return false;
      }
      if (other == this) {
        return true;
      }
      if (other instanceof PickerDoubleValuesSource == false) {
        return false;
      }
      return Arrays.equals(values, ((PickerDoubleValuesSource) other).values);
    }

    @Override
    public String toString() {
      return "no";
    }

    @Override
    public boolean isCacheable(LeafReaderContext ctx) {
      return false;
    }
  }
}
