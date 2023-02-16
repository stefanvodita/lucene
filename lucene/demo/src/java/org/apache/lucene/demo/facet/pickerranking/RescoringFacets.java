package org.apache.lucene.demo.facet.pickerranking;

import org.apache.lucene.expressions.Bindings;
import org.apache.lucene.expressions.Expression;
import org.apache.lucene.expressions.js.JavascriptCompiler;
import org.apache.lucene.facet.FacetsConfig;
import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;
import org.apache.lucene.facet.taxonomy.FloatTaxonomyFacets;
import org.apache.lucene.facet.taxonomy.TaxonomyReader;
import org.apache.lucene.search.DocIdSetIterator;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.util.BitSet;

/**
 * Facets implementation that "re-scores" provides taxonomy ordinals using the result of a {@link
 * DoubleValuesSource} that accepts ordinal values as inputs instead of documents.
 */
class RescoringFacets extends FloatTaxonomyFacets {

  RescoringFacets(
      TaxonomyReader taxoReader,
      FacetsConfig facetsConfig,
      BitSet ords,
      String rankingExpression,
      Bindings pickerBindings)
      throws Exception {
    super(
        FacetsConfig.DEFAULT_INDEX_FIELD_NAME,
        taxoReader,
        AssociationAggregationFunction.SUM,
        facetsConfig);
    rescore(ords, rankingExpression, pickerBindings);
  }

  private void rescore(BitSet ords, String rankingExpression, Bindings pickerBindings)
      throws Exception {
    Expression expression = JavascriptCompiler.compile(rankingExpression);
    DoubleValuesSource dvs = expression.getDoubleValuesSource(pickerBindings);
    // Values are global so we can pass a null context:
    DoubleValues expressionValues = dvs.getValues(null, null);
    int ord = -1;
    while (true) {
      ord = ords.nextSetBit(ord + 1);
      if (ord == DocIdSetIterator.NO_MORE_DOCS) {
        break;
      }
      boolean advanced = expressionValues.advanceExact(ord);
      assert advanced;
      double val = expressionValues.doubleValue();
      assert values[ord] == 0;
      values[ord] = (float) val;
    }
  }
}
