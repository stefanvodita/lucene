package org.apache.lucene.demo.facet.pickerranking;

import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;

/** Represents a parsed picker ranking expression component (field, function). */
record PickerRankingExpressionAggregationComponent(
    String matchsetFieldName, AggregationFunction aggregationFunction) {

  /** Generate the expected component string representation in a picker ranking expression. */
  String componentString() {
    return "msa_" + matchsetFieldName + "_" + aggregationFunction.name();
  }

  abstract static class AggregationFunction {
    abstract AssociationAggregationFunction getFunction();

    abstract String name();

    static final AggregationFunction MAX =
        new AggregationFunction() {
          @Override
          AssociationAggregationFunction getFunction() {
            return AssociationAggregationFunction.MAX;
          }

          @Override
          String name() {
            return "max";
          }
        };

    static final AggregationFunction SUM =
        new AggregationFunction() {
          @Override
          AssociationAggregationFunction getFunction() {
            return AssociationAggregationFunction.SUM;
          }

          @Override
          String name() {
            return "sum";
          }
        };
  }
}
