package org.apache.lucene.demo.facet.pickerranking;

import org.apache.lucene.facet.taxonomy.AssociationAggregationFunction;

/** Represents a parsed ranking expression component (field, function). */
record PickerRankingExpressionAggregationComponent(
    String matchsetName, String matchsetFieldName, AggregationFunction aggregationFunction) {

  /** Generate the expected component string representation in a picker ranking expression. */
  String componentString() {
    return matchsetName + "_" + matchsetFieldName + "_" + aggregationFunction.name();
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

      static final AggregationFunction DOC2ORD =
          new AggregationFunction() {
            @Override
            AssociationAggregationFunction getFunction() {
              return new AssociationAggregationFunction() {
                @Override
                public int aggregate(int existingVal, int newVal) {
                  return 0;
                }

                @Override
                public float aggregate(float existingVal, float newVal) {
                  if (existingVal == newVal) {
                    return existingVal;
                  }
                  // The existing value could have been intentionally set to 0,
                  // in which case this is wrong and we should throw exception.
                  if (existingVal == 0) {
                    return newVal;
                  }
                  System.out.println("Keys are not unique - possible wrong dimension");
                  return 0;
                }
              };
            }

            @Override
            String name() {
                  return "d2o";
              }
          };
  }
}
