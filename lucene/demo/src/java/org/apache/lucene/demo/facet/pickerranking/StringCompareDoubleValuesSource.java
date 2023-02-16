package org.apache.lucene.demo.facet.pickerranking;

import java.io.IOException;
import java.util.Objects;
import org.apache.lucene.index.DocValues;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.SortedSetDocValues;
import org.apache.lucene.search.DoubleValues;
import org.apache.lucene.search.DoubleValuesSource;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.BytesRef;

/**
 * Simulates a virtual function that returns posVal if the specified value is present in the
 * specified field, and negVal if not.
 */
class StringCompareDoubleValuesSource extends DoubleValuesSource {
  private final String field;
  private final String value;
  private final double posVal;
  private final double negVal;

  StringCompareDoubleValuesSource(String field, String value, double posVal, double negVal) {
    this.field = field;
    this.value = value;
    this.posVal = posVal;
    this.negVal = negVal;
  }

  @Override
  public DoubleValues getValues(LeafReaderContext ctx, DoubleValues scores) throws IOException {
    SortedSetDocValues dv = DocValues.getSortedSet(ctx.reader(), field);
    long valueOrd = dv.lookupTerm(new BytesRef(value));
    return new DoubleValues() {
      @Override
      public double doubleValue() throws IOException {
        for (int i = 0; i < dv.docValueCount(); i++) {
          if (valueOrd == dv.nextOrd()) {
            return posVal;
          }
        }
        return negVal;
      }

      @Override
      public boolean advanceExact(int doc) throws IOException {
        return dv.advanceExact(doc);
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
    return Objects.hash(field, value, posVal, negVal);
  }

  @Override
  public boolean equals(Object other) {
    if (other == null) {
      return false;
    }
    if (other == this) {
      return true;
    }
    if (other instanceof StringCompareDoubleValuesSource == false) {
      return false;
    }
    StringCompareDoubleValuesSource rhs = (StringCompareDoubleValuesSource) other;
    return Objects.equals(field, rhs.field)
        && Objects.equals(value, rhs.value)
        && posVal == rhs.posVal
        && negVal == rhs.negVal;
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
