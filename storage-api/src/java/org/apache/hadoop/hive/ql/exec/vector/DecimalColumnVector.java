/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.vector;


import java.util.Arrays;

import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.common.type.HiveDecimal;

public class DecimalColumnVector extends ColumnVector {

  /**
   * A vector of HiveDecimalWritable objects.
   *
   * For high performance and easy access to this low-level structure,
   * the fields are public by design (as they are in other ColumnVector
   * types).
   */
  public HiveDecimalWritable[] vector;
  public short scale;
  public short precision;

  public DecimalColumnVector(int precision, int scale) {
    this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
  }

  public DecimalColumnVector(int size, int precision, int scale) {
    super(size);
    this.precision = (short) precision;
    this.scale = (short) scale;
    vector = new HiveDecimalWritable[size];
    for (int i = 0; i < size; i++) {
      vector[i] = new HiveDecimalWritable(0);  // Initially zero.
    }
  }

  // Fill the all the vector entries with provided value
  public void fill(HiveDecimal value) {
    isRepeating = true;
    isNull[0] = false;
    if (vector[0] == null) {
      vector[0] = new HiveDecimalWritable(value);
    } else {
      vector[0].set(value);
    }
  }

  @Override
  // Simplify vector by brute-force flattening noNulls and isRepeating
  // This can be used to reduce combinatorial explosion of code paths in VectorExpressions
  // with many arguments.
  public void flatten(boolean selectedInUse, int[] sel, int size) {
    flattenPush();
    if (isRepeating) {
      isRepeating = false;
      HiveDecimalWritable repeat = vector[0];
      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          vector[i].set(repeat);
        }
      } else {
        for (int i = 0; i < size; i++) {
          vector[i].set(repeat);
        }
      }
      flattenRepeatingNulls(selectedInUse, sel, size);
    }
    flattenNoNulls(selectedInUse, sel, size);
  }

  @Override
  public void setElement(int outElementNum, int inputElementNum, ColumnVector inputVector) {
    if (inputVector.isRepeating) {
      inputElementNum = 0;
    }
    if (inputVector.noNulls || !inputVector.isNull[inputElementNum]) {
      vector[outElementNum].set(
          ((DecimalColumnVector) inputVector).vector[inputElementNum],
          precision, scale);
      if (!vector[outElementNum].isSet()) {
        isNull[outElementNum] = true;
        noNulls = false;
      } else {
        isNull[outElementNum] = false;
      }
    } else {
      isNull[outElementNum] = true;
      noNulls = false;
    }
  }

  @Override
  public void stringifyValue(StringBuilder buffer, int row) {
    if (isRepeating) {
      row = 0;
    }
    if (noNulls || !isNull[row]) {
      buffer.append(vector[row].toString());
    } else {
      buffer.append("null");
    }
  }

  public void set(int elementNum, HiveDecimalWritable writeable) {
    vector[elementNum].set(writeable, precision, scale);
    if (!vector[elementNum].isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
    }
  }

  public void set(int elementNum, HiveDecimal hiveDec) {
    vector[elementNum].set(hiveDec, precision, scale);
    if (!vector[elementNum].isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
    }
  }

  public void setNullDataValue(int elementNum) {
    // E.g. For scale 2 the minimum is "0.01"
    vector[elementNum].setFromLongAndScale(1L, scale);
  }

  @Override
  public void ensureSize(int size, boolean preserveData) {
    super.ensureSize(size, preserveData);
    if (size <= vector.length) return; // We assume the existing vector is always valid.
    HiveDecimalWritable[] oldArray = vector;
    vector = new HiveDecimalWritable[size];
    int initPos = 0;
    if (preserveData) {
      // we copy all of the values to avoid creating more objects
      // TODO: it might be cheaper to always preserve data or reset existing objects
      initPos = oldArray.length;
      System.arraycopy(oldArray, 0, vector, 0 , oldArray.length);
    }
    for (int i = initPos; i < vector.length; ++i) {
      vector[i] = new HiveDecimalWritable(0);  // Initially zero.
    }
  }

  @Override
  public void shallowCopyTo(ColumnVector otherCv) {
    DecimalColumnVector other = (DecimalColumnVector)otherCv;
    super.shallowCopyTo(other);
    other.scale = scale;
    other.precision = precision;
    other.vector = vector;
  }

  // Copy the current object contents into the output. Only copy selected entries,
  // as indicated by selectedInUse and the sel array.
  public void copySelected(
      boolean selectedInUse, int[] sel, int size, ColumnVector outputColVector) {

    DecimalColumnVector output = (DecimalColumnVector) outputColVector;
    boolean[] outputIsNull = output.isNull;

    // We do not need to do a column reset since we are carefully changing the output.
    output.isRepeating = false;

    // Handle repeating case
    if (isRepeating) {
      if (noNulls || !isNull[0]) {
        outputIsNull[0] = false;
        output.set(0, vector[0]);
      } else {
        outputIsNull[0] = true;
        output.noNulls = false;
        output.vector[0].setFromLong(0);
      }
      output.isRepeating = true;
      return;
    }

    // Handle normal case

    if (noNulls) {
      if (selectedInUse) {

        // CONSIDER: For large n, fill n or all of isNull array and use the tighter ELSE loop.

        if (!outputColVector.noNulls) {
          for(int j = 0; j != size; j++) {
           final int i = sel[j];
           // Set isNull before call in case it changes it mind.
           outputIsNull[i] = false;
           output.set(i, vector[i]);
         }
        } else {
          for(int j = 0; j != size; j++) {
            final int i = sel[j];
            output.set(i, vector[i]);
          }
        }
      } else {
        if (!outputColVector.noNulls) {

          // Assume it is almost always a performance win to fill all of isNull so we can
          // safely reset noNulls.
          Arrays.fill(outputIsNull, false);
          outputColVector.noNulls = true;
        }
        for(int i = 0; i != size; i++) {
          output.set(i, vector[i]);
        }
      }
    } else /* there are nulls in our column */ {

      // Carefully handle NULLs...

      if (selectedInUse) {
        for (int j = 0; j < size; j++) {
          int i = sel[j];
          if (!isNull[i]) {
            output.isNull[i] = false;
            output.set(i, vector[i]);
          } else {
            output.isNull[i] = true;
            output.noNulls = false;
          }
        }
      } else {
        for (int i = 0; i < size; i++) {
          if (!isNull[i]) {
            output.isNull[i] = false;
            output.set(i, vector[i]);
          } else {
            output.isNull[i] = true;
            output.noNulls = false;
          }
        }
      }
    }
  }
}
