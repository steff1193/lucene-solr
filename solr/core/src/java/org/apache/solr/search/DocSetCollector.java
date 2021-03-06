package org.apache.solr.search;

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

import java.io.IOException;
import java.util.ArrayList;

import org.apache.lucene.index.AtomicReaderContext;
import org.apache.lucene.search.Collector;
import org.apache.lucene.search.Scorer;
import org.apache.lucene.util.OpenBitSet;

/**
 *
 */

public class DocSetCollector extends Collector {
  int pos=0;
  OpenBitSet bits;
  final int smallSetSize;
  final int maxDoc;
  int base;

  // in case there aren't that many hits, we may not want a very sparse
  // bit array.  Optimistically collect the first few docs in an array
  // in case there are only a few.
  final ExpandingIntArray scratch;

  public DocSetCollector(int smallSetSize, int maxDoc) {
    this.smallSetSize = smallSetSize;
    this.maxDoc = maxDoc;
    this.scratch = new ExpandingIntArray();
  }

  @Override
  public void collect(int doc) throws IOException {
    doc += base;
    // optimistically collect the first docs in an array
    // in case the total number will be small enough to represent
    // as a small set like SortedIntDocSet instead...
    // Storing in this array will be quicker to convert
    // than scanning through a potentially huge bit vector.
    // FUTURE: when search methods all start returning docs in order, maybe
    // we could have a ListDocSet() and use the collected array directly.
    if (pos < smallSetSize) {
      scratch.add(pos, doc);
    } else {
      // this conditional could be removed if BitSet was preallocated, but that
      // would take up more memory, and add more GC time...
      if (bits==null) bits = new OpenBitSet(maxDoc);
      bits.fastSet(doc);
    }

    pos++;
  }

  public DocSet getDocSet() {
    if (pos <= scratch.size()) {
      // assumes docs were collected in sorted order!
      return new SortedIntDocSet(scratch.toArray(), pos);
    } else {
      // set the bits for ids that were collected in the array
      scratch.copyTo(bits);
      return new BitDocSet(bits,pos);
    }
  }

  @Override
  public void setScorer(Scorer scorer) throws IOException {
  }

  @Override
  public void setNextReader(AtomicReaderContext context) throws IOException {
    this.base = context.docBase;
  }

  @Override
  public boolean acceptsDocsOutOfOrder() {
    return false;
  }
  
  protected static class ExpandingIntArray {
    
    private ArrayList<int[]> arrays = null;
    private int[] currentAddArray = null;
    private int indexForNextAddInCurrentAddArray = -1;
    private int size = 0;
    
    public void add(int index, int value) {
      if (index != size) throw new IllegalArgumentException("Appending only suppported");
      
      if (arrays == null) arrays = new ArrayList<int[]>(10);
      if (currentAddArray == null) {
        currentAddArray = new int[10];
        arrays.add(currentAddArray);
        indexForNextAddInCurrentAddArray = 0;
      }
      if (indexForNextAddInCurrentAddArray >= currentAddArray.length) {
        currentAddArray = new int[currentAddArray.length*2];
        arrays.add(currentAddArray);
        indexForNextAddInCurrentAddArray = 0;
      }
      currentAddArray[indexForNextAddInCurrentAddArray++] = value;
      size++;
    }
    
    public void copyTo(OpenBitSet bits) {
      if (size > 0) {
        int resultPos = 0;
        for (int i = 0; i < arrays.size(); i++) {
          int[] srcArray = arrays.get(i);
          int intsToCopy = (i < (arrays.size()-1))?srcArray.length:indexForNextAddInCurrentAddArray;
          for (int j = 0; j < intsToCopy; j++) {
            bits.fastSet(srcArray[j]);
          }
          resultPos += intsToCopy;
        }
        assert resultPos == size;
      }
    }
    
    public int[] toArray() {
      int[] result = new int[size];
      if (size > 0) {
        int resultPos = 0;
        for (int i = 0; i < arrays.size(); i++) {
          int[] srcArray = arrays.get(i);
          int intsToCopy = (i < (arrays.size()-1))?srcArray.length:indexForNextAddInCurrentAddArray;
          System.arraycopy(srcArray, 0, result, resultPos, intsToCopy);
          resultPos += intsToCopy;
        }
        assert resultPos == size;
      }
      return result;
    }
    
    public int size() {
      return size;
    }
  }
  
}
