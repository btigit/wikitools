package lucene.hadoop.util;

/**
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

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.ja.tokenattributes.PartOfSpeechAttribute;
import org.apache.lucene.analysis.util.FilteringTokenFilter;
import org.apache.lucene.util.AttributeImpl;

/**
 * Tokenizes the input into n-grams of the given size(s).
 */
public final class UnknownWordsTokenFilter extends FilteringTokenFilter {

  private boolean knownOnly;
  
  /**
   * Creates NGramTokenFilter with given min and max n-grams.
   * @param input {@link TokenStream} holding the input to be tokenized
   * @param extractionFilters 
   * @param knownOnly 
   */
  public UnknownWordsTokenFilter(TokenStream input, boolean knownOnly) {
    super(true,input);
    this.knownOnly = knownOnly;
  }
  
  /**
   * Returns the next input Token whose term() is not a stop word.
   */
  @Override
  protected boolean accept() throws IOException {
    if(knownOnly){
      PartOfSpeechAttribute posAtt = getAttribute(PartOfSpeechAttribute.class);
      lucene.hadoop.util.PartOfSpeechAttributeImpl posAttClone = 
          new lucene.hadoop.util.PartOfSpeechAttributeImpl();
      ((AttributeImpl)posAtt).copyTo((AttributeImpl) posAttClone);
      if(posAttClone.isUnknown()) return false;
    }
    return true;
  }
}
