/**
 * Copyright (C) 2014-2018 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.core.common.predicate;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.core.common.Predicate;

import java.util.Arrays;
import java.util.List;


public class TextMatchPredicate extends Predicate {
  String _query;
  String _options;
  public TextMatchPredicate(String lhs, List<String> rhs) {
    super(lhs, Type.TEXT_MATCH, rhs);
    Preconditions.checkArgument(rhs.size() == 2);
    _query = rhs.get(0);
    _options = rhs.get(1);
  }

  @Override
  public String toString() {
    return "Predicate: type: " + getType() + ", left : " + getLhs() + ", right : " + Arrays.toString(new String[]{_query, _options}) + "\n";
  }
  
  public String getQuery(){
   return _query;
  }

  public String getQueryOptions(){
    return _options;
  }

}
