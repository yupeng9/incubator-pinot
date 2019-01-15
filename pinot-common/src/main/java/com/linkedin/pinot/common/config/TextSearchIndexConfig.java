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
package com.linkedin.pinot.common.config;
import java.io.File;
import org.codehaus.jackson.annotate.JsonIgnoreProperties;


@JsonIgnoreProperties(ignoreUnknown = true)
public class TextSearchIndexConfig {

  @ConfigKey("type")
  @ConfigDoc("Type of indexer to use")
  private String type;

  @ConfigKey("store")
  @ConfigDoc("Whether column should be stored. If false, it will just be indexed")
  private boolean store;

  public TextSearchIndexConfig(String type, boolean store) {
    this.type = type;
    this.store = store;
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public boolean shouldStore() {
    return store;
  }

  public void setStore(boolean store) {
    this.store = store;
  }

  public static TextSearchIndexConfig getDefaultConfig() {
    return new TextSearchIndexConfig("LUCENE", true);
  }
}
