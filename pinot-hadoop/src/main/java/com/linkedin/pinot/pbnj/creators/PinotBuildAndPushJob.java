/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
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
package com.linkedin.pinot.pbnj.creators;

import azkaban.jobExecutor.AbstractJob;
import com.linkedin.pinot.hadoop.job.SegmentCreationJob;
import com.linkedin.pinot.hadoop.job.SegmentTarPushJob;
import java.util.Properties;
import org.apache.log4j.Logger;
import org.slf4j.LoggerFactory;


public class PinotBuildAndPushJob extends AbstractJob {
  private static SegmentCreationJob _segmentCreationJob;
  private static SegmentTarPushJob _segmentTarPushJob;
  private static Properties _properties;

  private static final org.slf4j.Logger LOGGER = LoggerFactory.getLogger(PinotBuildAndPushJob.class);

  @Override
  public void run() throws Exception {
    _segmentCreationJob.run();
    _segmentTarPushJob.run();
  }

  public PinotBuildAndPushJob(String name, azkaban.utils.Props jobProps) throws Exception {
    super(name, Logger.getLogger(name));

    _properties = jobProps.toProperties();
    LOGGER.info("properties passed in : {}", _properties);

    _segmentCreationJob = new SegmentCreationJob(name, _properties);
    _segmentTarPushJob = new SegmentTarPushJob(name, _properties);
  }
}
