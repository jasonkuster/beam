/*
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

import common_job_properties

// Defines a job.
mavenJob('beam_PostCommit_Java_RunnableOnService_Dataflow') {
  description('Runs the RunnableOnService suite on the Dataflow runner.')

  // Set common parameters.
  common_job_properties.setTopLevelJobProperties(delegate)

  // Set maven paramaters.
  common_job_properties.setMavenConfig(delegate)

  // Set build triggers
  triggers {
    cron('0 */6 * * *')
    scm('* * * * *')
  }
  
  goals('-B -e clean verify -am -pl runners/google-cloud-dataflow-java -DforkCount=0 -DrunnableOnServicePipelineOptions=\'[ "--runner=org.apache.beam.runners.dataflow.testing.TestDataflowRunner", "--project=apache-beam-testing", "--tempRoot=gs://temp-storage-for-runnable-on-service-tests/" ]\'')

  publishers {
    // Notify the mailing list for each failed build.
    // mailer('commits@beam.incubator.apache.org', false, false)
  }
}

