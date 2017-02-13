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

package org.apache.hadoop.hive.llap.cli;

import java.io.File;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.yarn.api.records.ApplicationReport;
import org.apache.hadoop.yarn.exceptions.YarnException;
import org.apache.hadoop.yarn.util.Clock;
import org.apache.hadoop.yarn.util.SystemClock;
import org.apache.slider.api.types.ApplicationDiagnostics;
import org.apache.slider.client.SliderClient;
import org.apache.slider.common.params.ActionCreateArgs;
import org.apache.slider.common.params.ActionDestroyArgs;
import org.apache.slider.common.params.ActionFreezeArgs;
import org.apache.slider.common.params.ActionInstallPackageArgs;
import org.apache.slider.common.tools.SliderUtils;
import org.apache.slider.core.exceptions.UnknownApplicationInstanceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.io.Files;

public class LlapSliderUtils {
  private static final String SLIDER_GZ = "slider-agent.tar.gz";
  private static final Logger LOG = LoggerFactory.getLogger(LlapSliderUtils.class);

  public static SliderClient createSliderClient(
      Configuration conf) throws Exception {
    SliderClient sliderClient = new SliderClient() {
      @Override
      public void serviceInit(Configuration conf) throws Exception {
        super.serviceInit(conf);
        initHadoopBinding();
      }
    };
    Configuration sliderClientConf = new Configuration(conf);
    sliderClientConf = sliderClient.bindArgs(sliderClientConf,
      new String[]{"help"});
    sliderClient.init(sliderClientConf);
    sliderClient.start();
    return sliderClient;
  }

  public static ApplicationReport getAppReport(String appName, SliderClient sliderClient,
                                               long timeoutMs) throws
      LlapStatusServiceDriver.LlapStatusCliException {
    Clock clock = new SystemClock();
    long startTime = clock.getTime();
    long timeoutTime = timeoutMs < 0 ? Long.MAX_VALUE : (startTime + timeoutMs);
    ApplicationReport appReport = null;

    while (appReport == null) {
      try {
        appReport = sliderClient.getYarnAppListClient().findInstance(appName);
        if (timeoutMs == 0) {
          // break immediately if timeout is 0
          break;
        }
        // Otherwise sleep, and try again.
        if (appReport == null) {
          long remainingTime = Math.min(timeoutTime - clock.getTime(), 500l);
          if (remainingTime > 0) {
            Thread.sleep(remainingTime);
          } else {
            break;
          }
        }
      } catch (Exception e) { // No point separating IOException vs YarnException vs others
        throw new LlapStatusServiceDriver.LlapStatusCliException(
            LlapStatusServiceDriver.ExitCode.YARN_ERROR,
            "Failed to get Yarn AppReport", e);
      }
    }
    return appReport;
  }

  public static ApplicationDiagnostics getApplicationDiagnosticsFromYarnDiagnostics(
      ApplicationReport appReport, Logger LOG) {
    if (appReport == null) {
      return null;
    }
    String diagnostics = appReport.getDiagnostics();
    if (diagnostics == null || diagnostics.isEmpty()) {
      return null;
    }
    try {
      ApplicationDiagnostics appDiagnostics =
          ApplicationDiagnostics.fromJson(diagnostics);
      return appDiagnostics;
    } catch (IOException e) {
      LOG.warn(
          "Failed to parse application diagnostics from Yarn Diagnostics - {}",
          diagnostics);
      return null;
    }
  }
}
