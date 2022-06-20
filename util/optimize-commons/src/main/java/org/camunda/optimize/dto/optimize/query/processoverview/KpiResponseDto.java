/*
 * Copyright Camunda Services GmbH and/or licensed to Camunda Services GmbH under one or more contributor license agreements.
 * Licensed under a proprietary license. See the License.txt file for more information.
 * You may not use this file except in compliance with the proprietary license.
 */
package org.camunda.optimize.dto.optimize.query.processoverview;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.experimental.FieldNameConstants;
import org.camunda.optimize.dto.optimize.query.report.single.ViewProperty;

@Data
@FieldNameConstants
@AllArgsConstructor
@NoArgsConstructor
public class KpiResponseDto {

  private String reportId;
  private String reportName;
  private String value;
  private String target;
  private Boolean isBelow;
  private KpiType type;
  private ViewProperty measure;
  private String unit;

}
