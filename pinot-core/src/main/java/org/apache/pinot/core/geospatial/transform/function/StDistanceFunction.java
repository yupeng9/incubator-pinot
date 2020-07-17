/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pinot.core.geospatial.transform.function;

import com.google.common.base.Preconditions;
import org.apache.pinot.core.common.DataSource;
import org.apache.pinot.core.geospatial.GeometryType;
import org.apache.pinot.core.geospatial.GeometryUtils;
import org.apache.pinot.core.geospatial.serde.GeometrySerializer;
import org.apache.pinot.core.operator.blocks.ProjectionBlock;
import org.apache.pinot.core.operator.transform.TransformResultMetadata;
import org.apache.pinot.core.operator.transform.function.BaseTransformFunction;
import org.apache.pinot.core.operator.transform.function.TransformFunction;
import org.apache.pinot.core.plan.DocIdSetPlanNode;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;

import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.lang.Math.atan2;
import static java.lang.Math.cos;
import static java.lang.Math.sin;
import static java.lang.Math.sqrt;
import static java.lang.Math.toRadians;


/**
 * Function that measures the distance between the two geo-spatial objects. For geometry type, returns the 2-dimensional
 * cartesian minimum distance (based on spatial ref) between two geometries in projected units. For geography, returns
 * the great-circle distance in meters between two SphericalGeography points. Note that g1, g2 shall have the same type.
 */
public class StDistanceFunction extends BaseTransformFunction {
  private static final float MIN_LATITUDE = -90;
  private static final float MAX_LATITUDE = 90;
  private static final float MIN_LONGITUDE = -180;
  private static final float MAX_LONGITUDE = 180;
  public static final String FUNCTION_NAME = "ST_Distance";
  private TransformFunction _firstArgument;
  private TransformFunction _secondArgument;
  private double[] _results;

  @Override
  public String getName() {
    return FUNCTION_NAME;
  }

  @Override
  public void init(List<TransformFunction> arguments, Map<String, DataSource> dataSourceMap) {
    Preconditions
        .checkArgument(arguments.size() == 2, "2 arguments are required for transform function: %s", getName());
    TransformFunction transformFunction = arguments.get(0);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "First argument must be single-valued for transform function: %s", getName());
    _firstArgument = transformFunction;
    transformFunction = arguments.get(1);
    Preconditions.checkArgument(transformFunction.getResultMetadata().isSingleValue(),
        "Second argument must be single-valued for transform function: %s", getName());
    _secondArgument = transformFunction;
  }

  @Override
  public TransformResultMetadata getResultMetadata() {
    return DOUBLE_SV_NO_DICTIONARY_METADATA;
  }

  @Override
  public double[] transformToDoubleValuesSV(ProjectionBlock projectionBlock) {
    if (_results == null) {
      _results = new double[DocIdSetPlanNode.MAX_DOC_PER_CALL];
    }
    byte[][] firstValues = _firstArgument.transformToBytesValuesSV(projectionBlock);
    byte[][] secondValues = _secondArgument.transformToBytesValuesSV(projectionBlock);
    for (int i = 0; i < projectionBlock.getNumDocs(); i++) {
      Geometry firstGeometry = GeometrySerializer.deserialize(firstValues[i]);
      Geometry secondGeometry = GeometrySerializer.deserialize(secondValues[i]);
      if (GeometryUtils.isGeography(firstGeometry) != GeometryUtils.isGeography(secondGeometry)) {
        throw new RuntimeException(
            String.format("The first and second arguments shall either all be geometry" + " or all geography"));
      }
      if (GeometryUtils.isGeography(firstGeometry)) {
        _results[i] = sphericalDistance(firstGeometry, secondGeometry);
      } else {
        _results[i] =
            firstGeometry.isEmpty() || secondGeometry.isEmpty() ? null : firstGeometry.distance(secondGeometry);
      }
    }
    return _results;
  }

  public static void checkLatitude(double latitude) {
    if (Double.isNaN(latitude) || Double.isInfinite(latitude) || latitude < MIN_LATITUDE || latitude > MAX_LATITUDE) {
      throw new RuntimeException("Latitude must be between -90 and 90");
    }
  }

  public static void checkLongitude(double longitude) {
    if (Double.isNaN(longitude) || Double.isInfinite(longitude) || longitude < MIN_LONGITUDE
        || longitude > MAX_LONGITUDE) {
      throw new RuntimeException("Longitude must be between -180 and 180");
    }
  }

  public static Double sphericalDistance(Geometry leftGeometry, Geometry rightGeometry) {
    if (leftGeometry.isEmpty() || rightGeometry.isEmpty()) {
      return null;
    }

    validateGeographyType("ST_Distance", leftGeometry, EnumSet.of(GeometryType.POINT));
    validateGeographyType("ST_Distance", rightGeometry, EnumSet.of(GeometryType.POINT));
    Point leftPoint = (Point) leftGeometry;
    Point rightPoint = (Point) rightGeometry;

    // greatCircleDistance returns distance in KM.
    return greatCircleDistance(leftPoint.getY(), leftPoint.getX(), rightPoint.getY(), rightPoint.getX()) * 1000;
  }

  /**
   * Calculate the distance between two points on Earth.
   * <p>
   * This assumes a spherical Earth, and uses the Vincenty formula. (https://en.wikipedia
   * .org/wiki/Great-circle_distance)
   */
  public static double greatCircleDistance(double latitude1, double longitude1, double latitude2, double longitude2) {
    checkLatitude(latitude1);
    checkLongitude(longitude1);
    checkLatitude(latitude2);
    checkLongitude(longitude2);

    double radianLatitude1 = toRadians(latitude1);
    double radianLatitude2 = toRadians(latitude2);

    double sin1 = sin(radianLatitude1);
    double cos1 = cos(radianLatitude1);
    double sin2 = sin(radianLatitude2);
    double cos2 = cos(radianLatitude2);

    double deltaLongitude = toRadians(longitude1) - toRadians(longitude2);
    double cosDeltaLongitude = cos(deltaLongitude);

    double t1 = cos2 * sin(deltaLongitude);
    double t2 = cos1 * sin2 - sin1 * cos2 * cosDeltaLongitude;
    double t3 = sin1 * sin2 + cos1 * cos2 * cosDeltaLongitude;
    return atan2(sqrt(t1 * t1 + t2 * t2), t3) * GeometryUtils.EARTH_RADIUS_KM;
  }

  public static void validateGeographyType(String function, Geometry geometry, Set<GeometryType> validTypes) {
    GeometryType type = GeometryType.valueOf(geometry.getGeometryType().toUpperCase());
    if (!validTypes.contains(type)) {
      throw new RuntimeException(String
          .format("When applied to Geography inputs, %s only supports %s. Input " + "type is: %s", function,
              GeometryUtils.OR_JOINER.join(validTypes), type));
    }
  }
}