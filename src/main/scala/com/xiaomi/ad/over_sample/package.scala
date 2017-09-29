package com.xiaomi.ad.over_sample

import breeze.linalg.{DenseVector, Vector}

/**
  * Created by cailiming on 17-9-29.
  */
case class LabeledPoint(label: Double, features: Vector[Double])

case class distanceIndex(sampleRowId: Int, partitionId: Int, distanceVector: DenseVector[Double], neighborRowId: DenseVector[Int])
