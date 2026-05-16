package org.apache.spark.sql

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.classic.ExpressionColumnNode

object Spark4ColumnCompat {
  def fromCatalystExpression(expression: Expression): Column =
    Column(ExpressionColumnNode(expression))
}
