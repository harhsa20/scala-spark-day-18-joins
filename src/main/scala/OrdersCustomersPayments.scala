import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object OrdersCustomersPayments {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .appName("Day 18 - Orders Customers Payments Joins")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    // 1. Customers
    val customersDF = Seq(
      ("C101", "Harsha", "Hyderabad"),
      ("C102", "Ravi", "Chennai"),
      ("C103", "Priya", "Bengaluru"),
      ("C104", "Anil", "Vijayawada")
    ).toDF("customer_id", "customer_name", "city")

    // 2. Orders: C104 has no order; C105 is an unknown customer
    val ordersDF = Seq(
      ("O101", "C101", 2500.0),
      ("O102", "C102", 1800.0),
      ("O103", "C101", 3200.0),
      ("O104", "C103", 4500.0),
      ("O105", "C105", 1200.0)
    ).toDF("order_id", "customer_id", "order_amount")

    // 3. Payments: O104 has no payment; O106 has no matching order
    val paymentsDF = Seq(
      ("P101", "O101", 2500.0, "UPI"),
      ("P102", "O102", 1800.0, "Card"),
      ("P103", "O103", 3200.0, "UPI"),
      ("P104", "O106", 900.0, "Cash")
    ).toDF("payment_id", "order_id", "payment_amount", "payment_method")

    println("\n========== CUSTOMERS ==========")
    customersDF.show(false)

    println("\n========== ORDERS ==========")
    ordersDF.show(false)

    println("\n========== PAYMENTS ==========")
    paymentsDF.show(false)

    // 4. INNER JOIN: matching customers and orders only
    println("\n========== INNER JOIN ==========")

    customersDF.as("c")
      .join(
        ordersDF.as("o"),
        col("c.customer_id") === col("o.customer_id"),
        "inner"
      )
      .select(
        col("o.order_id"),
        col("c.customer_id"),
        col("c.customer_name"),
        col("c.city"),
        col("o.order_amount")
      )
      .show(false)

    // 5. LEFT JOIN: all customers, including those without orders
    println("\n========== LEFT JOIN ==========")

    val customerOrders = customersDF.as("c")
      .join(
        ordersDF.as("o"),
        col("c.customer_id") === col("o.customer_id"),
        "left"
      )
      .select(
        col("c.customer_id"),
        col("c.customer_name"),
        col("o.order_id"),
        col("o.order_amount")
      )

    customerOrders.show(false)

    // 6. NULL HANDLING after LEFT JOIN
    println("\n========== NULL HANDLING ==========")

    customerOrders
      .withColumn(
        "order_amount",
        coalesce(col("order_amount"), lit(0.0))
      )
      .withColumn(
        "order_status",
        when(col("order_id").isNull, lit("No Order"))
          .otherwise(lit("Ordered"))
      )
      .show(false)

    // 7. RIGHT JOIN: all orders, even if customer details are missing
    println("\n========== RIGHT JOIN ==========")

    customersDF.as("c")
      .join(
        ordersDF.as("o"),
        col("c.customer_id") === col("o.customer_id"),
        "right"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("c.customer_name"),
        col("o.order_amount")
      )
      .show(false)

    // 8. FULL OUTER JOIN: all customers and all orders
    println("\n========== FULL OUTER JOIN ==========")

    customersDF.as("c")
      .join(
        ordersDF.as("o"),
        col("c.customer_id") === col("o.customer_id"),
        "full_outer"
      )
      .select(
        col("c.customer_id").as("matched_customer_id"),
        col("c.customer_name"),
        col("o.order_id"),
        col("o.customer_id").as("order_customer_id"),
        col("o.order_amount")
      )
      .show(false)

    // 9. Join orders and payments
    println("\n========== ORDERS WITH PAYMENT DETAILS ==========")

    val orderPayments = ordersDF.as("o")
      .join(
        paymentsDF.as("p"),
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("o.order_id"),
        col("o.customer_id"),
        col("o.order_amount"),
        col("p.payment_id"),
        col("p.payment_amount"),
        col("p.payment_method")
      )

    orderPayments.show(false)

    // 10. Join all three datasets
    println("\n========== COMPLETE ORDER REPORT ==========")

    val completeReport = customersDF.as("c")
      .join(
        ordersDF.as("o"),
        col("c.customer_id") === col("o.customer_id"),
        "full_outer"
      )
      .join(
        paymentsDF.as("p"),
        col("o.order_id") === col("p.order_id"),
        "left"
      )
      .select(
        col("c.customer_name"),
        col("c.city"),
        col("o.order_id"),
        col("o.order_amount"),
        col("p.payment_amount"),
        col("p.payment_method")
      )

    completeReport.show(false)

    // 11. Shuffle Sort Merge Join: inspect Spark's physical plan
    println("\n========== JOIN EXECUTION PLAN ==========")
    completeReport.explain(true)

    spark.stop()
  }
}
