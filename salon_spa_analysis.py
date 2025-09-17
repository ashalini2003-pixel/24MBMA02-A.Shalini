
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.appName("SalonSpaAnalysis").getOrCreate()

# Load data from table
df = spark.table("workspace.default.salon_spa_data")

# 1. Total Revenue per Salon and Spa
total_revenue_df = df.groupBy("Salon Name").agg(F.sum("Amount Spent").alias("Total_Revenue")).orderBy(F.desc("Total_Revenue"))
total_revenue_df.show()

# 2. Average Peak Booking Period
booking_trend_df = df.groupBy("Booking Date").count().withColumnRenamed("count", "Booking_Count").orderBy("Booking Date")
booking_trend_df.show()
average_peak_df = booking_trend_df.agg(F.avg("Booking_Count").alias("Avg_Bookings_Per_Day"))
average_peak_df.show()

# 3. Average Services Booked per Customer
services_per_customer_df = df.groupBy("Customer ID").count().withColumnRenamed("count", "Services_Booked")
average_services_df = services_per_customer_df.agg(F.avg("Services_Booked").alias("Avg_Services_Booked_Per_Customer"))
average_services_df.show()

# 4a. Average Rating per Service
avg_rating_service_df = df.groupBy("Service Name").agg(F.avg("Rating").alias("Avg_Rating")).orderBy(F.desc("Avg_Rating"))
avg_rating_service_df.show()

# 4b. Average Rating per Salon
avg_rating_salon_df = df.groupBy("Salon Name").agg(F.avg("Rating").alias("Avg_Rating")).orderBy(F.desc("Avg_Rating"))
avg_rating_salon_df.show()

# 4c. Average Rating per Staff
avg_rating_staff_df = df.groupBy("Staff Name").agg(F.avg("Rating").alias("Avg_Rating")).orderBy(F.desc("Avg_Rating"))
avg_rating_staff_df.show()
