# scala

## date UDF functions :

1. Retrieve year, month and day from current day : 

```scala
def generateYearMonthDay() :ArrayBuffer[String] =
{
  var YMD = ArrayBuffer[String]()
  val currentDate = "20200116"
  val dtf = java.time.format.DateTimeFormatter.ofPattern("yyyyMMdd")
  val date = java.time.LocalDate.parse(currentDate,dtf)
  YMD += date.getYear().toString
  YMD += date.getMonthValue.toString
  YMD += date.getDayOfMonth().toString
  return YMD
}
```
2. Retrieve current date and previous date :
```scala
import org.apache.spark.sql.catalyst.expressions.CurrentDate
import java.text.SimpleDateFormat
import java.util.Calendar

val sdf = new SimpleDateFormat("yyyy-MM-dd 00:00:00")
val date2 = sdf.format(Calendar.getInstance().getTime)

val cal = Calendar.getInstance
cal.setTime(sdf.parse(date2))
cal.add(Calendar.DAY_OF_YEAR, -1)
val previousDate = cal.getTime
val date1 = sdf.format(previousDate)

print(date1,date2)
```
Output -> `(2020-01-15 00:00:00,2020-01-16 00:00:00)`

3. Retrieve partition range :
```scala
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Date

def generateCondition(date: LocalDate): String = {
    val condition = new StringBuilder
    condition.
      append("(").
      append("year=").
      append(date.getYear).
      append(" ").append("and").append(" ").
      append("month=").
      append(date.getMonthValue).
      append(" ").append("and").append(" ").
      append("day=").
      append(date.getDayOfMonth).
      append(")").toString()
}

val currentLocalDate = LocalDate.parse(new SimpleDateFormat("yyyy-MM-dd").format(new Date()), DateTimeFormatter.ofPattern("yyyy-MM-dd"))
var startDate = currentLocalDate.minusDays(5)
var endDate = currentLocalDate.plusDays(5)

var dayRangeList = new scala.collection.mutable.ListBuffer[LocalDate]()
while (!startDate.isAfter(endDate)) {
    dayRangeList += startDate
    startDate=startDate.plusDays(1)
}
print(dayRangeList)

var intermediateRangeQuery = for (elem <- dayRangeList) yield {
    val dateRangeFilter = new StringBuilder
    dateRangeFilter.append(generateCondition(elem)).append(" ").append("or").append(" ").toString()
}
intermediateRangeQuery += " 1=2 "

val finalRangeQuery = new StringBuilder
finalRangeQuery.append(" ( ")
intermediateRangeQuery.foreach(elem => finalRangeQuery.append(elem))
finalRangeQuery.append(" ) ")
finalRangeQuery.toString()

print(finalRangeQuery)
```

Putput -> `( (year=2020 and month=1 and day=11) or (year=2020 and month=1 and day=12) or (year=2020 and month=1 and day=13) or (year=2020 and month=1 and day=14) or (year=2020 and month=1 and day=15) or (year=2020 and month=1 and day=16) or (year=2020 and month=1 and day=17) or (year=2020 and month=1 and day=18) or (year=2020 and month=1 and day=19) or (year=2020 and month=1 and day=20) or (year=2020 and month=1 and day=21) or  1=2  )`

