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
2. 

