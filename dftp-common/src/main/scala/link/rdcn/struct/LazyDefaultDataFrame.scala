package link.rdcn.struct

import link.rdcn.util.{DataUtils, ResourceUtils}

case class LazyDefaultDataFrame(
                                 schema: StructType,
                                 getStream: () => ClosableIterator[Row], // Lazily provided stream
                                 dataFrameStatistics: DataFrameStatistics = DataFrameStatistics.empty()
                               ) extends DataFrame {

  // Cache for the stream
  @volatile private var _stream: Option[ClosableIterator[Row]] = None

  // This method loads the stream lazily only when it is needed
  private def stream: ClosableIterator[Row] = {
    _stream.getOrElse {
      val newStream = getStream()
      _stream = Some(newStream)
      newStream
    }
  }

  override def map(f: Row => Row): DataFrame = {
    val iter = ClosableIterator(stream.map(f(_)))(stream.close())
    DataUtils.getDataFrameByStream(iter)
  }

  override def filter(f: Row => Boolean): DataFrame = {
    val iter = ClosableIterator(stream.filter(f(_)))(stream.close())
    DataUtils.getDataFrameByStream(iter)
  }

  override def select(columns: String*): DataFrame = {
    val selectedSchema = schema.select(columns: _*)
    val selectedStream = stream.map { row =>
      val selectedValues = columns.map { colName =>
        val idx = schema.indexOf(colName).getOrElse {
          throw new IllegalArgumentException(s"列名 '$colName' 不存在")
        }
        row.get(idx)
      }
      Row.fromSeq(selectedValues)
    }
    LazyDefaultDataFrame(selectedSchema, () => ClosableIterator(selectedStream)(stream.close()))
  }

  override def limit(n: Int): DataFrame = {
    LazyDefaultDataFrame(schema, () => ClosableIterator(stream.take(n))(stream.close()))
  }

  override def foreach(f: Row => Unit): Unit = ResourceUtils.using(stream) { iter => iter.foreach(f(_)) }

  override def collect(): List[Row] = ResourceUtils.using(stream) {
    _.toList
  }

  override def mapIterator[T](f: ClosableIterator[Row] => T): T = f(stream)

  override def getDataFrameStatistic: DataFrameStatistics = dataFrameStatistics
}

object LazyDefaultDataFrame {
  // Factory methods to create a LazyDefaultDataFrame

  def apply(schema: StructType, getStream: () => Iterator[Row]): LazyDefaultDataFrame = {
    new LazyDefaultDataFrame(schema, () => ClosableIterator(getStream())())
  }

  def createDataFrame(schema: StructType, getStream: () => ClosableIterator[Row], dataFrameStatistics: DataFrameStatistics = DataFrameStatistics.empty()): LazyDefaultDataFrame = {
    new LazyDefaultDataFrame(schema, getStream, dataFrameStatistics)
  }
}
