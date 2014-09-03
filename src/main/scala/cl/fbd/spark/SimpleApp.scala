/* SimpleApp.scala */

package cl.fbd.spark

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf


case class Valor (time : Long, valor : Double)
case class Lectura (nombre : String, idValor : Valor)

object GenaraIdValor {
  val randomId = new scala.util.Random
  val randomValor = new scala.util.Random
    
  def nextValor = {
    val valor = randomValor.nextDouble * 50.0 + 100.0
      
    Valor (new java.util.Date ().getTime(), valor)        
  }  
  
  def nextLectura () = {
    val id = Math.abs (randomId.nextInt % 10)

    Lectura ("tag_" + id, nextValor)
  }
  
  def generaSeq : Seq [Lectura] = {
    for (i <- 1 to 1000000) yield nextLectura
  }
}

/*
 * 
 */

object SimpleApp6 {
  def main (args: Array[String]) = {
    val conf = new SparkConf().setAppName("Prueba Spark-SQL lee archivo Parquet")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    
    val bigParquetFile = sqlContext.parquetFile ("/home/hadoop_data/big_file.parquet")
   
    // para probar filtrar sobre la columna
    /*
    bigParquetFile.
      map (row => IdValor (row.getInt (0), row.row.getDouble (1))).
      filter (_.valor > 145.0).
      saveAsParquetFile("/home/hadoop_data/big_file_filtrado.parquet")
     
      */
  }  
}



object SimpleApp5 {
  def main (args: Array[String]) = {
    val conf = new SparkConf().setAppName("Prueba Spark-SQL crea archivo Parquet")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    
    println ("genera la lista en memoria")
    val data = GenaraIdValor.generaSeq
      
    println ("guarda en disco")
    sc.parallelize(data).
     saveAsParquetFile("/home/hadoop_data/big_file.parquet")    
  }  
  
}

/*
object SimpleApp3 {
  def main (args: Array[String]) = {
    val dataFile = "/home/francois/proyectos/pruebas/crea-archivo-grande/big_file.csv"
      
    val conf = new SparkConf().setAppName("Prueba Spark-SQL crea archivo Parquet")
    val sc = new SparkContext(conf)
    
    val sqlContext = new org.apache.spark.sql.SQLContext (sc)
    
    // createSchemaRDD is used to implicitly convert an RDD to a SchemaRDD.
    import sqlContext.createSchemaRDD
    
    sc.textFile(dataFile, 2).
      map (sz => sz.split (";")).
      map (a => {
        val id = a (0).trim.toInt
        val valor = a (1).trim.toDouble
        
        IdValor (id, 0L, valor :: Nil)
      }).
     saveAsParquetFile("big_file.parquet")    
  }  
}


object SimpleApp2 {
  def main (args: Array[String]) = {
    val dataFile = "/home/francois/proyectos/pruebas/crea-archivo-grande/big_file.csv"
      
    val conf = new SparkConf().setAppName("Prueba archivo local 2 : filtro")
    val sc = new SparkContext(conf)
    
    val registro = sc.textFile(dataFile, 2).
      map (sz => sz.split (";")).
      filter (a => {
          val valor = a (1).toDouble
          
          valor > 120.0
      }).
      map (a => a (0) + ";" + a (1)).
      saveAsTextFile ("data_file_fitrado")      
    ()
  }  
}


object SimpleApp1 {
  def main (args: Array[String]) = {
    val dataFile = "/home/francois/proyectos/pruebas/spark/data_file_fitrado"
      
    val conf = new SparkConf().setAppName("Prueba archivo local sobre filtrado")
    val sc = new SparkContext(conf)
    
    val registro = sc.textFile(dataFile, 2).
      map (sz => sz.split (";")).groupBy (a => a (0)).map (
          g => g match {
            case (k, v) => {
              val min = v.map (_.apply (1).toDouble).min
              val max = v.map (_.apply (1).toDouble).max
              
              k.toString () + " -> " + min + "; " + max
            }
            
          }).
      saveAsTextFile ("data_file_filtrado_groupby_id")
      
    ()
  }  
}

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/home/swf/spark-1.0.2-bin-hadoop2/README.md" // Should be some file on your system
    val conf = new SparkConf().setAppName("Simple Application")
    val sc = new SparkContext(conf)
    val logData = sc.textFile(logFile, 2).cache()
    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}
*/

/*
TIEMPO_BAJO_MEDIA;
TIEMPO_SOBRE_MEDIA;
FLUJO_ACTUAL;
FLUJO_MEDIA_HORA;
FLUJO_DOS_HORAS;

5
FLUJO_CUATRO_HORAS;
FLUJO_MINIMO_MEDIA_HORA;
FLUJO_MINIMO_DOS_HORA;
FLUJO_MINIMO_CUATRO_HORA;
FLUJO_MAXIMO_MEDIA_HORA;

10
FLUJO_MAXIMO_DOS_HORA;
FLUJO_MAXIMO_CUATRO_HORA;
DESVIACION;
ID_TAG;
TIPO_EQUIPO;

15
EQUIPO;
PROCESO;
TIPO_TAG;
CORRELATIVO;
CORRELATIVO_PROMEDIO;

20
FLUJO_OCHO_HORAS;
FLUJO_DOCE_HORAS;
VALOR_PREDICTIVO;
FECHA_LECTURA;
DESVIACIONMEDIA;

25
DESVIACIONUNA;
DESVIACIONDOS 
*/


/*
 * para replicar la logica de PIG
 */ 

/*
object FueraPig {
  def main(args: Array[String]) {
    val lecturasFile = "/home/francois/proyectos/pruebas/spark/Muestra.txt" 
    val conf = new SparkConf().setAppName("Fuera Pig")
    val sc = new SparkContext(conf)
    
    val lecturas = sc.textFile(lecturasFile, 2).
      map (sz => sz.split (";")).groupBy (a => a (23)).map (g => g match {case (k, v) => k.toString () + " -> " + v.map (_.mkString (" [", ";", "] ")).mkString (";")}).
      saveAsTextFile ("lecturas_grouped_by_fecha")
    
  }
}
*/

