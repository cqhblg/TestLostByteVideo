import java.util

import scala.util.control.Breaks._
import org.bytedeco.javacpp.Loader
import org.bytedeco.javacv.{FFmpegFrameGrabber, FFmpegFrameGrabber1, FFmpegFrameRecorder}
import java.io.{File, FileInputStream, FileOutputStream, InputStream, OutputStream}
import java.nio.ByteBuffer
import java.nio.channels.{Channels, DatagramChannel}
import java.nio.channels.Pipe.{SinkChannel, SourceChannel}
import java.util.concurrent.{ExecutorService, Executors}

import org.bytedeco.ffmpeg.global.avcodec
import org.seekloud.theia.processor.stream.PipeStream

import scala.util.Random


/*
1.使用JavaIo流读出本地文件中的视频
2.开启线程将视频流写入管道（随机丢几个Byte的数据）
3.开启一个线程从管道中读取数据
4.解码后将视频保存到本地
 */
object TestLostPackage {
  val FilePath = "D:/ScalaWorkSpace/theia/processor/src/test/scala/trailer.mkv"
  val OutPath = "D:/ScalaWorkSpace/theia/processor/src/test/scala/testout.mp4"
  var audioChannels = 2 //todo 待议
  var frameRate = 30
  val bitRate = 2000000
  val lostrate = 2  // 丢包率为 lostrate%

  class ReadFromPipeThread(fis:SourceChannel) extends Runnable {
    override def run(): Unit ={

      println("start thread")
      //从管道中读取数据
      val buf = Channels.newInputStream(fis)

      println(s"buf = $buf")
      val grabber = new FFmpegFrameGrabber1(buf,Integer.MAX_VALUE - 8)
//      grabber.setFormat("mkv")
      println(s"grabber = $grabber")
      try {
        grabber.start()
      } catch {
        case e: Exception =>
          println(e)
          println(s"exception occured in grabber start")
      }
      println("grabber started")
      val ffLength = grabber.getLengthInFrames()
      println(s"length = $ffLength")
      val outputStream = new FileOutputStream(new File(OutPath))
      val recorder = new FFmpegFrameRecorder(outputStream,640,480,audioChannels)
      recorder.setFrameRate(frameRate)
      recorder.setVideoBitrate(bitRate)
      recorder.setVideoCodec(avcodec.AV_CODEC_ID_MPEG2VIDEO)
      recorder.setAudioCodec(avcodec.AV_CODEC_ID_MP2)
      recorder.setMaxBFrames(0)
      recorder.setFormat("mpegts")
      try {
        recorder.startUnsafe()
      } catch {
        case e: Exception =>
          println(s" recorder meet error when start:$e")
      }
      var i = 0
      while (i<ffLength){
        val frame = grabber.grab()
        if(frame != null){
          println(frame)
          recorder.record(frame)
        }
        i+=1
      }
    }
  }


  class FileInputThread(fis: FileInputStream,out:SinkChannel) extends Runnable {
    override def run(): Unit = {
      var l= 0
      while (l != -1){
//      println("read")
        var buf_tempRead = new Array[Byte](188 * 7)
        l = fis.read(buf_tempRead)

        //随机丢一些包
        val i = Random.nextInt(100)
        if(i>= 100 - lostrate){
          buf_tempRead = new Array[Byte](188 * 7)
        }
        println(s"l=$l")
        val buf = ByteBuffer.wrap(buf_tempRead)
        out.write(buf)
      }
      fis.close()
    }
  }

  def main(args: Array[String]): Unit = {

    val threadPool:ExecutorService = Executors.newFixedThreadPool(60)

    val pipe = new PipeStream
    val source = pipe.getSource
    val sink = pipe.getSink
    val out = Channels.newOutputStream(sink)
    val in = Channels.newInputStream(source)
    val fileInputStream = new FileInputStream(new File(FilePath))
    try {
      Thread.sleep(3000)
      //从文件流写入管道
      threadPool.execute(new FileInputThread(fileInputStream,sink))
      //从管道读取数据
      threadPool.execute(new ReadFromPipeThread(source))
    }finally {
      threadPool.shutdown()
    }
  }
}
