package maven.integrate;
import java.io.*;
import java.util.*;
import java.text.SimpleDateFormat;
import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.Partitioner;
import kafka.producer.ProducerConfig;
import kafka.serializer.StringEncoder;
import java.util.regex.*;


public class Agent{
  public static void main(String[] args )
  {
    Properties props = new Properties();
    /*-------------------------------------------------------------------
    //根据这个配置获取metadata,不必是kafka集群上的所有broker,可以填写多个broker
    //props.put("metadata.broker.list", "192.168.197.152:9092,192.168.197.152:9093");
    --------------------------------------------------------------------*/
    props.put("metadata.broker.list", "192.168.197.152:9092");

    //消息传递到broker时的序列化方式
    props.put("serializer.class", StringEncoder.class.getName());

    //zookeeper连接信息
    props.put("zookeeper.connect", "192.168.197.152:2181");

    /*-------------------------------------------------------------------
    //是否获取反馈
    //0是不获取反馈(消息有可能传输失败)
    //1是获取消息传递给leader后反馈(其他副本有可能接受消息失败)
    //-1是所有in-sync replicas接受到消息时的反馈
    --------------------------------------------------------------------*/
    props.put("request.required.acks", "1");

    //创建Kafka的生产者, key是消息的key的类型, value是消息的类型
    Producer<Integer, String> producer = new Producer<Integer, String>(
    new ProducerConfig(props)
    );

    /*--------------------------------------------------------------------
    //dstat命令，自行放在jar的目录，或者redhat的光盘有自带，此命令依赖python
    如果命令中带了管道符|的话，需要用这种方式执行：
    String[] s_check = {"/bin/bash","-c","vmstat 1 2 | sed -n '4p' |  awk '{print $15}'"};
    --------------------------------------------------------------------*/
    String[] s_hostname = {"/bin/bash","-c","hostname"};
    String[] s_date = {"/bin/bash","-c","date '+%F %T'"};
    String[] s_ip = {"/bin/bash","-c","cat /etc/hosts | grep $(hostname) | awk '{print $1}'"};
    String[] s_check_cpu = {"/bin/bash","-c","dstat -c 1 1 | sed -n '4p'"};
    String[] s_check_mem = {"/bin/bash","-c","dstat -m 1 1 | sed -n '4p'"};
    String[] s_check_disk = {"/bin/bash","-c","dstat -d 1 1 | sed -n '4p'"};
    String[] s_check_net = {"/bin/bash","-c","dstat -n 1 1 | sed -n '4p'"};
    String[] s_check_ip = {"/bin/bash", "-c", "date '+%F %T'"};
    String[] s_check = {"/bin/bash","-c","dstat -cmdn 1 1 | sed -n '4p'"};

    /*--------------------------------------------------------------------
    记录结果的相关变量：
    keyedMessage      : kafka的消息实例；
    message_hostname  : 主机名
    message_date      : 消息发生时的时间
    message_ip        : 主机ip地址
    message_cpu_idle  : 主机cpu空闲百分比 %
    message_cpu_usr   : 用户使用的cpu百分比 %
    message_cpu_sys   : 系统使用的cpu百分比 %
    message_cpu_wait  : cpu等待百分比 %
    message_mem_free  : 内存空闲量 MB
    message_mem_buffer: 内存对块设备缓冲量 MB
    message_mem_cache : 内存对寄存器缓存量 MB
    message_mem_used  : 内存使用量 MB
    message_net_send  : 网络总发送量 KB
    message_net_recv  : 网络总接收量 KB
    message_disk_write: 磁盘写入量 KB
    message_disk_read : 磁盘读取量 KB
    message_push      : 保存发送信息
    --------------------------------------------------------------------*/
    KeyedMessage<Integer, String> keyedMessage = null;
    String message_hostname = exe_command(s_hostname);
    String message_date = null;
    String message_ip = exe_command(s_ip);
    String message_cpu_idle = null;
    String message_cpu_usr = null;
    String message_cpu_sys = null;
    String message_cpu_wait = null;
    String message_mem_free = null;
    String message_mem_buffer = null;
    String message_mem_cache = null;
    String message_mem_used = null;
    String message_net_send = null;
    String message_net_recv = null;
    String message_disk_write = null;
    String message_disk_read = null;
    String message_push = null;
    Pattern p = null;
    Matcher m = null;
    String regex_str = "(\\d+\\s+)(\\d+\\s+)(\\d+\\s+)(\\d+\\s+)(\\d+\\s+)(\\d+)" +      //CPU
                       "(|)(.+[BkMG0]\\s+)(.+[BkMG0]\\s+)(.+[BkMG0]\\s+)(.+[BkMG0])" +   //MEMORY
                       "(|)(.+[BkMG0]\\s+)(.+[BkMG0])" +                                 //DISK
                       "(|)(.+[BkMG0]\\s+)(.+[BkMG0])";                                  //NETWORK

    while(true)
    {
      /*--------------------------------------------------------------------
      date
      --------------------------------------------------------------------*/
      message_date = exe_command(s_date);

      /*--------------------------------------------------------------------
      循环执行命令，获取系统状态信息：
      使用正则表达式获取各项值：

      Integrate-fetch，一次获取CPU，memory，disk，network信息，减少取样间隔
      --------------------------------------------------------------------*/
      p = Pattern.compile(regex_str);
      m = p.matcher(exe_command(s_check));
      while( m.find() )
      {
        message_cpu_usr = m.group(1).trim();
        message_cpu_sys = m.group(2).trim();
        message_cpu_idle = m.group(3).trim();
        message_cpu_wait = m.group(4).trim();
        message_mem_used = m.group(8).replace("|", "").trim();
        message_mem_buffer = m.group(9).trim();
        message_mem_cache = m.group(10).trim();
        message_mem_free = m.group(11).replace("|", "").trim();
        message_disk_read = m.group(13).replace("|", "").trim();
        message_disk_write = m.group(14).replace("|", "").trim();
        message_net_recv = m.group(16).replace("|", "").trim();
        message_net_send = m.group(17).replace("|", "").trim();
      }



      /*--------------------------------------------------------------------
      处理结果输出,返回json格式
      --------------------------------------------------------------------*/
      message_push = "{\"ip\":\""
      + message_ip
      + "\",\"hostname\":\""
      + message_hostname
      + "\",\"date\":\""
      + message_date
      + "\",\"cpu_usr\":\""
      + message_cpu_usr
      + "\",\"cpu_sys\":\""
      + message_cpu_sys
      + "\",\"cpu_idle\":\""
      + message_cpu_idle
      + "\",\"cpu_wait\":\""
      + message_cpu_wait
      + "\",\"mem_used\":\""
      + message_mem_used
      + "\",\"mem_buffer\":\""
      + message_mem_buffer
      + "\",\"mem_cache\":\""
      + message_mem_cache
      + "\",\"mem_free\":\""
      + message_mem_free
      + "\",\"disk_write\":\""
      + message_disk_write
      + "\",\"disk_read\":\""
      + message_disk_read
      + "\",\"net_send\":\""
      + message_net_send
      + "\",\"net_recv\":\""
      + message_net_recv
      + "\"}";

      message_push = message_push.replaceAll("\\r", "");


      //消息topic是test
      keyedMessage = new KeyedMessage<Integer, String>("test", message_push);
      //message可以带key, 根据key来将消息分配到指定区, 如果没有key则随机分配到某个区
      //          KeyedMessage<Integer, String> keyedMessage = new KeyedMessage<Integer, String>("test", 1, message);
      //producer.send(keyedMessage_df);
      producer.send(keyedMessage);
      //producer.send(keyedMessage_mem);

      try {
        Thread.sleep(1000);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }


  }

  public static String exe_command(String[] cmd)
  {
    InputStreamReader stdISR = null;
    InputStreamReader errISR = null;
    Process process = null;
    String line = null;
    String re = null;
    try
    {
      process = Runtime.getRuntime().exec(cmd);
      stdISR = new InputStreamReader(process.getInputStream());
      BufferedReader stdBR = new BufferedReader(stdISR);
      while ((line = stdBR.readLine()) != null)
      {
        if(re == null)
        {
          re = line + "\r";
        }
        else
        {
          re = re + line + "\r";
        }
      }
      errISR = new InputStreamReader(process.getErrorStream());
      BufferedReader errBR = new BufferedReader(errISR);
      while ((line = errBR.readLine()) != null)
      {
        System.out.println("ERR line:" + line);
      }
      //System.out.println(re);
      return re;
    }
    catch(IOException e)
    {
      e.printStackTrace();
    }
    finally
    {
      try
      {
        if(stdISR != null)
        {
          stdISR.close();
        }
        if(errISR != null)
        {
          errISR.close();
        }
        if(process != null)
        {
          process.destroy();
        }
      }
      catch(IOException e)
      {
        System.out.println("正式执行命令：" + cmd + "有IO异常");
      }
    }
    return "error";
  }

}
