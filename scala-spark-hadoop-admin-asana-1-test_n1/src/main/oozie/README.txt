Размещение файлов на hdfs
hdfs://hadoop/project-MD2/jobs/daily-statistic/lib/daily-statistic-0.1.jar
hdfs://hadoop/project-MD2/jobs/daily-statistic/workflow.xml
hdfs://hadoop/project-MD2/jobs/daily-statistic/coordinator.xml
hdfs://hadoop/project-MD2/jobs/daily-statistic/sharelib
Здесь в принципе всё понятно без комментариев за исключением директории sharelib. В эту директорию мы положим все библиотеки, которые использовались в процессе создания зашей задачи.
В нашем случае это все библиотеки Spark 2.0.0, который мы указывали в зависимостях проекта. Зачем это нужно? Дело в том, что в зависимостях проекта мы указали "provided". Это говорит системе сборки не нужно включать зависимости в проект, они будут предоставлены окружением запуска, но мир не стоит на месте, администраторы кластера могут обновить версию Spark. Наша задача может оказаться чувствительной к этому обновлению, поэтому для запуска будет использоваться набор библиотек из директории sharelib. Как это конфигурируется покажу ниже.
Написание workflow.xml
Для запуска задачи через Oozie нужно описать конфигурацию запуска в файле workflow.xml. Ниже привожу пример для нашей задачи:
<workflow-app name="project-md2-daily-statistic" xmlns="uri:oozie:workflow:0.5">

   <global>
      <configuration>
         <property>
            <name>oozie.launcher.mapred.job.queue.name</name>
            <value>${queue}</value>
         </property>
      </configuration>
   </global>

   <start to="project-md2-daily-statistic" />

   <action name="project-md2-daily-statistic">
      <spark xmlns="uri:oozie:spark-action:0.1">
         <job-tracker>${jobTracker}</job-tracker>
         <name-node>${nameNode}</name-node>
         <master>yarn-client</master>
         <name>project-md2-daily-statistic</name>
         <class>ru.daily.Statistic</class>
         <jar>${nameNode}${jobDir}/lib/daily-statistic-0.1.jar</jar>
         <spark-opts>
            --queue ${queue}
            --master yarn-client
            --num-executors 5
            --conf spark.executor.cores=8
            --conf spark.executor.memory=10g
            --conf spark.executor.extraJavaOptions=-XX:+UseG1GC
            --conf spark.yarn.jars=*.jar
            --conf spark.yarn.queue=${queue}
         </spark-opts>
         <arg>${nameNode}${dataDir}</arg>
         <arg>${datePartition}</arg>
         <arg>${nameNode}${saveDir}</arg>
       </spark>

       <ok to="end" />
       <error to="fail" />

   </action>

   <kill name="fail">
      <message>Statistics job failed [${wf:errorMessage(wf:lastErrorNode())}]</message>
   </kill>

   <end name="end" />

</workflow-app>
В блоке global устанавливается очередь, для MapReduce задачи которая будет находить нашу задачи и запускать её.
В блоке action описывается действие, в нашем случае запуск spark задачи, и что нужно делать при завершении со статусом ОК или ERROR.
В блоке spark определяется окружение, конфигурируется задача и передаются аргументы. Конфигурация запуска задачи описывается в блоке spark-opts. Параметры можно посмотреть в официальной документации
Если задача завершается со статусом ERROR, то выполнение переходит в блок kill и выводится кратное сообщение об ошибки. Параметры в фигурных скобках, например ${queue}, мы будем определять при запуске.
Написание coordinator.xml
Для организации регулярного запуска нам потребуется ещё coordinator.xml. Ниже приведу пример для нашей задачи:
<coordinator-app name="project-md2-daily-statistic-coord" frequency="${frequency}" start="${startTime}" end="${endTime}" timezone="UTC" xmlns="uri:oozie:coordinator:0.1">
    <action>
        <workflow>
            <app-path>${workflowPath}</app-path>
            <configuration>
                <property>
                    <name>datePartition</name>
                    <value>${coord:formatTime(coord:dateOffset(coord:nominalTime(), -1, 'DAY'), "yyyy/MM/dd")}</value>
                </property>
            </configuration>
        </workflow>
    </action>
</coordinator-app>
Здесь из интересного, параметры frequency, start, end, которые определяют частоту выполнения, дату и время начала выполнения задачи, дату и время окончания выполнения задачи соответственно.
В блоке workflow указывается путь к директории с файлом workflow.xml, который мы определим позднее при запуске.
В блоке configuration определяется значение свойства datePartition, которое в данном случае равно текущей дате в формате yyyy/MM/dd минус 1 день.
Запуск регулярного выполнения
И так сё готово к волнительному моменту запуска. Мы будем запускать задачу через консоль. При запуске нужно задать значения свойствам, которые мы использовали в xml файлах. Вынесем эти свойства в отдельный файл coord.properties:
# описание окружения
nameNode=hdfs://hadoop
jobTracker=hadoop-m1.rtk:8032

# путь к директории с файлом coordinator.xml
oozie.coord.application.path=/project-MD2/jobs/daily-statistic

# частота в минутах (раз в 24 часа)
frequency=1440
startTime=2017-09-01T07:00Z
endTime=2099-09-01T07:00Z

# путь к директории с файлом workflow.xml
workflowPath=/project-MD2/jobs/daily-statistic

# имя пользователя, от которого будет запускаться задача
mapreduce.job.user.name=username
user.name=username

# директория с данными и для сохранения результата
dataDir=/project-MD2/data 
saveDir=/project-MD2/status
jobDir=/project-MD2/jobs/daily-statistic 

# очередь для запуска задачи
queue=DMC

# использовать библиотеке из указанной директории на hdfs вместо системных
oozie.libpath=/project-MD2/jobs/daily-statistic/sharelib
oozie.use.system.libpath=false
Замечательно, тереть всё готово. Запускаем регулярное выполнение командой:
oozie job -oozie http://hadoop-m2.rtk:11000/oozie -config coord.properties -run
После запуска в консоль выведется id задачи. Используя это id можно посмотреть информацию о статусе выполнения задачи:
oozie job -info {job_id}
Остановить задачу:
oozie job -kill {job_id}
Если Вы не знаете id задачи, то можно найти его, показав все регулярные задачи для вашего пользователя:
oozie jobs -jobtype coordinator -filter user={user_name}
