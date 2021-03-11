*************** Distributed System Programming****************

**** student: **********
  - Moshe Dahan - 203509229

****************** System Overview and objective ***********
In our assignment, we were tasked to create a real-world application to distributively
process a list of PDF files, perform some operations on them, and display the result on a web page.
Each application will communicate with an application residing in an EC2 client.
The Web application will handle and distribute the PDF tasks between other EC2 clients using SQS and S3 buckets.
 
****************** Running instructions **************
Make sure you have the following files in the same folder:
1. LocalApp_jar.jar
2. input-file.txt

We also submitted another two jars file but they are already in the requested bucket!
1. manager_jar.jar
2. worker_jar.jar

The application should be run as follows:
1. Enter your AWS Credentials using export. 

	export AWS_SESSION_TOKEN=my-token

	export AWS_SECRET_ACCESS_KEY=my-access-key

	export AWS_ACCESS_KEY_ID=my-key-id

2. Enter AWS Region with Export.
	
	export AWS_REGION=us-east-1

3. Print one of the two optional command below:

	java -jar LocalApp_jar.jar inputFileName outputFileName n

	or, if you want to terminate the manager:

	java -jar LocalApp_jar.jar inputFileName outputFileName n terminate

where:

inputFileName is the name of the input file.  (must be as such "XXX.txt")
outputFileName is the name of the output file.(must be as such "XXX.html") 
n is: workers - files ratio (how many PDF files per worker).


**************** Implementation ************************

***** System Architecture ****
Our system is composed of 4 elements:

1. Local application - 
	The application resides on a local (non-cloud) machine. Each Local application starts with a unique id which we called APPId.
	Once started, it reads the input file from the user, and checks if a Manager node is active on the EC2 cloud. 
	If it is not, the application will start the manager node.
	Uploads the input-file to S3.
	Sends a message to an SQS queue ("manager_queue"), stating the location of the file on S3 ("inputfilesbucket123") and the APPId the file belongs to.
	Checks an SQS queue("summaryqueue123") for a message indicating the process is done and the response (the summary file) is available on S3 in "summarybucket123".
	Downloads the summary file from S3, and create an html file representing the results in the name of the outputfile stated in the running command by the user.
	Sends a termination message to the Manager if it was supplied as one of its input arguments.
	
	
2. Manager - 
	The manager process resides on an EC2 node. It checks a special SQS queue ("manager_queue") for messages from local applications. 
	The manager creates a ThreadPool for input files handling ("InputFileParser").
	Once it receives a message it:
		If the message is that of a new task it:
			the manager assigns a new/existing thread in the threadpool for handling it. 
			Checks the SQS message count and starts Worker processes (nodes) accordingly.
			The manager should create a worker for every n messages if there are no running workers.
			If there are k active workers, and the new job requires m workers, then the manager should create m-k new workers, if possible.

		If the message is a termination message, then the manager:
			It does not accept any more input files from local applications.
			Waits for all the workers to finish their job, and then terminate them.
			Creates response messages for the jobs, if needed.
			Terminates.

		For every message in the "doneworkersqueue" queue, the manager updates the "messagecounter" for the message Appid counter and log it details 
		for later use in the summaryfile creation. 
		
		If for a specific APPId the message count in the "doneworkersqueue" has reached 0 (meaning all PDF tasks has been processed), then 
		the manager creates a SummryFile ("summaryfile"+APPId) in  "summarybucket123". after that the manager sends a "done task" message indicating the APPId,
		summaryfile url to the queue "summaryqueue".

3. Worker - 
	A worker process resides on an EC2 node. Its life cycle is as follows:
	
	Repeatedly:

		Get a message from an SQS queue("workerQueue").
		Download the PDF file indicated in the message attributes.
		Perform the operation requested on the file.
		Upload the resulting output file to S3 ("donePDFBucket123" + APPId).
		Put a message in an SQS queue("doneworkersqueue123") indicating the APPId, original URL of the PDF, the S3 url of the new image file, and the operation that was performed.
		remove the processed message from the SQS queue("workerQueue123")
	
4. InputFileParser Threadpool - 
	inside The Manager, there is a ThreadPool (currently we used a fixed-sized threadpool in the size of 10) controlled by the main thread.
	The Main manager thread assigns each thread in the threadpool an input file it receives using an Executor.
	Each thread downloads the input file from S3("inputfilesbucket123") and parses the input file it receives. 
	Each line in the inputfile represents a worker task, so for every line,  in the input-file the thread creates an SQS message ("new PDF task" to the "workerQueue123") .
	it also attaches to the message the Appid so the worker can connect the task to its origin.
	

The elements will communicate with each other using queues (SQS) and storage (S3).

******* AMI: *******
We used for every worker and manager this AMI:  ami-076515f20540e6e0b.
Type -T2-Micro
  
******* Queues and Messages: *******

 1. manager_queue123 - used for LocalApp -> Manager communication. Each LocalApp sends an SQS "new task" message which in turn the manager collects and processes it. 
 2. workerQueue123 - used for manager -> workers communication. After assigning and parsing the input file a "new PDF task" message is created which in turn a worker will collect and process it. 
 3. doneworkersqueue123 - used for workers -> manager communication. After Processing a File and creating relevant output the worker sends a "done PDF task" with the specified APPId  which in turn the manager collects and processes it. 
 4. summaryqueue123 - used for Manager -> LocalApp communication. After completing all PDF tasks by the workers the manager sends a "Done task" which stats  the location of summaryfile which contains the summation of all the tasks work.
 
 
******* Buckets: *******

 1. inputfilesbucket123 - containing all input-files to process.
 2. jarbucketforlife123 - if necessary contains the .jar files for manager and worker.
 3. donePDFBucket123+APPId - for each localapp this bucket created and contains all the results for its input-file Pdfs and tasks.
 4. summarybucket123 - contains all the summaryfiles for all the localapps.
 
 
******* Termination process: *******

when a LocalApp receives "terminate" as one of its arguments, a "terminate" message is sent to do manager in the manager_queue123.
When the manager receives the "terminate" message, first of all, it is changing an inside flag which stating to stop receiving any more input-files.
Then the manager makes sure all current tasks made by previous local apps are finished and their summaryfiles were created.
only then it terminates all active workers, shutdown its Threads and threadpool service and in the end, it terminates itself.
 

******* Work-Sharing: *******

To avoid overload and even work distribution we created the following:
 1. Java executor framework  -  we used an Executor and a threadpool for dividing and releasing the Main manager thread  to be occupied by parsing inputfiles.
	when a manager detects a new message with a new input file, it hands the task of parsing and creating "new PDF tasks" messages to the Executor which assigns this task to an available thread.
	that way the manager stays free and can accept new local app connections by reading new messages and the work for parsing the files are divided evenly by the executor.
	
 2. We choose workers-Queue communication instead of worker-manager for assigning PDF tasks. Each worker essentially throughout it life polls the workerQueue for a new PDF message.
	when a message like that exists in the queue the worker process and handles the task within.  once it is done, the worker returns for polling the queue. 
	In this manner of work no worker is doing more work then it fellow workers. All are polling from the same queue if there are tasks in the queue the work is assigned dynamically.
	Meaning all workers are working until the tasks are finished in the queue.
	If we would have assigned a group of tasks to specific workers we could have resulted in uneven distribution.
	
	
******* Persistence: *******

If for some reason worker nodes died, our system can detect that.
The manager reads the number of messages in workers queue and in relation to N and existing workers it creates new workers so the workers - files ratio will remain true.

If for some reason a worker is getting stuck and doesn't complete its task then the visibility property will be activated. 
When a message is read by some worker a timer starts for the message visibility. Any other worker could not read this message until the time runs out in the timer. 
This means, if any worker gets stuck then the message becomes visible again and some other worker will receive it and complete this task.

In case of manager crashing it is possible to add a restore method which will be responsible to keep the state of two main map: 
TASKS_COUNTER_FOR_EACH_LOCAL_APP and HTML_BODY_FOR_EACH_LOCAL_APP which in our case stores the manager state. Then Local app will detect a manager is not running and will start a new one.
when the manager will start it will call this method which uses the rostered maps and will resume from the point the last manager stopped.

******* Scalability: *******

Our project is scalable and defined by system limitations and client demands:

threadpool - Our manager contains an Executor service with fixed threadpool (10). We kept it low because we didn't want to overload the system.
In our course and project, we are limited in funds(credits) so we chose a t2.micro setting for the ec2 clients we created. 
This setting is a low CPU power and Network Performance. If the customer, which will buy the program define he wants a stronger faster system we can choose a higher level setting
and increase the number of threads in our thread pool for results enhancing. we can even 

Number of workers - besides of N the worker-tasks ratio, we are limited by amazon in creating many clients for workers as we want. so we put a limit inside the code to not create
too many workers even if the ratio allows it. If we will pay for a higher clearance account we will be able to scale up our system and support many more clients in the 
approximately the same computation time.


The system can support as many clients as the storage allows us. Every additional client communicates through the SQS and doesn't change our system architecture besides adding more workers.
Note: It won't be as fast with many clients because of the limitations I stated above.

******** Other questions: ******

#security - we did not enter our credentials in the code, so it is safe.

#in-flight-mode - A message is considered to be in flight after it is received from a queue by a consumer (worker/manager/localapp), but not yet deleted from the queue.

#You have 2 apps connecting to the manager, both sent task requests and are waiting for an answer. The manager uses a single queue to write answers to apps. The manager posted 2 answers in the queue. 
How would you play with visibility time to make your program as efficient as possible?
We would put the lowest possible visibility time. In our project, each app has a unique id (APPId), which all the messages contain related to this app.
So when an app reads the message which contains different APPId it discards it and moves on. In that way, the other app won't have to wait for visibility timeout to end.

#What happens if you delete a message immediately after you take from the queue? Two scenarios:
 1) worker takes a message deletes it from the queue processes it and returns an answer. 
	# In this case it appears the message is being processed and a result is saved and all is good. 
	  But there could be another scenario in which the worker fails in the process and the deletion which will cause that we can't restore the message and we will lose results.
	Hence we don't delete messages before we process them, to prevent this scenario.
 
 2) Local app takes a message from the answer queue deletes it and downloads the file.
	# in this case the manager could fail in the process of the file because it was corrupted or for any other reason, therefore the deletion has caused that 
	we can't restore the message and we lost the file request by the app.	
	
#Questions about memory:
	Lets say the manager saves the result of each message in its memory , is such a solution scalable? How would you solve it? 
	(write to files buffers of reviews once it reaches a certain size)
		#it is scalable up until the memory limitations, if we can't increase the size of the manager memory we are in a problem.
		 we can semi-solve this by using the S3 storage and writing some of the results files buffers to a S3 bucket file and read from it when needed.

#Threads - as Bonus: implement in a scalable efficient Way
	Lets say the manager opens a thread for each local applications, advantages and disadvantages? (advantages: faster, disadvantages: not scalable) (possible improvement: thread pool		 
		# the advantages are that each thread handles on file and sends messages on its own while releasing the manager to do its work. This resulting in a faster implementation.
		  the disadvantage is every thread is taking up space and memory, CPU usage and overloads the system and don't release its resources when it finished.    
		# solution: thread pool
          actually in our project we did something like that. we created threadpool for handling every file the manager receives. it has a fixed size so it won't grow bigger and bigger.
          and we enjoy a faster mechanism while keeping the overhead relatively low.
******** Example Run: *********
We run the following line java -jar localapp.jar input-sample1.txt my3.html 33 terminate

The local app created a manager and the manager created 3 workers.
It took 3:45 seconds to the system to complete its job.
There were 100 files in the inputFile.txt . 