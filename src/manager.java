import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import java.io.*;
import java.nio.file.Paths;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class manager {
    private static final Region REGION = Region.US_EAST_1;
    private static ExecutorService EXECUTOR = Executors.newFixedThreadPool(10);
    private static final String WORKER_TYPE = "worker";
    private static Map<String, Integer> TASKS_COUNTER_FOR_EACH_LOCAL_APP = new HashMap<>();
    private static Integer NUMBER_OF__LOCAL_APPS = 1;
    private static Map<String, List<String>> HTML_BODY_FOR_EACH_LOCAL_APP = new HashMap<>(); //<appID,List<Strting> - Html Body per appId

    private static final Integer PENDING_INSTANCE = 0;
    private static final Integer RUNNING_INSTANCE = 16;

    private static final String NEW_PDF_TASKS_BUCKET = "newpdftasksbucket123";
    private static final String SUMMARY_BUCKET = "summarybucket123";
    private static final String JAR_BUCKET = "jarsbucketforlife123";

    private static final String WORKERS_QUEUE = "workerQueue123";
    private static final String MANAGER_QUEUE = "managerQueue123";
    private static final String DONE_WORKERS_QUEUE = "doneworkersqueue123";
    private static final String SUMMARY_QUEUE = "summaryqueue123";

    public static void main(String[] args) {
        // -------- S3 --------
        S3Client s3 = S3Client.builder().region(REGION).build();
        createBucket(SUMMARY_BUCKET, s3);

        // -------- SQS --------
        SqsClient sqs = SqsClient.builder().region(REGION).build();
        String workersQueueUrl = createQueue(WORKERS_QUEUE, sqs);
        String doneWorkersQueueUrl = createQueue(DONE_WORKERS_QUEUE, sqs);

        // -------- EC2 --------
        Ec2Client ec2 = Ec2Client.create();

        // -------- RUN --------
        mangerLogic(ec2, NEW_PDF_TASKS_BUCKET, workersQueueUrl, sqs, false, s3, doneWorkersQueueUrl, getQueueURL(SUMMARY_QUEUE, sqs));

    }

    public static String getQueueURL(String queueName, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static void scanAllMessagesInDoneQueue(SqsClient sqs, S3Client s3, String doneWorkersQueueUrl, String summaryQueueUrl) {
        if (getNumberOfMessagesInQueue(DONE_WORKERS_QUEUE, sqs) != 0) {
            List<Message> messagesList = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(doneWorkersQueueUrl).maxNumberOfMessages(10).messageAttributeNames("All").build()).messages();
            for (Message currentMsg : messagesList) {
                Map<String, MessageAttributeValue> msgAttributes = currentMsg.messageAttributes();
                String currentAppId = msgAttributes.get("Appid").stringValue();
                if (msgAttributes.containsKey("type") && msgAttributes.get("type").stringValue().equals("newAppId")) {
                    String currentUrlCounter = msgAttributes.get("urlcount").stringValue();
                    if (!(TASKS_COUNTER_FOR_EACH_LOCAL_APP.containsKey(currentAppId))) {
                        TASKS_COUNTER_FOR_EACH_LOCAL_APP.put(currentAppId, Integer.parseInt(currentUrlCounter));
                    }
                } else if (msgAttributes.containsKey("type") && (msgAttributes.get("type").stringValue().equals("done PDF task") || msgAttributes.get("type").stringValue().equals("pdf error"))) {
                    if (TASKS_COUNTER_FOR_EACH_LOCAL_APP.containsKey(currentAppId)) {
                        int currentTasksCount = TASKS_COUNTER_FOR_EACH_LOCAL_APP.get(currentAppId) - 1;
                        if (TASKS_COUNTER_FOR_EACH_LOCAL_APP.get(currentAppId) != 0) {
                            TASKS_COUNTER_FOR_EACH_LOCAL_APP.put(currentAppId, currentTasksCount);
                        }
                        String exceptionMessage = msgAttributes.get("exceptionMessage").stringValue();
                        String toDo = msgAttributes.get("toDo").stringValue();
                        String url = msgAttributes.get("url").stringValue();
                        String outputFileUrl = msgAttributes.get("outputFileUrl").stringValue();
                        String type = msgAttributes.get("type").stringValue();
                        addDoneSummaryToSummeryBodyPerAppId(currentAppId, toDo, url, outputFileUrl, type, exceptionMessage);
                        if (TASKS_COUNTER_FOR_EACH_LOCAL_APP.get(currentAppId) == 0) {
                            Map<String, MessageAttributeValue> doneMessageAttributes = new HashMap<>();
                            String summaryFileUrl = generateSummaryFile(s3, HTML_BODY_FOR_EACH_LOCAL_APP.get(currentAppId), currentAppId);
                            doneMessageAttributes.put("resultsbucket", MessageAttributeValue.builder().dataType("String").stringValue(SUMMARY_BUCKET).build());
                            doneMessageAttributes.put("Appid", MessageAttributeValue.builder().dataType("String").stringValue(currentAppId).build());
                            doneMessageAttributes.put("SummaryFileUrl", MessageAttributeValue.builder().dataType("String")
                                    .stringValue(summaryFileUrl).build());
                            doneMessageAttributes.put("type", MessageAttributeValue.builder().dataType("String").stringValue("done task").build());
                            sqsRequest(sqs, "done task", summaryQueueUrl, doneMessageAttributes);
                            NUMBER_OF__LOCAL_APPS--;
                        }
                    } else {
                        break;
                    }
                }
                sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(doneWorkersQueueUrl).receiptHandle(currentMsg.receiptHandle()).build());
            }
        }
    }

    private static String generateSummaryFile(S3Client s3, List<String> messageBody, String appID) {
        File summaryHtmlFile = new File("summary-file" + appID);
        String summeryFileContent = "";
        for (String line : messageBody) {
            summeryFileContent = summeryFileContent + line + "\n";
        }
        FileWriter myWriter = null;
        try {
            myWriter = new FileWriter(summaryHtmlFile.getName());
            myWriter.write(summeryFileContent);
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        s3.putObject(PutObjectRequest.builder().bucket(SUMMARY_BUCKET).key(summaryHtmlFile.getName())
                        .acl(ObjectCannedACL.PUBLIC_READ_WRITE).build(),
                RequestBody.fromFile(Paths.get(summaryHtmlFile.getPath())));
        return "https://" + SUMMARY_BUCKET + ".s3.amazonaws.com/" + summaryHtmlFile.getName();
    }

    private static void addDoneSummaryToSummeryBodyPerAppId(String AppId, String toDo, String Source, String Destination, String type, String exceptionMessage) {
        String dest = exceptionMessage;
        if (!HTML_BODY_FOR_EACH_LOCAL_APP.containsKey(AppId)) {
            List<String> NewListToNewAppId = new ArrayList<>();
            HTML_BODY_FOR_EACH_LOCAL_APP.put(AppId, NewListToNewAppId);
        }
        if (!(type.equals("pdf error"))) {
            dest = Destination;
        }
        String HTMLMSGsummary = toDo + " " + Source + " " + dest;
        HTML_BODY_FOR_EACH_LOCAL_APP.get(AppId).add(HTMLMSGsummary);
    }

    public static void mangerLogic(Ec2Client ec2, String newPdfTasksBucketName, String workersQueueUrl, SqsClient sqs, boolean terminate, S3Client s3, String doneWorkersQueueUrl, String summaryQueueUrl) {
        String managerQueueURL = getQueueURL(MANAGER_QUEUE, sqs);
        boolean doneQueueOfAllApps = false;
        int N = 0; //to prevent entering in the first time. indicates first loop.
        boolean stopReceivingInputFiles = false;
        while (!terminate || !doneQueueOfAllApps) {
            if (NUMBER_OF__LOCAL_APPS == 0) {
                doneQueueOfAllApps = true;
            }
            List<Message> messagesList = sqs.receiveMessage(ReceiveMessageRequest.builder().maxNumberOfMessages(10).queueUrl(managerQueueURL).messageAttributeNames("All").build()).messages();
            if (!(messagesList.size() == 0)) {
                for (Message currentMsg : messagesList) {
                    Map<String, MessageAttributeValue> msgAttributes = currentMsg.messageAttributes();
                    if (msgAttributes.containsKey("type") && msgAttributes.get("type").stringValue().equals("terminate")) {
                        stopReceivingInputFiles = true;
                        if (doneQueueOfAllApps) {
                            terminate = true;
                            sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(managerQueueURL).receiptHandle(currentMsg.receiptHandle()).build());
                            TerminateWorkersInstances(ec2);
                            EXECUTOR.shutdown();
                            try {
                                EXECUTOR.awaitTermination(Long.MAX_VALUE, TimeUnit.NANOSECONDS);
                            } catch (InterruptedException e) {
                                System.out.println("Couldn't Shut Down EXECUTOR");
                            }
                            break;
                        }
                    } else if (!stopReceivingInputFiles) {
                        if (N != 0) { // skips increasing this flag in the first time
                            NUMBER_OF__LOCAL_APPS++;
                        }
                        doneQueueOfAllApps = false;
                        String fileName = msgAttributes.get("fileName").stringValue();
                        String appID = msgAttributes.get("AppID").stringValue();
                        String bucketName = msgAttributes.get("bucketName").stringValue();
                        N = Integer.parseInt(msgAttributes.get("N").stringValue());
                        System.out.println("Manager start running with new input file");
                        sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(managerQueueURL).receiptHandle(currentMsg.receiptHandle()).build());
                        System.out.println("Manager deleted the new input file after loading it.");
                        EXECUTOR.execute(new InputFileParser(newPdfTasksBucketName, workersQueueUrl, sqs, fileName, bucketName, appID, getQueueURL(DONE_WORKERS_QUEUE, sqs), s3));
                    }
                }
            }
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            if (getNumberOfMessagesInQueue(WORKERS_QUEUE, sqs) > 0 && N != 0) {
                activateWorkers(ec2, N, sqs);
            }
            scanAllMessagesInDoneQueue(sqs, s3, doneWorkersQueueUrl, summaryQueueUrl);
        }
        killAllInstances(ec2);
        System.out.println("Manager Terminated");
    }

    private static void TerminateWorkersInstances(Ec2Client ec2) {
        List<Instance> ActiveWorkers = getWorkers(ec2);
        List<String> instancesIdList = new ArrayList<>();
        for (Instance workerInst : ActiveWorkers) {
            instancesIdList.add(workerInst.instanceId());
        }
        if (instancesIdList.size() > 0) {
            ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instancesIdList).build());
        }
    }

    private static List<Instance> getWorkers(Ec2Client ec2) {
        List<Instance> workertsList = new ArrayList<>();
        for (Reservation res : ec2.describeInstances().reservations()) {
            for (Instance instance : res.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.value().equals(WORKER_TYPE) && tag.key().equals("type")) {
                        int mode = instance.state().code();
                        if (mode == PENDING_INSTANCE || mode == RUNNING_INSTANCE) {
                            workertsList.add(instance);
                        }
                    }
                }
            }
        }
        return workertsList;
    }

    private static void killAllInstances(Ec2Client ec2) {
        for (Reservation res : ec2.describeInstances().reservations()) {
            for (Instance instance : res.instances()) {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
            }
        }
        System.out.println("Delete All Instances");
    }

    private static void activateWorkers(Ec2Client ec2, int N, SqsClient sqs) {
        int currentNumberOfWorkers = getNumberOfWorkers(ec2);
        int numberOfMessageInWorkersQueue = getNumberOfMessagesInQueue(WORKERS_QUEUE, sqs);
        int newNumberOfWorkers = Math.min((int) Math.ceil((double) numberOfMessageInWorkersQueue / N) - currentNumberOfWorkers, 19);
        String amiId = "ami-076515f20540e6e0b";

        if (currentNumberOfWorkers < newNumberOfWorkers) {
            List<Tag> tags = new ArrayList<>();
            tags.add(Tag.builder()
                    .key("type")
                    .value(WORKER_TYPE)
                    .build());
            tags.add(Tag.builder()
                    .key("Name")
                    .value(WORKER_TYPE)
                    .build());

            RunInstancesRequest runRequest = RunInstancesRequest.builder()
                    .imageId(amiId)
                    .instanceType(InstanceType.T2_MICRO)
                    .userData(getEC2WorkerData(WORKER_TYPE))
                    .maxCount(newNumberOfWorkers)
                    .minCount(newNumberOfWorkers)
                    .tagSpecifications(TagSpecification.builder().tags(tags).resourceType("instance").build())
                    .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("ec2Role").build())
                    .build();
            try {
                ec2.runInstances(runRequest);
            } catch (Ec2Exception e) {
                System.err.println(e.getMessage());
            }
            System.out.println("Manager created " + newNumberOfWorkers + " " + WORKER_TYPE + " successfully");

        }
    }

    private static int getNumberOfWorkers(Ec2Client ec2) {
        int numberOfWorkers = 0;
        for (Reservation res : ec2.describeInstances().reservations()) {
            for (Instance instance : res.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.value().equals(WORKER_TYPE) && tag.key().equals("type")) {
                        int mode = instance.state().code();
                        if (mode == PENDING_INSTANCE || mode == RUNNING_INSTANCE) {
                            numberOfWorkers++;
                        }
                    }
                }
            }
        }
        return numberOfWorkers;
    }

    public static int getNumberOfMessagesInQueue(String queueName, SqsClient sqs) {
        String queueURL = getQueueURL(queueName, sqs);
        GetQueueAttributesResponse messagesList = sqs.getQueueAttributes(GetQueueAttributesRequest.builder().attributeNames(QueueAttributeName.fromValue("ApproximateNumberOfMessages")).queueUrl(queueURL).build());
        Map<String, String> app = messagesList.attributesAsStrings();
        return Integer.parseInt(app.get("ApproximateNumberOfMessages"));
    }

    private static void createBucket(String bucket, S3Client s3) {
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucket)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        System.out.println("Manager created bucket: " + bucket);
    }


    public static String createQueue(String queueName, SqsClient sqs) {
        Map<QueueAttributeName, String> timeOutAttribute = new HashMap<QueueAttributeName, String>();
        timeOutAttribute.put(QueueAttributeName.RECEIVE_MESSAGE_WAIT_TIME_SECONDS, "20");
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
                    .attributes(timeOutAttribute)
                    .build();
            CreateQueueResponse create_result = sqs.createQueue(request);
        } catch (QueueNameExistsException e) {
            throw e;

        }
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        String queueUrl = sqs.getQueueUrl(getQueueRequest).queueUrl();
        return queueUrl;
    }

    public static void sqsRequest(SqsClient sqs, String messageBody, String
            queueUrl, Map<String, MessageAttributeValue> messageAttributes) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .delaySeconds(5)
                .messageAttributes(messageAttributes)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static String getEC2WorkerData(String instaceType) {
        String userData = "";
        if (instaceType.equals(WORKER_TYPE)) {
            userData = userData + "#!/bin/bash" + "\n";
            userData = userData + "aws s3 cp s3://" + JAR_BUCKET + "/worker_jar.jar worker_jar.jar" + "\n";
            userData = userData + "java -jar worker_jar.jar" + "\n";
        }
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }

    private static class InputFileParser implements Runnable {
        private final String newPdfTasksBucketName;
        private final String workersQueueUrl;
        private final SqsClient sqs;
        private final String inputFile;
        private final String inputFileBucket;
        private final String appID;
        private final String doneWorkersQueueNameUrl;
        private final S3Client s3;

        public InputFileParser(String newPdfTasksBucketName, String workersQueueUrl, SqsClient sqs, String inputFile, String inputFileBucket, String appID, String doneWorkersQueueNameUrl, S3Client s3) {
            this.newPdfTasksBucketName = newPdfTasksBucketName;
            this.workersQueueUrl = workersQueueUrl;
            this.sqs = sqs;
            this.inputFile = inputFile;
            this.inputFileBucket = inputFileBucket;
            this.appID = appID;
            this.doneWorkersQueueNameUrl = doneWorkersQueueNameUrl;
            this.s3 = s3;
        }

        @Override
        public void run() {
            List<String> pdfList = generatePDFListFromInputFile(inputFile, inputFileBucket, s3);
            sqsRequestForEachPDF(newPdfTasksBucketName, sqs, pdfList, workersQueueUrl, appID, doneWorkersQueueNameUrl);
        }

        private static List<String> generatePDFListFromInputFile(String fileName, String bucketName, S3Client s3) {
            List<String> pdfList = new ArrayList<>();
            ResponseInputStream<GetObjectResponse> s3objectResponse = s3.getObject(GetObjectRequest.builder().bucket(bucketName).key(fileName).build());
            BufferedReader reader = new BufferedReader(new InputStreamReader(s3objectResponse));
            String line;
            while (true) {
                try {
                    if ((line = reader.readLine()) == null) break;
                    pdfList.add(line);
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            return pdfList;
        }

        private static void sqsRequestForEachPDF(String newPdfTasksBucketName, SqsClient sqs, List<String> pdfList, String workersQueueURL, String appID, String doneWorkersQueueNameUrl) {
            int counter = 0;
            for (String newTask : pdfList) {
                String[] toDoAndUrl = newTask.split("\t");
                String bucketName = newPdfTasksBucketName + appID;
                Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
                messageAttributes.put("type", MessageAttributeValue.builder().dataType("String").stringValue("new PDF task").build());
                messageAttributes.put("toDo", MessageAttributeValue.builder().dataType("String").stringValue(toDoAndUrl[0]).build());
                messageAttributes.put("Appid", MessageAttributeValue.builder().dataType("String").stringValue(appID).build());
                messageAttributes.put("bucketName", MessageAttributeValue.builder().dataType("String").stringValue(bucketName).build());
                messageAttributes.put("url", MessageAttributeValue.builder().dataType("String").stringValue(toDoAndUrl[1]).build());
                sqsRequest(sqs, "new PDF task", workersQueueURL, messageAttributes);
                counter++;
            }

            Map<String, MessageAttributeValue> counterMessageAttributes = new HashMap<>();
            counterMessageAttributes.put("urlcount", MessageAttributeValue.builder().dataType("String").stringValue(String.valueOf(counter)).build());
            counterMessageAttributes.put("Appid", MessageAttributeValue.builder().dataType("String").stringValue(appID).build());
            counterMessageAttributes.put("type", MessageAttributeValue.builder().dataType("String").stringValue("newAppId").build());
            sqsRequest(sqs, "number of tasks for local app" + appID, doneWorkersQueueNameUrl, counterMessageAttributes);
        }
    }
}


