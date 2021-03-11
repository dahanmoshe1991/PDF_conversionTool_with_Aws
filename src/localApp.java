import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.ec2.Ec2Client;
import software.amazon.awssdk.services.ec2.model.*;
import software.amazon.awssdk.services.ec2.model.Tag;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;
import software.amazon.awssdk.regions.Region;

import java.io.*;
import java.net.URL;
import java.util.*;
import java.util.concurrent.TimeUnit;

public class localApp {
    private static S3Client s3;
    private static final Integer PENDING_INSTANCE = 0;
    private static final Integer RUNNING_INSTANCE = 16;
    private static final String MANAGER_TYPE = "manager";

    private static final String APP_ID = UUID.randomUUID().toString().replace('\\', '_').replace('/', '_').replace(':', '_');
    private static final Region REGION = Region.US_EAST_1;

    private static final String JAR_BUCKET = "jarsbucketforlife123";
    private static final String INPUT_FILES_BUCKET = "inputfilesbucket123";

    private static final String SUMMARY_QUEUE = "summaryqueue123";
    private static final String MANAGER_QUEUE_NAME = "managerQueue123";

    private static final String MANAGER_JAR_FILE_PATH = "manager.jar"; //todo change this if want to use uploadJar function.
    private static final String WORKER_JAR_FILE_PATH = "worker_jar.jar"; //todo change this if want to use uploadJar function.
    private static final String MANAGER_JAR_FILE_NAME = "manager.jar";
    private static final String WORKER_JAR_FILE_NAME = "worker_jar.jar";


    public static void main(String[] args) throws IOException {
        String inputFile = args[0];
        String outputFileName = args[1];
        int N = Integer.parseInt(args[2]);
        String terminateArg = "";
        boolean terminateInTheEnd = false;
        if (args.length > 3) {
            terminateArg = args[3];
            if (terminateArg.equals("terminate")) {
                terminateInTheEnd = true;
            }
        }

        // -------- SQS --------
        SqsClient sqs = SqsClient.builder().region(REGION).build();
        // --------  S3 --------
        s3 = S3Client.builder().region(REGION).build(); // Creates S3 Client.
        // -------- EC2 --------
        Ec2Client ec2 = Ec2Client.create();

        //-------- SHUTDOWN -----
//        ShutDownEverything(sqs, ec2, s3);

        // -------- JAR --------
        //createBucket(JAR_BUCKET, REGION, s3);
        //uploadJars(s3);

        // ----- INPUTFILE -----
        createBucket(INPUT_FILES_BUCKET, REGION, s3);
        putFile(new File(inputFile), inputFile, INPUT_FILES_BUCKET, true, s3);
        System.out.println("Uploaded Input File");
        String summaryQueueUrl = createQueue(SUMMARY_QUEUE, sqs);

        if (!isManagerRunning(ec2)) {
            ec2Builder(MANAGER_TYPE, ec2);
        }
        String queueUrl = createQueue(MANAGER_QUEUE_NAME, sqs);
        Map<String, MessageAttributeValue> fileToSend = new HashMap<String, MessageAttributeValue>();
        fileToSend.put("FilePath", MessageAttributeValue.builder().dataType("String").stringValue("s3://" + INPUT_FILES_BUCKET + "/" + inputFile).build());
        fileToSend.put("AppID", MessageAttributeValue.builder().dataType("String").stringValue(APP_ID).build());
        fileToSend.put("bucketName", MessageAttributeValue.builder().dataType("String").stringValue(INPUT_FILES_BUCKET).build());
        fileToSend.put("fileName", MessageAttributeValue.builder().dataType("String").stringValue(inputFile).build());
        fileToSend.put("N", MessageAttributeValue.builder().dataType("Number").stringValue(String.valueOf(N)).build());
        sqsRequest(sqs, "New Task", queueUrl, fileToSend);
        Boolean shouldTerminate = false;
        while (!shouldTerminate) {
            shouldTerminate = waitForDoneTask(sqs, summaryQueueUrl, terminateInTheEnd, outputFileName);
        }

    }

    private static void ShutDownEverything(SqsClient sqs, Ec2Client ec2, S3Client s3) {
        for (String queueUrl : sqs.listQueues().queueUrls()) {
            sqs.deleteQueue(DeleteQueueRequest.builder().queueUrl(queueUrl).build());
        }
        System.out.println("Delete All Queues");

        for (Reservation res : ec2.describeInstances().reservations()) {
            for (Instance instance : res.instances()) {
                ec2.terminateInstances(TerminateInstancesRequest.builder().instanceIds(instance.instanceId()).build());
            }
        }
        System.out.println("Delete All Instances");
    }

    private static void uploadJars(S3Client s3) {
        putFile(new File(WORKER_JAR_FILE_PATH), MANAGER_JAR_FILE_NAME, JAR_BUCKET, true, s3);
        System.out.println("Uploaded Manager Jar Files To S3.");
        putFile(new File(MANAGER_JAR_FILE_PATH), WORKER_JAR_FILE_NAME, JAR_BUCKET, true, s3);
        System.out.println("Uploaded Worker Jar Files To S3.");
    }

    private static Boolean waitForDoneTask(SqsClient sqs, String summaryQueueUrl, boolean terminateInTheEnd, String outputFileName) throws IOException {
        if (getNumberOfMessagesInQueue(SUMMARY_QUEUE, sqs) != 0) {
            List<Message> messagesList = sqs.receiveMessage(ReceiveMessageRequest.builder().queueUrl(summaryQueueUrl).maxNumberOfMessages(10).messageAttributeNames("All").build()).messages();
            for (Message currentMsg : messagesList) {
                Map<String, MessageAttributeValue> msgAttributes = currentMsg.messageAttributes();
                String currentAppId = msgAttributes.get("Appid").stringValue();
                if (msgAttributes.containsKey("type") && msgAttributes.get("type").stringValue().equals("done task") && currentAppId.equals(APP_ID)) {
                    System.out.println("Received Done Task message");
                    convertSummaryFileToHtml(msgAttributes.get("SummaryFileUrl").stringValue(), outputFileName);
                    if (terminateInTheEnd) {
                        createTerminateMessage(sqs);
                    }
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(summaryQueueUrl).receiptHandle(currentMsg.receiptHandle()).build());
                    return true;
                }
            }
        }
        return false;
    }

    public static void createTerminateMessage(SqsClient sqs) {
        Map<String, MessageAttributeValue> teminateAtt = new HashMap<String, MessageAttributeValue>();
        teminateAtt.put("type", MessageAttributeValue.builder().dataType("String").stringValue("terminate").build());
        sqsRequest(sqs, "terminate", getQueueURL(MANAGER_QUEUE_NAME, sqs), teminateAtt);
    }

    public static int getNumberOfMessagesInQueue(String queueName, SqsClient sqs) {
        String queueURL = getQueueURL(queueName, sqs);
        GetQueueAttributesResponse messagesList = sqs.getQueueAttributes(GetQueueAttributesRequest.builder().attributeNames(QueueAttributeName.fromValue("ApproximateNumberOfMessages")).queueUrl(queueURL).build());
        Map<String, String> app = messagesList.attributesAsStrings();
        return Integer.parseInt(app.get("ApproximateNumberOfMessages"));
    }

    public static String getQueueURL(String queueName, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static File downloadSummaryFileFromS3(String SummaryFileUrl, String outputFileName) throws IOException {
        File SummaryFileNeedToBeConvertedToHtmal = new File(outputFileName);
        int readTil;
        URL urlToProcess = new URL(SummaryFileUrl);
        InputStream inputStream = urlToProcess.openStream();
        OutputStream outputStream = new FileOutputStream(SummaryFileNeedToBeConvertedToHtmal);
        byte[] byteReader = new byte[2048 * 2];
        while ((readTil = inputStream.read(byteReader)) != -1) {
            outputStream.write(byteReader, 0, readTil);
        }
        inputStream.close();
        outputStream.close();
        return SummaryFileNeedToBeConvertedToHtmal;
    }

    private static File convertSummaryFileToHtml(String SummaryFileUrl, String outputFileName) throws IOException {
        File SummaryFileHtml = downloadSummaryFileFromS3(SummaryFileUrl, outputFileName);
        FileWriter myWriter = null;
        String summeryFileContent = "<HTML><BODY>";
        BufferedReader reader;
        try {
            reader = new BufferedReader(new FileReader(
                    SummaryFileHtml.getName()));
            String line = reader.readLine();
            while (line != null) {
                summeryFileContent = summeryFileContent + "<p>" + line + "</p>" + "\n";
                line = reader.readLine();
            }
            reader.close();
            summeryFileContent = summeryFileContent + "</BODY></HTML>";
            myWriter = new FileWriter(SummaryFileHtml.getName());
            myWriter.write(summeryFileContent);
            myWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return SummaryFileHtml;
    }

    public static Instance ec2Builder(String instanceName, Ec2Client ec2) {
        String amiId = "ami-076515f20540e6e0b";

        List<Tag> tags = new ArrayList<>();
        tags.add(Tag.builder()
                .key("type")
                .value(instanceName)
                .build());

        tags.add(Tag.builder()
                .key("Name")
                .value(instanceName)
                .build());

        RunInstancesRequest runRequest = RunInstancesRequest.builder()
                .imageId(amiId)
                .instanceType(InstanceType.T2_MICRO)
                .userData(getEC2ManagerData(instanceName))
                .maxCount(1)
                .minCount(1)
                .tagSpecifications(TagSpecification.builder().tags(tags).resourceType("instance").build())
                .iamInstanceProfile(IamInstanceProfileSpecification.builder().name("ec2Role").build())
                .build();
        RunInstancesResponse response = null;
        try {
            response = ec2.runInstances(runRequest);
        } catch (Ec2Exception e) {
            System.err.println(e.getMessage());
        }
        System.out.println("Local-app created " + instanceName + " successfully");
        if (response == null) throw new AssertionError();
        return response.instances().get(0);
    }

    public static String createQueue(String queueName, SqsClient sqs) {
        try {
            CreateQueueRequest request = CreateQueueRequest.builder()
                    .queueName(queueName)
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

    public static void sqsRequest(SqsClient sqs, String messageBody, String queueUrl, Map<String, MessageAttributeValue> messageAttributes) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .delaySeconds(5)
                .messageAttributes(messageAttributes)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static void putFile(File filePath, String fileName, String bucketName, Boolean deleteOldFile, S3Client s3) {
        Boolean isFileExists = s3.listBuckets().buckets().contains(filePath);
        if (deleteOldFile && isFileExists) {
            s3.deleteObject(DeleteObjectRequest.builder().bucket(bucketName).key(fileName).build());
            System.out.println("Deleted old file: " + fileName);
        }
        try {
            s3.putObject(PutObjectRequest.builder().acl(ObjectCannedACL.PUBLIC_READ).bucket(bucketName).key(fileName)
                            .build(),
                    RequestBody.fromFile(filePath));
        } catch (
                Exception e) {
            System.out.println("couldn't put object" + e);
        }
    }

    private static void createBucket(String bucketName, Region region, S3Client s3) {
        for (Bucket bucket : s3.listBuckets().buckets()) {
            if (bucket.name().equals(bucketName)) {
                System.out.println("Using existing bucket: " + bucketName);
                return;
            }
        }
        s3.createBucket(CreateBucketRequest
                .builder()
                .bucket(bucketName)
                .createBucketConfiguration(
                        CreateBucketConfiguration.builder()
                                .build())
                .build());
        System.out.println("Local-app created new bucket: " + bucketName);
    }

    private static String getEC2ManagerData(String instaceType) {
        String userData = "";
        if (instaceType.equals(MANAGER_TYPE)) {
            userData = userData + "#!/bin/bash" + "\n";
            userData = userData + "aws s3 cp s3://" + JAR_BUCKET + "/manager.jar manager.jar" + "\n";
            userData = userData + "java -jar manager.jar" + "\n";
        }
        String base64UserData = null;
        try {
            base64UserData = new String(Base64.getEncoder().encode(userData.getBytes("UTF-8")), "UTF-8");
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
        return base64UserData;
    }

    private static Boolean isManagerRunning(Ec2Client ec2) {
        for (Reservation res : ec2.describeInstances().reservations()) {
            for (Instance instance : res.instances()) {
                for (Tag tag : instance.tags()) {
                    if (tag.value().equals(MANAGER_TYPE) && tag.key().equals("type")) {
                        int mode = instance.state().code();
                        if (mode == PENDING_INSTANCE || mode == RUNNING_INSTANCE) {
                            System.out.println("Manager is already in running/pending mode - No need to Create new one.");
                            return true;
                        }
                    }
                }
            }
        }
        return false;
    }
}