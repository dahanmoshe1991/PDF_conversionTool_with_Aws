import java.awt.image.BufferedImage;
import java.io.*;
import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.pdfbox.pdmodel.PDDocument;
import org.apache.pdfbox.rendering.PDFRenderer;
import org.apache.pdfbox.text.PDFTextStripper;
import org.apache.pdfbox.tools.PDFText2HTML;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;

import javax.imageio.ImageIO;


public class worker {
    private static final int firstPage = 1;
    private static final Region REGION = Region.US_EAST_1;

    private static final String WORKERS_QUEUE = "workerQueue123";
    private static final String DONE_WORKERS_QUEUE = "doneworkersqueue123";

    private static final String DONE_PDF_TASK_BUCKET = "donepdftaskbucket123";

    public static void main(String[] args) {
        // -------- S3 --------
        S3Client s3 = S3Client.builder().region(REGION).build();

        // -------- SQS --------
        SqsClient sqs = SqsClient.builder().region(REGION).build();
        String workersQueueUrl = getQueueURL(WORKERS_QUEUE, sqs);
        String doneWorkersQueueUrl = getQueueURL(DONE_WORKERS_QUEUE, sqs);

        runWorker(workersQueueUrl, doneWorkersQueueUrl, s3, sqs);
    }

    public static void runWorker(String workersQueueUrl, String doneWorkersQueueUrl, S3Client s3, SqsClient sqs) {
        List<Message> taskList;
        while (true) {
            taskList = ReceiveMessage(workersQueueUrl, sqs);
            if (taskList.size() != 0) {
                Message task = taskList.get(0);
                Map<String, MessageAttributeValue> taskAttributes = task.messageAttributes();
                String toDo = taskAttributes.get("toDo").stringValue();
                String url = taskAttributes.get("url").stringValue();
                String appId = taskAttributes.get("Appid").stringValue();
                String fileName = getFileNameFromURL(url);
                String donePDFTaskBucketName = DONE_PDF_TASK_BUCKET + appId;
                try {
                    File pdfFileToConvert = preProcessMessage(taskAttributes.get("url").stringValue());
                    File convertedFile = getConvertedFile(pdfFileToConvert, toDo);
                    String convertedFileName = convertedFile.getName();
                    createBucket(donePDFTaskBucketName, REGION, s3);
                    putFile(convertedFile, convertedFileName, donePDFTaskBucketName, s3);
                    createDoneMessage(sqs, "done PDF task", doneWorkersQueueUrl, appId, donePDFTaskBucketName, url, toDo, convertedFileName, "success");
                    System.out.println("Worker done single pdf task");
                } catch (Exception ex) {
                    createDoneMessage(sqs, "pdf error", doneWorkersQueueUrl, appId, donePDFTaskBucketName, url, toDo, "pdf-error", "Exception: " + ex.getMessage());
                    System.out.println("Worker done single pdf task");
                }
                try {
                    sqs.deleteMessage(DeleteMessageRequest.builder().queueUrl(workersQueueUrl).receiptHandle(task.receiptHandle()).build());
                } catch (Exception ex) {
                    System.out.println("Couldn't delete pdf task");
                }
            }
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
        System.out.println("Worker created new bucket: " + bucketName);
    }

    private static void createDoneMessage(SqsClient sqs, String messageBody, String doneWorkersQueueUrl, String appId, String donePDFTaskBucketName, String url, String toDo, String fileName, String exceptionMessage) {
        Map<String, MessageAttributeValue> messageAttributes = new HashMap<>();
        messageAttributes.put("type", MessageAttributeValue.builder().dataType("String").stringValue(messageBody).build());
        messageAttributes.put("Appid", MessageAttributeValue.builder().dataType("String").stringValue(appId).build());
        messageAttributes.put("toDo", MessageAttributeValue.builder().dataType("String").stringValue(toDo).build());
        messageAttributes.put("bucketName", MessageAttributeValue.builder().dataType("String").stringValue(donePDFTaskBucketName).build());
        messageAttributes.put("url", MessageAttributeValue.builder().dataType("String").stringValue(url).build());
        messageAttributes.put("outputFileUrl", MessageAttributeValue.builder().dataType("String").stringValue("https://" + donePDFTaskBucketName + ".s3.amazonaws.com/" + fileName).build());
        messageAttributes.put("exceptionMessage", MessageAttributeValue.builder().dataType("String").stringValue(exceptionMessage).build());
        sqsRequest(sqs, messageBody, doneWorkersQueueUrl, messageAttributes);
    }

    private static void sqsRequest(SqsClient sqs, String messageBody, String queueUrl, Map<String, MessageAttributeValue> messageAttributes) {
        SendMessageRequest send_msg_request = SendMessageRequest.builder()
                .queueUrl(queueUrl)
                .messageBody(messageBody)
                .delaySeconds(5)
                .messageAttributes(messageAttributes)
                .build();
        sqs.sendMessage(send_msg_request);
    }

    private static String getFileNameFromURL(String url) {
        String[] splitedURL = url.split("/");
        return splitedURL[splitedURL.length - 1];
    }

    private static void putFile(File file, String fileName, String bucketName, S3Client s3) {
        s3.putObject(PutObjectRequest.builder().bucket(bucketName).key(fileName)
                        .build(),
                RequestBody.fromFile(file));
    }

    private static List<Message> ReceiveMessage(String queueURL, SqsClient sqs) {
        List<Message> messageList = new ArrayList<>();
        try {
            messageList = sqs.receiveMessage(ReceiveMessageRequest.builder().waitTimeSeconds(4).visibilityTimeout(150)
                    .maxNumberOfMessages(1).messageAttributeNames("All").queueUrl(queueURL).build()).messages();
        } catch (Exception ex) {
            System.out.println("Worker exception in ReceiveMessage function: " + ex);
        }
        return messageList;
    }

    private static String getQueueURL(String queueName, SqsClient sqs) {
        GetQueueUrlRequest getQueueRequest = GetQueueUrlRequest.builder()
                .queueName(queueName)
                .build();
        return sqs.getQueueUrl(getQueueRequest).queueUrl();
    }

    private static File getConvertedFile(File pdfFileToConvert, String toDo) throws IOException {
        File convertedFile = null;
        if ("ToHTML".equals(toDo)) {
            convertedFile = toHTML(pdfFileToConvert);
        } else if ("ToText".equals(toDo)) {
            convertedFile = toText(pdfFileToConvert);
        } else if ("ToImage".equals(toDo)) {
            convertedFile = toPNG(pdfFileToConvert);
        }
        return convertedFile;
    }

    private static File toText(File pdfFileToConvert) throws IOException {
        File outputTextFile = null;
        PrintWriter printer;
        String reader;
        PDDocument currentPDFFile = PDDocument.load(pdfFileToConvert);
        PDFTextStripper pdfStrip = new PDFTextStripper();
        pdfStrip.setEndPage(1);
        reader = pdfStrip.getText(currentPDFFile);
        currentPDFFile.close();
        printer = new PrintWriter(pdfFileToConvert.getAbsolutePath().substring(0, pdfFileToConvert.getAbsolutePath().length() - 4) + ".txt");
        printer.println(reader);
        printer.close();
        outputTextFile = new File(pdfFileToConvert.getAbsolutePath().substring(0, pdfFileToConvert.getAbsolutePath().length() - 4) + ".txt");
        return outputTextFile;
    }

    private static File toHTML(File pdfFileToConvert) throws IOException {
        String path = pdfFileToConvert.getAbsolutePath().substring(0, pdfFileToConvert.getAbsolutePath().length() - 4) + ".html";
        File outputHTMLFile = new File(path);
        PDDocument currecntPDFFile = PDDocument.load(pdfFileToConvert);
        PDFText2HTML converter = new PDFText2HTML();
        Writer writeToHTMLFile = new BufferedWriter(new FileWriter(path));
        converter.setStartPage(firstPage);
        converter.setEndPage(firstPage);
        writeToHTMLFile.write(converter.getText(currecntPDFFile));
        writeToHTMLFile.close();
        currecntPDFFile.close();
        return outputHTMLFile;
    }

    private static File toPNG(File pdfFileToConvert) throws IOException {
        File file = new File(pdfFileToConvert.getAbsolutePath());
        PDDocument document;
        document = PDDocument.load(file);
        PDFRenderer renderer = new PDFRenderer(document);
        BufferedImage image = renderer.renderImage(0, 4);
        File outputFile = outputFile = new File(pdfFileToConvert.getAbsolutePath().substring(0, pdfFileToConvert.getAbsolutePath().length() - 4) + ".png");
        ImageIO.write(image, "png", outputFile);
        document.close();
        document.close();
        return outputFile;
    }

    private static File preProcessMessage(String url) throws IOException {
        String fileName = url.substring(url.lastIndexOf('/') + 1);
        File fileToConvert = new File(fileName);
        int readTil;
        URL urlToProcess = new URL(url);
        InputStream inputStream = urlToProcess.openStream();
        OutputStream outputStream = new FileOutputStream(fileToConvert);
        byte[] byteReader = new byte[2048 * 2];
        while ((readTil = inputStream.read(byteReader)) != -1) {
            outputStream.write(byteReader, 0, readTil);
        }
        inputStream.close();
        outputStream.close();
        return fileToConvert;
    }
}