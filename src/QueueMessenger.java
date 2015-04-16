
import java.util.*;
import java.util.Map.Entry;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.DeleteQueueRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

public class QueueMessenger {
	private AmazonSQS sqs;
	private String queueUrl;
	
	private static final String TweetQueue = "TweetQueue";

	public QueueMessenger() {
		// Get the credentials
		AWSCredentials credentials = null;
        try {
            credentials = new ProfileCredentialsProvider("default").getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException("Could not load credential profiles", e);
        }
        
        // Initialize SQS for USEast
        sqs = new AmazonSQSClient(credentials);
        Region usEast = Region.getRegion(Regions.US_EAST_1);
        sqs.setRegion(usEast);

        // Get the TweetQueue URL
        queueUrl = sqs.getQueueUrl(QueueMessenger.TweetQueue).getQueueUrl();
        System.out.println(queueUrl);
        
        // Set a test message
        // Create attributes that will hold Twitter ID
        System.out.println();
        System.out.println("Sending a message to MyQueue.\n");
        Map<String, MessageAttributeValue> msgAttributes = new HashMap<>();
        MessageAttributeValue val = new MessageAttributeValue().withDataType("Number").withStringValue("588717690140917760");
        System.out.println("Setting value " + val.getStringValue());
        msgAttributes.put("tweetId", val);
        for (String s : msgAttributes.keySet())
        	System.out.println(s);
        
        
        // Create message with text body
        SendMessageRequest request = new SendMessageRequest();
        request.withMessageBody("This is the body for a tweet.");
        request.withQueueUrl(queueUrl);
        request.withMessageAttributes(msgAttributes);
        sqs.sendMessage(request);
        System.out.println();
        
        // Let's get the test message back
        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("tweetId")).getMessages();
        for (Message message : messages) {
            System.out.println("  Message");
            System.out.println("    MessageId:     " + message.getMessageId());
            System.out.println("    ReceiptHandle: " + message.getReceiptHandle());
            System.out.println("    MD5OfBody:     " + message.getMD5OfBody());
            System.out.println("    Body:          " + message.getBody());
            
            Map<String, MessageAttributeValue> msgAts = message.getMessageAttributes();
            System.out.println("The tweet ID is " + msgAts.get("tweetId").getStringValue());
        }
        System.out.println();
        
        // Delete a message
        System.out.println("Deleting a message.\n");
        String messageRecieptHandle = messages.get(0).getReceiptHandle();
        sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
        System.out.println("Deletion completed");
	}
}
