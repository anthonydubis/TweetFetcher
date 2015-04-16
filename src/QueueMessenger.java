
import java.util.*;

import twitter4j.Status;

import com.amazonaws.AmazonClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.sqs.model.MessageAttributeValue;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClient;
import com.amazonaws.services.sqs.model.DeleteMessageRequest;
import com.amazonaws.services.sqs.model.Message;
import com.amazonaws.services.sqs.model.ReceiveMessageRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequest;
import com.amazonaws.services.sqs.model.SendMessageBatchRequestEntry;
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
        
//        while (true) {
//        	ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
//            List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("TweetId")).getMessages();
//            if (messages.isEmpty()) {
//            	break;
//            }
//            
//            for (Message message : messages) {
//            	System.out.println("Deleting message");
//            	String messageRecieptHandle = message.getReceiptHandle();
//                sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
//            }
//        }
        
//        // Let's get the test message back
//        ReceiveMessageRequest receiveMessageRequest = new ReceiveMessageRequest(queueUrl);
//        List<Message> messages = sqs.receiveMessage(receiveMessageRequest.withMessageAttributeNames("TweetId")).getMessages();
//        for (Message message : messages) {
//            System.out.println("    Body:          " + message.getBody());
//            Map<String, MessageAttributeValue> msgAts = message.getMessageAttributes();
//            System.out.println("The tweet ID is " + msgAts.get("TweetId").getStringValue());
//        }
//        System.out.println();
//        
//        // Delete a message
//        for (Message message : messages) {
//        	String messageRecieptHandle = message.getReceiptHandle();
//            sqs.deleteMessage(new DeleteMessageRequest(queueUrl, messageRecieptHandle));
//        }
//        System.out.println("Deletion completed");
	}
	
	private SendMessageBatchRequestEntry getBatchSendRequestForTweet(Status status) {
		SendMessageBatchRequestEntry entry = new SendMessageBatchRequestEntry();
		entry.withId("" + status.getId());
		entry.withMessageBody(status.getText());
		return entry;
		
//	    Map<String, MessageAttributeValue> msgAttributes = new HashMap<>();
//	    MessageAttributeValue val = new MessageAttributeValue().withDataType("Number");
//	    val.withStringValue("" + status.getId());
//	    msgAttributes.put("TweetId", val);        
//	      
//	    // Create message with text body
//      SendMessageRequest request = new SendMessageRequest();
//	    request.withMessageBody(status.getText());
//	    request.withQueueUrl(queueUrl);
//      request.withMessageAttributes(msgAttributes);
//      sqs.sendMessage(request);
	}
	
	/*
	 * Sends off the batch entries
	 * Assumes there is at least one in the list
	 */
	private void queueBatchEntries(List<SendMessageBatchRequestEntry> entries) {
		SendMessageBatchRequest request = new SendMessageBatchRequest(queueUrl, entries);
		sqs.sendMessageBatch(request);
	}
	
	void queueTweets(Map<String, List<Status>> map) {
		List<SendMessageBatchRequestEntry> entries = new ArrayList<SendMessageBatchRequestEntry>();
		for (String key : map.keySet()) {
			List<Status> statuses = map.get(key);
			for (Status status : statuses) {
				if (entries.size() == 10) {
					// Send off the batch and initialize a new list
					queueBatchEntries(entries);
					entries = new ArrayList<SendMessageBatchRequestEntry>();
				}
				entries.add(getBatchSendRequestForTweet(status));
			}
		}
		if (entries.size() > 0) {
			queueBatchEntries(entries);
		}
	}
}
