package comp512st.paxos;

// Access to the GCL layer
import comp512.gcl.*;

import comp512.utils.*;

// Any other imports that you may need.
import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.*;

import javax.net.ssl.SNIHostName;

import java.net.UnknownHostException;

// ANY OTHER classes, etc., that you add must be private to this package and not visible to the application layer.

// extend / implement whatever interface, etc. as required.
// NO OTHER public members / methods allowed. broadcastTOMsg, acceptTOMsg, and shutdownPaxos must be the only visible methods to the application layer.
//		You should also not change the signature of these methods (arguments and return value) other aspects maybe changed with reasonable design needs.
public class Paxos {
	GCL gcl;
	FailCheck failCheck;
	Logger logger;
	int numProcesses;
	private int ballotSequence = 0;
	private int maxBallotID = 0;
	private int sequenceNumber = 0;
	private char acceptedValue;
	private String myProcess;
	private char promiseValue;
	private int playerNumMove;
	private Object[] confirmMove;
	private ArrayList<GCMessage> proposeMessages;
	private int proposeCount;
	private int lastProposeCount;
	private ArrayList<GCMessage> promiseMessages;
	private int promiseCount;
	private int lastPromiseCount;
	private ArrayList<GCMessage> acknowledgeMessages;
	private int acknowledgeCount;
	private int lastAcknowledgeCount;
	private ArrayList<GCMessage> acceptMessages;
	private int acceptCount;
	private int lastAcceptCount;
	private ArrayList<GCMessage> confirmMessages;
	private int confirmCount;
	private int lastConfirmCount;
	private int sequenceDelievered;
	private ArrayList<Object[]> playerMoves;
	private Boolean sendConfirm;
	private int acceptedBallotId;
	private int overallSequence;

	private String[] otherProcesses;

	private final Object lock = new Object();

	public Paxos(String myProcess, String[] allGroupProcesses, Logger logger, FailCheck failCheck)
			throws IOException, UnknownHostException {
		// Rember to call the failCheck.checkFailure(..) with appropriate arguments
		// throughout your Paxos code to force fail points if necessary.
		this.failCheck = failCheck;
		this.logger = logger;

		// Initialize the GCL communication system as well as anything else you need to.
		this.gcl = new GCL(myProcess, allGroupProcesses, null, logger);

		this.numProcesses = allGroupProcesses.length;
		this.acceptedValue = '\0';
		this.myProcess = myProcess;
		this.promiseValue = '\0';

		this.proposeMessages = new ArrayList<GCMessage>();
		this.promiseMessages = new ArrayList<GCMessage>();
		this.acknowledgeMessages = new ArrayList<GCMessage>();
		this.acceptMessages = new ArrayList<GCMessage>();
		this.confirmMessages = new ArrayList<GCMessage>();
		this.proposeCount = -1;
		this.promiseCount = -1;
		this.acknowledgeCount = -1;
		this.acceptCount = -1;
		this.confirmCount = -1;
		this.lastProposeCount = -1;
		this.lastPromiseCount = -1;
		this.lastAcknowledgeCount = -1;
		this.lastAcceptCount = -1;
		this.lastConfirmCount = -1;
		this.playerNumMove = 0;
		this.sendConfirm = false;
		this.sequenceDelievered = 0;
		this.confirmMove = new Object[]{0, "\0"};
		this.playerMoves = new ArrayList<Object[]>();
		this.acceptedBallotId = 0;
		this.overallSequence = 0;

		this.otherProcesses = allGroupProcesses;
		// int j = 0;
		// for(int i = 0; i < this.numProcesses; i++) {
		// 	if(!allGroupProcesses[i].equals(myProcess)) {
		// 		this.otherProcesses[j++] = allGroupProcesses[i];
		// 	}
		// }
		
	}

	// This is what the application layer is going to call to send a message/value,
	// such as the player and the move
	public void broadcastTOMsg(Object[] val) throws InterruptedException {
		// This is just a place holder.
		// Extend this to build whatever Paxos logic you need to make sure the messaging
		// system is total order.
		// Here you will have to ensure that the CALL BLOCKS, and is returned ONLY when
		// a majority (and immediately upon majority) of processes have accepted the
		// value.

		tryPaxos(val);


	}

	// This is what the application layer is calling to figure out what is the next
	// message in the total order.
	// Messages delivered in ALL the processes in the group should deliver this in
	// the same order.
	//
	// Note: changed return from Object to Object[] because that is what is expected at TreasureIslandApp line 42.
	// Don't think that's allowed, so will need to be dealt with later
	public Object[] acceptTOMsg() throws InterruptedException {

		if (Thread.interrupted()) {
			while (this.sequenceDelievered  < this.playerMoves.size() + 1) {
				System.out.println("here");
				this.sequenceDelievered++;
				return this.playerMoves.get(sequenceDelievered); 
			} 
			return new Object[]{'n', 'n'};
		}
		//we're gonna use this new way, basically we poll the accept messages every time and check we message has arrived,
		//this way, we are able to actually reject certain propose messages and we dont get stuck waiting for a process that might fail before sending the confirm.
		if(this.sendConfirm) {
			this.sequenceDelievered++;
			if(this.sequenceDelievered  < this.playerMoves.size()) {
				return this.playerMoves.get(sequenceDelievered);
			} else {
				return new Object[]{'n', 'n'};
			}
		} else {
			GCMessage acceptorMessage = getAcceptorMessages();
			Object[] messageVal = (Object[]) acceptorMessage.val;
			String type = (String) messageVal[0];
			switch (type) {
				case "propose":
					promise(acceptorMessage);
					return acceptTOMsg();
				case "accept":
					sendAcknowledgement(acceptorMessage);
					// Thread.sleep(100);
					return acceptTOMsg();
				case "confirm":
					Object[] playerMove = acceptConfirm(acceptorMessage);
					this.acceptedValue = '\0';
					if((Character)playerMove[1] != 'n') {
						logger.info("About to return playerMove " + playerMove.toString());
						return playerMove;
					}  else {
						return acceptTOMsg();
					}
			}
		}

		return new Object[]{null, 'n'};
	}

	// Add any of your own shutdown code into this method.
	public void shutdownPaxos() {
		if(this.sequenceDelievered  < this.playerMoves.size()) {
			this.sendConfirm = true;	
		}
		gcl.shutdownGCL();
	}

	// Used to determine if the proposer will be elected based on ballotID
	private int propose(Object val) throws InterruptedException {
		int majority = this.numProcesses / 2;
		int votes = 0;
		int processId = (Integer) val;
		int ballotId = this.ballotSequence + processId;
		int totalVotes = 0;
		int denies = 0;

		// send propose message
		gcl.broadcastMsg(new Object[] {"propose", ballotId});
		Thread.sleep(100);

		//to be invoked immediately AFTER a process sends out its proposal to become a leader.
		failCheck.checkFailure(FailCheck.FailureType.AFTERSENDPROPOSE);

		// wait until majority has returned a promise or denial
		while (votes <= majority && denies <= majority) {
			GCMessage gcmsg = getPromise();
			Object[] messageVal = (Object[]) gcmsg.val;
			if (messageVal.length > 2 ) {
				if((Character) messageVal[2] != '\0') {
					this.promiseValue = (Character) messageVal[2];
					this.playerNumMove = (Integer) messageVal[3];
				}
				votes++;
			} else {
				denies++;
			}
			totalVotes++;
		}
		// return -1 if majority of process rejected this ballot id
		if(denies > majority) {
			logger.info("Ballot id " + ballotId + " was rejected");
			ballotId = -1;
		}
		//to be invoked immediately AFTER a process sees that it has been accepted by the majority as the leader.
		else {
			logger.info("Ballot id " + ballotId + " got a majority of promises");
			failCheck.checkFailure(FailCheck.FailureType.AFTERBECOMINGLEADER);
		}
		return ballotId;
	}

	// helper function to send propose message until it gets promise from majority with higher ballotSequence every retry
	private int proposeUntilAccepted(Object val) throws InterruptedException {
		int ballotId = -1;
		while(ballotId == -1) {
			ballotId = propose(val);
			logger.info("I am proposing a new ballotId");
			if(ballotId == -1) {
				this.ballotSequence += 10;
				Thread.sleep(500);
			}
		}
		return ballotId;
	}

	// send the accept? move message
	// the sent move will correspond to any values returned
	// by processes that have already accepted a value, if not send the requested move
	private Object[] sendAcceptMove(int ballotId, char move) throws InterruptedException {
		char moveToSend = move;
		if(this.promiseValue != '\0') {
			moveToSend = this.promiseValue;
			this.sequenceNumber++;
			logger.info("About to sendAcceptMove with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
			gcl.broadcastMsg(new Object[]{"accept", ballotId, moveToSend, (this.sequenceNumber)});
			Thread.sleep(100);
			this.promiseValue = '\0';
			int player = this.playerNumMove;
			return new Object[]{moveToSend, 'r', player};
		} else {
			this.sequenceNumber++;
			logger.info("About to sendAcceptMove with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
			gcl.broadcastMsg(new Object[]{"accept", ballotId, moveToSend, (this.sequenceNumber)});
			Thread.sleep(100);
			return new Object[]{moveToSend, '\0'};
		}
	}

	private Boolean[] confirmation() throws InterruptedException
	{
		int majority = this.numProcesses / 2;
		int votes = 0;
		int totalVotes = 0;
		int denies = 0;
		Boolean newSequence = false;
		
		while (votes <= majority && denies <= majority) {
			GCMessage gcmsg = getAcknowledge();
			Object[] messageVal = (Object[]) gcmsg.val;
			if(((String)messageVal[1]).equals("Accept")) {
				if(messageVal.length > 2) {
					this.sequenceNumber = (Integer)messageVal[2];
					newSequence = true;
				}
				votes++;
			} else {
				if(messageVal.length > 2) {
					this.sequenceNumber = (Integer)messageVal[2];
					newSequence = true;
				}
				denies++;
			}
			totalVotes++;
		}

		if(denies > majority) return new Boolean[]{false, false};
		else {
			if(newSequence) {
				failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
				return new Boolean[]{true, true};
			} else {
				//to be invoked immediately by the process once a majority has accepted it’s proposed value.
				failCheck.checkFailure(FailCheck.FailureType.AFTERVALUEACCEPT);
				return new Boolean[]{true, false};
			}
		}

	}

	private void tryPaxos(Object[] val) throws InterruptedException
	{
		Boolean[] success = new Boolean[]{false, true};

		int ballotId = 0;
		char move = (Character) val[1];
		Object[] moveToSendArray = {'\0', '\0', 0};
		int playerNum = getPlayerNum(myProcess);

		while(!success[0]) {
				// propose ballotSequence until promised by majority
				ballotId = proposeUntilAccepted(val[0]);

				while (success[1]) {
					Thread.sleep(1000);
					moveToSendArray = sendAcceptMove(ballotId, move);
					success = confirmation();
				}
				this.acceptedValue = '\0';
				if(success[0]) {
					if((Character)moveToSendArray[1] != '\0') {
						success[0] = false;
						success[1] = true;
						gcl.broadcastMsg(new Object[]{"confirm", moveToSendArray[0], (Integer)moveToSendArray[2], this.sequenceNumber});
						Thread.sleep(100);
					} else {
						success[1] = true;
						gcl.broadcastMsg(new Object[]{"confirm", moveToSendArray[0], playerNum, this.sequenceNumber});
						Thread.sleep(100);
					}
				} else {
					success[1] = true;
				}
			}
	}

	// Acknowledge the leader
	private void promise(GCMessage gcmsg) throws InterruptedException
	{
		// GCMessage gcmsg = getPropose();
		//to be invoked immediately when a process receives a propose message.
		failCheck.checkFailure(FailCheck.FailureType.RECEIVEPROPOSE);
		Object[] messageVal = (Object[]) gcmsg.val;
		int ballotId = (Integer) messageVal[1];
		String sendProcess = gcmsg.senderProcess;

		if (ballotId < this.maxBallotID) {
			logger.info("maxBallotID is " + maxBallotID + " and ballotId is " + ballotId + " so proposal is denied");
			gcl.sendMsg(new Object[]{"promise", this.maxBallotID}, sendProcess);
			Thread.sleep(100);
			//to be invoked immediately AFTER a process sends out its vote for leader election.
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
		} else {
			logger.info("maxBallotID is " + maxBallotID + " and ballotId is " + ballotId + " so proposal is promised");
			this.maxBallotID = ballotId;
			this.ballotSequence = (maxBallotID / 10 + 1) * 10;
			int playerNum = getPlayerNum(myProcess);
			gcl.sendMsg(new Object[]{"promise", ballotId, '\0', playerNum}, sendProcess);
			Thread.sleep(100);
			//to be invoked immediately AFTER a process sends out its vote for leader election.
			failCheck.checkFailure(FailCheck.FailureType.AFTERSENDVOTE);
		}
	}

	private void sendAcknowledgement(GCMessage gcmsg) throws InterruptedException
	{
		// GCMessage gcmsg = getAccept();
		Object[] messageVal = (Object[]) gcmsg.val;
		int ballotId = (Integer) messageVal[1];
		char move = (Character) messageVal[2];
		int proposedSequence = (Integer) messageVal[3];

		String sendProcess = gcmsg.senderProcess;
		Thread.sleep(100);
	
		if (ballotId >= this.acceptedBallotId) {
			this.acceptedBallotId = ballotId;
			this.acceptedValue = move;
			if(proposedSequence > getSequence()) {
				writeSequence(proposedSequence);
				this.sequenceNumber = proposedSequence;
				logger.info("About to sendAcknowledgement with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
				gcl.sendMsg(new Object[]{"ack", "Accept"}, sendProcess);
			} else {
				logger.info("About to sendAcknowledgement with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
				gcl.sendMsg(new Object[]{"ack", "Accept", this.overallSequence}, sendProcess);
			}
		} else {
			if(proposedSequence == (this.overallSequence+1)) {
				logger.info("About to deny sendAcknowledgement with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
				gcl.sendMsg(new Object[]{"ack", "Deny"}, sendProcess);
			} else {
				logger.info("About to deny sendAcknowledgement with ballotId " + ballotId + " and sequenceNumber " + this.sequenceNumber);
				gcl.sendMsg(new Object[]{"ack", "Deny", proposedSequence}, sendProcess);
			}
		}
		Thread.sleep(100);

	}

	private Object[] acceptConfirm(GCMessage gcmsg) throws InterruptedException
	{
		// GCMessage gcmsg = getConfirm();
		Object[] messageVal = (Object[]) gcmsg.val;
		Object[] confirmVal = new Object[]{messageVal[2], messageVal[1]};
		int sequenceNumberReceived = (Integer)messageVal[3];
		int nextSequence = this.sequenceDelievered + 1;
		System.out.println("sequence number received: " + sequenceNumberReceived);
		bufferConfirmMessages(confirmVal, sequenceNumberReceived);
		Object[] playerMove = this.playerMoves.get(sequenceDelievered);
		this.sequenceDelievered++;
		return new Object[]{playerMove[1], playerMove[2]};
		// if(sequenceNumberReceived == (this.sequenceDelievered)) {
		// 	for(int i = 0; i < this.playerMoves.size(); i++) {
		// 		if((Integer)this.playerMoves.get(i)[0] == nextSequence) {
		// 			this.sendConfirm = true;
		// 		}
		// 	}
		// 	return confirmVal;
		// } else if(sequenceNumberReceived > (this.sequenceDelievered + 1)) {
		// 	bufferConfirmMessages(confirmVal, sequenceNumberReceived);
		// }
		// return new Object[]{null, 'n'};
	}

	private int getPlayerNum(String sender) {
		int playerNum = 0;
		for(int i = 0; i < this.numProcesses; i++) {
			if(this.otherProcesses[i].equals(sender)) {
				playerNum = i + 1;
			}
		}
		return playerNum;
	}

	//TODO: debug this message buffer system.

	//I think the current problem is that multiple places call readMessagesToBuffer. The first call happens in 
	//getPromise as soon as the app is launched. When a char is input, another call happens in getPropose. 
	//The propose message is read from the GCL; this happens in the first call to readMessagesToBuffer, meaning in
	//getPromise. PromiseCount is incremented, but because the call in getPropose never concludes, the while loop
	//is never exited and the process stalls.
	//
	//I'm still trying to figure out what the best fix is. I think there could be a solution where the reader is 
	//constantly running in a single call. GetPromise, getPropose, etc. wouldn't call the buffer function as it
	//would already be running, they would just wait for the counts to change and go based off of that
	
	// yeah if we want to do this, we can just have reading the messages
	// i jsut managed to fix something because i put getPromise and getPropose in the oposite places, making some progress
	// i think we can send it to all processes' except for the process that is the leader because now it is sending it to itself and thats what is confusing it

	//also we have to do something with the logger (use it as memory) but im not sure how to use it

	// since multiple messages can come in at different times, we don't want to be reading a 
	// promise message when we are trying to read an acknowledgement message so we buffer them with this
	private void readMessagesToBuffer() throws InterruptedException
	{
		GCMessage gcmsg = gcl.readGCMessage();
		Object[] messageVal = (Object[]) gcmsg.val;
		String msgType = (String) messageVal[0];
		switch (msgType) {
			case "propose":
				this.proposeMessages.add(gcmsg);
				this.proposeCount++;
				break;
			case "promise":
				this.promiseMessages.add(gcmsg);
				this.promiseCount++;
				break;
			case "ack":
				this.acknowledgeMessages.add(gcmsg);
				this.acknowledgeCount++;
				break;
			case "accept":
				this.acceptMessages.add(gcmsg);
				this.acceptCount++;
				break;
			case "confirm":
				this.confirmMessages.add(gcmsg);
				this.confirmCount++;
				break;
				
		}
	}

	// uses to replace gcl.readGCMessage() in propose
	// if there a propose that we haven't read yet, read it
	// else try to read messages until propose message arrives
	private GCMessage getPropose() throws InterruptedException
	{
		int count = this.proposeCount;
		if(this.lastProposeCount < this.proposeCount) {
			this.lastProposeCount++;
			return this.proposeMessages.get(this.lastProposeCount);
		} else {
			while(count == this.proposeCount) {
				readMessagesToBuffer();
			}
			this.lastProposeCount++;
			return this.proposeMessages.get(this.proposeCount);
		}
	}

	// uses to replace gcl.readGCMessage() in promise
	private GCMessage getPromise() throws InterruptedException
	{
		int count = this.promiseCount;
		if(this.lastPromiseCount < this.promiseCount) {
			this.lastPromiseCount++;
			return this.promiseMessages.get(this.lastPromiseCount);
		} else {
			while(count == this.promiseCount) {
				readMessagesToBuffer();
			}
			this.lastPromiseCount++;
			return this.promiseMessages.get(this.promiseCount);
		}
	}

	// uses to replace gcl.readGCMessage() in Acknowledge
	private GCMessage getAcknowledge() throws InterruptedException
	{
		int count = this.acknowledgeCount;
		if(this.lastAcknowledgeCount < this.acknowledgeCount) {
			this.lastAcknowledgeCount++;
			return this.acknowledgeMessages.get(this.lastAcknowledgeCount);
		} else {
			while(count == this.acknowledgeCount) {
				readMessagesToBuffer();
			}
			this.lastAcknowledgeCount++;
			return this.acknowledgeMessages.get(this.acknowledgeCount);
		}
	}

	// uses to replace gcl.readGCMessage() in Accept
	private GCMessage getAccept() throws InterruptedException
	{
		int count = this.acceptCount;
		if(this.lastAcceptCount < this.acceptCount) {
			this.lastAcceptCount++;
			return this.acceptMessages.get(this.lastAcceptCount);
		} else {
			while(count == this.acceptCount) {
				readMessagesToBuffer();
			}
			this.lastAcceptCount++;
			return this.acceptMessages.get(this.acceptCount);
		}
	}

	// uses to replace gcl.readGCMessage() in Confirm
	private GCMessage getConfirm() throws InterruptedException
	{
		int count = this.confirmCount;
		if(this.lastConfirmCount < this.confirmCount) {
			this.lastConfirmCount++;
			return this.confirmMessages.get(this.lastConfirmCount);
		} else {
			while(count == this.confirmCount) {
				readMessagesToBuffer();
			}
			this.lastConfirmCount++;
			return this.confirmMessages.get(this.confirmCount);
		}
	}

	private GCMessage getAcceptorMessages() throws InterruptedException
	{
		if(this.lastConfirmCount < this.confirmCount) {
			this.lastConfirmCount++;
			return this.confirmMessages.get(this.lastConfirmCount);
		} else if(this.lastAcceptCount < this.acceptCount) {
			this.lastAcceptCount++;
			return this.acceptMessages.get(this.lastAcceptCount);
		} else if(this.lastProposeCount < this.proposeCount) {
			this.lastProposeCount++;
			return this.proposeMessages.get(this.lastProposeCount);
		} else {
			while(this.lastProposeCount == this.proposeCount && this.lastAcceptCount == this.acceptCount && this.lastConfirmCount == this.confirmCount) {
				readMessagesToBuffer();
			}
			if(this.lastProposeCount != this.proposeCount) {
				this.lastProposeCount++;
				return this.proposeMessages.get(this.lastProposeCount);
			} else if(this.lastAcceptCount != this.acceptCount) {
				this.lastAcceptCount++;
				return this.acceptMessages.get(this.lastAcceptCount);
			} else if(this.lastConfirmCount != this.confirmCount) {
				this.lastConfirmCount++;
				return this.confirmMessages.get(this.lastConfirmCount);
			}
		}
		return null;
	}

	private void bufferConfirmMessages(Object[] confirmMessage, int sequence) 
	{
		Object[] playerMove = new Object[]{sequence, confirmMessage[0], confirmMessage[1]};
		int initialSize = this.playerMoves.size();
		if(sequenceDelievered==0) {
			this.playerMoves.add(playerMove);
		} else {
			for(int i = this.sequenceDelievered; i < this.playerMoves.size(); i++) {
				if(sequence < (Integer)this.playerMoves.get(i)[0]) {
					this.playerMoves.add(i, playerMove);
				}
			}
			if(this.playerMoves.size() == initialSize) {
				this.playerMoves.add(playerMove);
			}
		}
	}

	private void writeSequence(int sequence) {
		synchronized (lock) {
			overallSequence = sequence;
		}
	}

	private int getSequence() {
		synchronized (lock) {
			return overallSequence;
		}
	} 

}
