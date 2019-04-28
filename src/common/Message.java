package common;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;

import common.KVMessage;
import ecs.ECSNode;
import ecs.IECSNode;

public class Message implements KVMessage {
	private String key;
	private String value;
	private StatusType status;
	private List<ECSNode> metadata;

	public Message(String key, String value, StatusType status, List<ECSNode> md) {
		this.key = key;
		this.value = value;
		this.status = status;
		this.metadata = md;
	}

	@Override
	public String getKey() {
		return key;
	}

	@Override
	public String getValue() {
		return value;
	}

	@Override
	public StatusType getStatus() {
		return status;
	}

	public List<ECSNode> getMetadata() {
		return metadata;
	}
	
	public void setMetadata(List<ECSNode> metadata) {
		this.metadata = metadata;
	}
		
	@Override
	public IECSNode getResponsibleServer() {
		try {
      MessageDigest md5 = MessageDigest.getInstance("MD5");
      byte[] digestArr = md5.digest(key.getBytes());
      BigInteger keyHash = new BigInteger(1, digestArr);
      for (ECSNode node : metadata) {
        boolean isWrapAround = node.getHashStart().compareTo(node.getHashEnd()) > 0;
        if (isWrapAround) {
          if (keyHash.compareTo(node.getHashStart()) > 0) {
            return node;
          } else if (keyHash.compareTo(node.getHashEnd()) <= 0) {
            return node;
          }
        } else if (keyHash.compareTo(node.getHashStart()) > 0
            && keyHash.compareTo(node.getHashEnd()) <= 0) {
          return node;
        }
      }
    } catch (NoSuchAlgorithmException nsae) {
      nsae.printStackTrace();
    }

		return null;
	}	
}

