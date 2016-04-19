package at.grahsl.gab2016.eventhub;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class MessageEH implements Serializable {
	
	private static final long serialVersionUID = 3058803024082301987L;
	
	public final Date createdAt;
	public final long id;
	public final List<String> emojis;
	public final String text;
	
	public MessageEH(Date createdAt, long id, List<String> emojis,String text) {
		super();
		this.createdAt = createdAt;
		this.id = id;
		this.emojis = emojis;
		this.text = text;
	}

}
