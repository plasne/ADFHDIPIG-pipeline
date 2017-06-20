package input;

import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class LogEntity extends TableServiceEntity {

	private String message;
	
	private String level;
	
    public LogEntity() {
        super();
    }

    public LogEntity(final String partitionKey, final String rowKey, final String level, final String message) {
        super();
        this.partitionKey = partitionKey;
        this.rowKey = rowKey;
		this.level = level;
		this.message = message;
	}

	public final String getLevel() {
		return level;
	}

	public final void setLevel(String level) {
		this.level = level;
	}

	public final String getMessage() {
		return message;
	}

	public final void setMessage(String message) {
		this.message = message;
	}

}