package input;

import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Date;
import com.microsoft.windowsazure.services.table.client.TableServiceEntity;

public class LogEntity extends TableServiceEntity {

    private String level;
	private String message;
	private String associated;
	
    public LogEntity() {
        super();
    }

    public LogEntity(final String partitionKey, final String rowKey, final String level, final String message) {
        super();

        // properties
        this.partitionKey = partitionKey;
        this.rowKey = rowKey;
		this.level = level;
		this.message = message;

        // create an associated timestamp for coorelating logs
        Calendar calendar = Calendar.getInstance();
        Timestamp max = new Timestamp(new Date(Long.MAX_VALUE).getTime());
        calendar.set(Calendar.HOUR, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 0);
        this.associated = String.valueOf(max.getTime() - calendar.getTimeInMillis());

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

	public final String getAssociated() {
		return message;
	}

	public final void setAssociated(String associated) {
		this.associated = associated;
	}

}