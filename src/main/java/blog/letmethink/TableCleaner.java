package blog.letmethink;

import java.util.Iterator;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.dynamodbv2.document.spec.ScanSpec;
import com.amazonaws.services.dynamodbv2.document.ItemCollection;
import com.amazonaws.services.dynamodbv2.document.ScanOutcome;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PrimaryKey;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;

/**
 * Truncates table, handy in testing and debugging.
 * Credentials are read from ~/.aws/credentials.
 * See <a href="http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html">the documentation</a>
 * for more details.
 */
public class TableCleaner {
    private final DynamoDB db;

    public TableCleaner(DynamoDB db) {
        this.db = db;
    }

    public static void run(String[] args) {
        if (args.length != 1) {
            System.err.print("Usage: java TableCleaner <table:partition_key[:range_key]>\n");
            System.exit(1);
        }
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.US_WEST_2)
                .build();
        final var db = new DynamoDB(client);
        new TableCleaner(db).truncateTable(args[0]);
    }

    public void truncateTable(String tableSpec) {
        try {
            String[] tokens = tableSpec.split(":");
            if (tokens.length == 2) {
                truncateTable(tokens[0], tokens[1]);
            } else if (tokens.length == 3) {
                truncateTable(tokens[0], tokens[1], tokens[2]);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void truncateTable(String tableName, String hashKeyName, String rangeKeyName) throws Exception {
        Table table = db.getTable(tableName);
        ScanSpec spec = new ScanSpec();
        ItemCollection<ScanOutcome> items = table.scan(spec);
        Iterator<Item> it = items.iterator();
        while (it.hasNext()) {
            Item item = it.next();
            String hashKey = item.getString(hashKeyName);
            String rangeKey = item.getString(rangeKeyName);
            PrimaryKey key = new PrimaryKey( hashKeyName, hashKey, rangeKeyName, rangeKey);
            table.deleteItem(key);
            System.out.printf("Deleted item with key: <%s, %s>\n", hashKey, rangeKey);
        }
    }

    private void truncateTable(String tableName, String hashKeyName) throws Exception {
        Table table = db.getTable(tableName);
        ScanSpec spec = new ScanSpec();
        ItemCollection<ScanOutcome> items = table.scan(spec);
        Iterator<Item> it = items.iterator();
        while (it.hasNext()) {
            Item item = it.next();
            String hashKey = item.getString(hashKeyName);
            PrimaryKey key = new PrimaryKey(hashKeyName, hashKey);
            table.deleteItem(key);
            System.out.printf("Deleted item with key: %s\n", hashKey);
        }
    }
}