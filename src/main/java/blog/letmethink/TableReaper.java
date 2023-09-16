package blog.letmethink;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.model.ResourceNotFoundException;

/**
 * Deletes a list of tables. Handy during testing and debugging.
 * Credentials are read from ~/.aws/credentials.
 * See <a href="http://docs.aws.amazon.com/sdk-for-java/v1/developer-guide/setup-credentials.html">the documentation</a>
 * for more details.
 */
public class TableReaper {
    private final DynamoDB db;

    public TableReaper(DynamoDB db) {
        this.db = db;
    }

    public static void main(String[] args) {
        if (args.length < 1) {
            System.err.print("Usage: java DeleteTables <table1> <table2> ... <tableN>\n");
            System.exit(1);
        }
        final var client = AmazonDynamoDBClientBuilder.standard()
                .withCredentials(new ProfileCredentialsProvider())
                .withRegion(Regions.US_WEST_2)
                .build();
        final var db = new DynamoDB(client);
        new TableReaper(db).deleteTables(args);
    }

    public void deleteTables(String[] tableNames) {
        try {
            for (String tableName : tableNames) {
                deleteTable(tableName);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void deleteTable(String tableName) {
        final var table = db.getTable(tableName);
        try {
            System.out.printf("Deleting table %s\n", tableName);
            table.delete();
            table.waitForDelete();
            System.out.printf("Successfully deleted table %s\n", tableName);
        } catch (ResourceNotFoundException e) {
            System.err.printf("Table %s not found\n", tableName);
        } catch (Exception e) {
            System.err.printf("Failed to delete table %s\n", tableName);
            e.printStackTrace();
        }
    }
}
