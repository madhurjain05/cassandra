import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;

public class Create {
	
public static void main(String[] args) {
	//Query to create keyspace
	String query="create keyspace IF NOT EXISTS TestKeySpace with replication = {'class':'NetworkTopologyStrategy'}";
	
	//Creating Cluster.Builder object

	Cluster.Builder builder = Cluster.builder();
	
	//Building a cluster
	Cluster cluster = builder.addContactPoint( "127.0.0.1" ).build();
	
	//Creating a session to existing TestKeySpace
	Session session = cluster.connect("TestKeySpace");
	
	//Executing the query
    session.execute(query);
   
    //using the KeySpace
    session.execute("USE testkeyspace");
    System.out.println("Keyspace created");
    
    //Query to create table
    query = "CREATE TABLE emp(emp_id int PRIMARY KEY, "
       + "emp_name text, "
       + "emp_city text, "
       + "emp_sal varint, "
       + "emp_phone varint );";
    
  //Executing the query
    session.execute(query);
    
    System.out.println("Table Created");
}

}
