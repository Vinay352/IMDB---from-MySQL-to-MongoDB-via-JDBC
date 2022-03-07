package edu.rit.ibd.a4;

import java.sql.*;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import com.mongodb.BasicDBObject;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Decimal128;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class IMDBSQLToMongo {

	public static void main(String[] args) throws Exception {
		final String dbURL = args[0]; // jdbc:mysql://localhost:3306/imdb_ibd
		final String user = args[1]; // root
		final String pwd = args[2]; // 12569388MySQL
		final String mongoDBURL = args[3]; // None  // mongodb://localhost:27017
		final String mongoDBName = args[4]; // imdb_ibd_new
		
		System.out.println(new Date() + " -- Started");
		
		Connection con = DriverManager.getConnection(dbURL, user, pwd);
		
		MongoClient client = getClient(mongoDBURL);
		MongoDatabase db = client.getDatabase(mongoDBName);
		
		// TODO 0: Your code here!
		
		/*
		 * 
		 * Everything in MongoDB is a document (both data and queries). To create a document, I use primarily two options but there are others
		 * 	if you ask the Internet. You can use org.bson.Document as follows:
		 * 
		 * 		Document d = new Document();
		 * 		d.append("name_of_the_field", value);
		 * 
		 * 	The type of the field will be the conversion of the Java type of the value.
		 * 
		 * 	Another option is to parse a string representing the document:
		 * 
		 * 		Document d = Document.parse("{ _id:1, name:\"Name\" }");
		 * 
		 * 	It will parse only well-formed documents. Note that the previous approach will use the Java data types as the types of the pieces of
		 * 		data to insert in MongoDB. However, the latter approach will not have that info as everything is a string; therefore, be mindful
		 * 		of these differences and use the approach it will fit better for you.
		 * 
		 * If you wish to create an embedded document, you can use the following:
		 * 
		 * 		Document outer = new Document();
		 * 		Document inner = new Document();
		 * 		outer.append("doc", inner);
		 * 
		 * To connect to a MongoDB database server, use the getClient method above. If your server is local, just provide "None" as input.
		 * 
		 * You must extract data from MySQL and load it into MongoDB. Note that, in general, the data in MongoDB is denormalized, which means that it includes
		 * 	redundancy. You must think of ways of extracting such redundant data in batches, that is, you should think of a bunch of queries that will retrieve
		 * 	the whole database in a format it will be convenient for you to load in MongoDB. Performing many small SQL queries will not work.
		 * 
		 * If you execute a SQL query that retrieves large amounts of data, all data will be retrieved at once and stored in main memory. To avoid such behavior,
		 * 	the JDBC URL will have the following parameter: 'useCursorFetch=true' (already added by the grading software). Then, you can control the number of 
		 * 	tuples that will be retrieved and stored in memory as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ...");
		 * 		st.setFetchSize(batchSize);
		 * 
		 * where batchSize is the number of rows.
		 * 
		 * Null values in MySQL must be translated as documents without such fields.
		 * 
		 * Once you have composed a specific document with the data retrieved from MySQL, insert the document into the appropriate collection as follows:
		 * 
		 * 		MongoCollection<Document> col = db.getCollection(COLLECTION_NAME);
		 * 
		 * 		...
		 * 
		 * 		Document d = ...
		 * 
		 * 		...
		 * 
		 * 		col.insertOne(d);
		 * 
		 * You should focus first on inserting all the documents you need (movies and people). Once those documents are already present, you should deal with
		 * 	the mapping relations. To do so, MongoDB is optimized to make small updates of documents referenced by their keys (different than MySQL). As a 
		 * 	result, it is a good idea to update one document at a time as follows:
		 * 
		 * 		PreparedStatement st = con.prepareStatement("SELECT ..."); // Select from mapping table.
		 * 		st.setFetchSize(batchSize);
		 * 		ResultSet rs = st.executeQuery();
		 * 		while (rs.next()) {
		 * 			col.updateOne(Document.parse("{ _id : "+rs.get(...)+" }"), Document.parse(...));
		 * 			...
		 * 
		 * The updateOne method updates one single document based on the filter criterion established in the first document (the _id of the document to fetch
		 * 	in this case). The second document provided as input is the update operation to perform. There are several updates operations you can perform (see
		 * 	https://docs.mongodb.com/v3.6/reference/operator/update/). If you wish to update arrays, $push and $addToSet are the best options but have slightly
		 * 	different semantics. Make sure you read and understand the differences between them.
		 * 
		 * When dealing with arrays, another option instead of updating one by one is gathering all values for a specific document and perform a single update.
		 * 
		 * Note that array fields that are empty are not allowed, so you should not generate them.
		 *  
		 */
		
		
		MongoCollection<Document> col = db.getCollection("Movies");

		MongoCollection<Document> col1 = db.getCollection("MoviesDenorm");


		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		PreparedStatement st = con.prepareStatement("SELECT m.id, m.otitle, m.ptitle, m.adult, m.year, m.runtime, m.rating, m.totalvotes, GROUP_CONCAT(name SEPARATOR \", \") AS genres\n" +
				"FROM moviegenre AS mg JOIN genre AS g ON mg.gid = g.id \n" +
				"\tRIGHT OUTER JOIN movie AS m ON m.id = mg.mid\n" +
				"GROUP BY m.id");

		st.setFetchSize(/* Batch size */ 25000);
		ResultSet rs = st.executeQuery();
		// ResultSetMetaData rsMetaData = rs.getMetaData();

//		int count = rsMetaData.getColumnCount();
//		for( int i = 1; i <= count; i++ ) {
//			System.out.println( rsMetaData.getColumnName( i ) );
//		}
//		int counter = 0;

		while (rs.next() /*&& counter < 20*/) {
			//counter++;

			Document d = new Document();
			Document d1 = new Document();

			d.append("_id", rs.getInt("id"));
			//System.out.println("id : " + rs.getInt("id"));

			d1.append("_id", rs.getInt("id"));

			String temp_otitle = rs.getString("otitle");
			if(!rs.wasNull()){
				d.append("otitle", temp_otitle);
				//System.out.println("otitle : " + temp_otitle);
			}

			String temp_ptitle = rs.getString("ptitle");
			if(!rs.wasNull()){
				d.append("ptitle", temp_ptitle);
				//System.out.println("ptitle : " + temp_ptitle);
			}

			boolean temp_adult = rs.getBoolean("adult");
			if(!rs.wasNull()) {
				d.append("adult", temp_adult);
				//System.out.println("adult : " + temp_adult);
			}

			int temp_year = rs.getInt("year");
			if(!rs.wasNull()) {
				d.append("year", temp_year);
				//System.out.println("year : " + temp_year);
			}

			int temp_runtime = rs.getInt("runtime");
			if(!rs.wasNull()) {
				d.append("runtime", temp_runtime);
				//System.out.println("runtime : " + temp_runtime);
			}

			// To deal with float attributes, use the code below to retrieve big decimals for attribute x in MySQL and create Decimal128 in MongoDB.

			String temp_rating = rs.getString("rating");
			if(!rs.wasNull()) {
				Decimal128 x = new Decimal128(rs.getBigDecimal("rating"));
				//System.out.println("rating : " + x);
				d.append("rating", x);
			}

			int temp_totalvotes = rs.getInt("totalvotes");
			if(!rs.wasNull()) {
				d.append("totalvotes", temp_totalvotes);
				//System.out.println("totalvotes : " + temp_totalvotes);
			}

			String temp_genres = rs.getString("genres");
			if(!rs.wasNull()) {
				// String temp = rs.getString("genres");

				String[] temp_list = temp_genres.strip().split(", ");
				// d.append("genres", temp_list);

				List<String> temp_genresList = new ArrayList<String>(List.of(temp_list));

				d.append("genres", temp_genresList);

				//System.out.println(temp_genresList);
			}

			//String[] temp_list = temp.split(", ");
			//System.out.println("---------------");

			col.insertOne(d);

			col1.insertOne(d1);

			// If something is NULL, then, do not include the field!


		}
		rs.close();
		st.close();




		st = con.prepareStatement("Select mid, group_concat(pid separator ', ') as actors\n" +
				"from actor as a\n" +
				"group by mid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_actors = rs.getString("actors");
			if(!rs.wasNull()) {

				// String temp = rs.getString("genres");

				String[] temp_list = temp_actors.strip().split(", ");
				// d.append("genres", temp_list);

				int[] num = new int[temp_list.length];
				for(int i = 0 ; i < temp_list.length; i++){
					num[i] = Integer.parseInt(temp_list[i]);
				}

				// d.append("genres", temp_list);

				List<Integer> temp_actorsList = new ArrayList<Integer>(num.length);
				for(int i : num){
					temp_actorsList.add(i);
				}

				// List<String> temp_actorsList = new ArrayList<String>(List.of(temp_list));

				Document query = new Document();
				query.append("_id",rs.getInt("mid"));

				Document setData = new Document();
				setData.append("actors", temp_actorsList);

				Document update = new Document();
				update.append("$set", setData);

				// To update single Document
				// collection.updateOne(query, update);

				// System.out.println(temp_genresList);

				// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

				col1.updateOne(query, update);

			}

		}
		rs.close();
		st.close();

		st = con.prepareStatement("Select mid, group_concat(pid separator ', ') as directors\n" +
				"from director as d\n" +
				"group by mid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_directors = rs.getString("directors");
			if(!rs.wasNull()) {

				// String temp = rs.getString("genres");

				String[] temp_list = temp_directors.strip().split(", ");
				// d.append("genres", temp_list);

				int[] num = new int[temp_list.length];
				for(int i = 0 ; i < temp_list.length; i++){
					num[i] = Integer.parseInt(temp_list[i]);
				}

				// d.append("genres", temp_list);

				List<Integer> temp_actorsList = new ArrayList<Integer>(num.length);
				for(int i : num){
					temp_actorsList.add(i);
				}

				// List<String> temp_directorsList = new ArrayList<String>(List.of(temp_list));

				Document query = new Document();
				query.append("_id",rs.getInt("mid"));

				Document setData = new Document();
				setData.append("directors", temp_actorsList);

				Document update = new Document();
				update.append("$set", setData);

				// To update single Document
				// collection.updateOne(query, update);

				// System.out.println(temp_genresList);

				// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

				col1.updateOne(query, update);

			}

		}
		rs.close();
		st.close();


		st = con.prepareStatement("Select mid, group_concat(pid separator ', ') as producers\n" +
				"from producer as p\n" +
				"group by mid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_producers = rs.getString("producers");
			if(!rs.wasNull()) {

				// String temp = rs.getString("genres");

				String[] temp_list = temp_producers.strip().split(", ");
				// d.append("genres", temp_list);

				int[] num = new int[temp_list.length];
				for(int i = 0 ; i < temp_list.length; i++){
					num[i] = Integer.parseInt(temp_list[i]);
				}

				// d.append("genres", temp_list);

				List<Integer> temp_producersList = new ArrayList<Integer>(num.length);
				for(int i : num){
					temp_producersList.add(i);
				}

				// List<String> temp_producersList = new ArrayList<String>(List.of(temp_list));

				Document query = new Document();
				query.append("_id",rs.getInt("mid"));

				Document setData = new Document();
				setData.append("producers", temp_producersList);

				Document update = new Document();
				update.append("$set", setData);

				// To update single Document
				// collection.updateOne(query, update);

				// System.out.println(temp_genresList);

				// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

				col1.updateOne(query, update);

			}

		}
		rs.close();
		st.close();


		st = con.prepareStatement("Select mid, group_concat(pid separator ', ') as writers\n" +
				"from writer as w\n" +
				"group by mid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_producers = rs.getString("writers");
			if(!rs.wasNull()) {

				// String temp = rs.getString("genres");

				String[] temp_list = temp_producers.strip().split(", ");
				// d.append("genres", temp_list);

				int[] num = new int[temp_list.length];
				for(int i = 0 ; i < temp_list.length; i++){
					num[i] = Integer.parseInt(temp_list[i]);
				}

				// d.append("genres", temp_list);

				List<Integer> temp_actorsList = new ArrayList<Integer>(num.length);
				for(int i : num){
					temp_actorsList.add(i);
				}

				// List<String> temp_producersList = new ArrayList<String>(List.of(temp_list));

				Document query = new Document();
				query.append("_id",rs.getInt("mid"));

				Document setData = new Document();
				setData.append("writers", temp_actorsList);

				Document update = new Document();
				update.append("$set", setData);

				// To update single Document
				// collection.updateOne(query, update);

				// System.out.println(temp_genresList);

				// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

				col1.updateOne(query, update);

			}

		}
		rs.close();
		st.close();




		MongoCollection<Document> col2 = db.getCollection("People");
		MongoCollection<Document> col3 = db.getCollection("PeopleDenorm");

		// Try to use few queries that retrieve big chunks of data rather than many queries that retrieve small pieces of data.
		st = con.prepareStatement("SELECT id, name, byear, dyear\n" +
				"FROM person AS p");

		st.setFetchSize(/* Batch size */ 50000);
		rs = st.executeQuery();
		// rsMetaData = rs.getMetaData();

//		int count = rsMetaData.getColumnCount();
//		for( int i = 1; i <= count; i++ ) {
//			System.out.println( rsMetaData.getColumnName( i ) );
//		}
//		int counter = 0;

		while (rs.next() /*&& counter < 20*/) {
			//counter++;

			Document d = new Document();
			Document d1 = new Document();

			d.append("_id", rs.getInt("id"));
			//System.out.println("id : " + rs.getInt("id"));

			d1.append("_id", rs.getInt("id"));


			String temp_name = rs.getString("name");
			if(!rs.wasNull()){
				d.append("name", temp_name);
				//System.out.println("otitle : " + temp_otitle);
			}

			int temp_byear = rs.getInt("byear");
			if(!rs.wasNull()){
				d.append("byear", temp_byear);
				//System.out.println("ptitle : " + temp_ptitle);
			}

			int temp_dyear = rs.getInt("dyear");
			if(!rs.wasNull()) {
				d.append("dyear", temp_dyear);
				//System.out.println("adult : " + temp_adult);
			}

			col2.insertOne(d);

			col3.insertOne(d1);

			// If something is NULL, then, do not include the field!


		}
		rs.close();
		st.close();



		st = con.prepareStatement("Select pid, group_concat(mid separator ', ') as acted\n" +
				"from actor as a\n" +
				"group by pid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_actors = rs.getString("acted");
			if(!temp_actors.isBlank()){
				if(!rs.wasNull()) {

					// String temp = rs.getString("genres");
					String[] temp_list = temp_actors.strip().split(", ");
					if(temp_list.length > 0){
						int[] num = new int[temp_list.length];
						for(int i = 0 ; i < temp_list.length; i++){
							num[i] = Integer.parseInt(temp_list[i].replace(",","").strip());
						}

						// d.append("genres", temp_list);

						List<Integer> temp_actorsList = new ArrayList<Integer>(num.length);
						for(int i : num){
							temp_actorsList.add(i);
						}

						if(temp_actorsList.isEmpty()){
							System.out.println("This is empty");
						}

						Document query = new Document();
						query.append("_id",rs.getInt("pid"));

						Document setData = new Document();
						setData.append("acted", temp_actorsList);

						Document update = new Document();
						update.append("$set", setData);

						// To update single Document
						// collection.updateOne(query, update);

						// System.out.println(temp_genresList);

						// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

						col3.updateOne(query, update);
					}



				}
			}

		}
		rs.close();
		st.close();

		st = con.prepareStatement("Select pid, group_concat(mid separator ', ') as directed\n" +
				"from director as d\n" +
				"group by pid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_directors = rs.getString("directed");
			if(!temp_directors.isBlank()){
				if(!rs.wasNull()) {

					// String temp = rs.getString("genres");

					String[] temp_list = temp_directors.strip().split(", ");
					// d.append("genres", temp_list);

					if(temp_list.length > 0){
						int[] num = new int[temp_list.length];
						for(int i = 0 ; i < temp_list.length; i++){
							num[i] = Integer.parseInt(temp_list[i].replace(",","").strip());
						}

						// d.append("genres", temp_list);

						List<Integer> temp_directorsList = new ArrayList<Integer>(num.length);
						for(int i : num){
							temp_directorsList.add(i);
						}

						// List<String> temp_directorsList = new ArrayList<String>(List.of(temp_list));

						Document query = new Document();
						query.append("_id",rs.getInt("pid"));

						Document setData = new Document();
						setData.append("directed", temp_directorsList);

						Document update = new Document();
						update.append("$set", setData);

						// To update single Document
						// collection.updateOne(query, update);

						// System.out.println(temp_genresList);

						// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

						col3.updateOne(query, update);
					}

				}
			}


		}
		rs.close();
		st.close();



		st = con.prepareStatement("Select pid, group_concat(mid separator ', ') as kf\n" +
				"from knownfor \n" +
				"group by pid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_producers = rs.getString("kf");
			if(!temp_producers.isBlank()){
				if(!rs.wasNull()) {

					// String temp = rs.getString("genres");

					String[] temp_list = temp_producers.strip().split(", ");
					// d.append("genres", temp_list);

					if(temp_list.length > 0){
						int[] num = new int[temp_list.length];
						for(int i = 0 ; i < temp_list.length; i++){
							num[i] = Integer.parseInt(temp_list[i].replace(",","").strip());
						}

						// d.append("genres", temp_list);

						List<Integer> temp_producersList = new ArrayList<Integer>(num.length);
						for(int i : num){
							temp_producersList.add(i);
						}


						// List<String> temp_producersList = new ArrayList<String>(List.of(temp_list));

						Document query = new Document();
						query.append("_id",rs.getInt("pid"));

						Document setData = new Document();
						setData.append("knownfor", temp_producersList);

						Document update = new Document();
						update.append("$set", setData);

						// To update single Document
						// collection.updateOne(query, update);

						// System.out.println(temp_genresList);

						// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

						col3.updateOne(query, update);
					}



				}
			}


		}
		rs.close();
		st.close();


		st = con.prepareStatement("Select pid, group_concat(mid separator ', ') as produced\n" +
				"from producer as p\n" +
				"group by pid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_producers = rs.getString("produced");
			if(!temp_producers.isBlank()){
				if(!rs.wasNull()) {

					// String temp = rs.getString("genres");

					String[] temp_list = temp_producers.strip().split(", ");
					// d.append("genres", temp_list);

					if(temp_list.length > 0){
						int[] num = new int[temp_list.length];
						for(int i = 0 ; i < temp_list.length; i++){
							num[i] = Integer.parseInt(temp_list[i].replace(",","").strip());
						}

						// d.append("genres", temp_list);

						List<Integer> temp_producersList = new ArrayList<Integer>(num.length);
						for(int i : num){
							temp_producersList.add(i);
						}

						// List<String> temp_producersList = new ArrayList<String>(List.of(temp_list));

						Document query = new Document();
						query.append("_id",rs.getInt("pid"));

						Document setData = new Document();
						setData.append("produced", temp_producersList);

						Document update = new Document();
						update.append("$set", setData);

						// To update single Document
						// collection.updateOne(query, update);

						// System.out.println(temp_genresList);

						// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

						col3.updateOne(query, update);
					}

				}
			}


		}
		rs.close();
		st.close();


		st = con.prepareStatement("Select pid, group_concat(mid separator ', ') as written\n" +
				"from writer as w\n" +
				"group by pid");
		rs = st.executeQuery();
		while (rs.next()){

			String temp_producers = rs.getString("written");
			if(!temp_producers.isBlank()){
				if(!rs.wasNull()) {

					// String temp = rs.getString("genres");

					String[] temp_list = temp_producers.strip().split(", ");
					// d.append("genres", temp_list);

					if(temp_list.length > 0){
						int[] num = new int[temp_list.length];
						for(int i = 0 ; i < temp_list.length; i++){
							num[i] = Integer.parseInt(temp_list[i].replace(",","").strip());
						}

						// d.append("genres", temp_list);

						List<Integer> temp_producersList = new ArrayList<Integer>(num.length);
						for(int i : num){
							temp_producersList.add(i);
						}

						// List<String> temp_producersList = new ArrayList<String>(List.of(temp_list));

						Document query = new Document();
						query.append("_id",rs.getInt("pid"));

						Document setData = new Document();
						setData.append("written", temp_producersList);

						Document update = new Document();
						update.append("$set", setData);

						// To update single Document
						// collection.updateOne(query, update);

						// System.out.println(temp_genresList);

						// col1.updateOne(/* Filter to grab a single document */ (Bson) null, /* Changes to perform; use $push/$addToSet to add values to arrays. */ (Bson) null);

						col3.updateOne(query, update);
					}

				}
			}


		}
		rs.close();
		st.close();


		
		// TODO 0: End of your code.
		
		client.close();
		con.close();
	}
	
	private static MongoClient getClient(String mongoDBURL) {
		MongoClient client = null;
		if (mongoDBURL.equals("None"))
			client = new MongoClient();
		else
			client = new MongoClient(new MongoClientURI(mongoDBURL));
		return client;
	}

}
