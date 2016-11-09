package com.virtusa.test;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.Iterator;

import org.apache.commons.io.FileUtils;
import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoImporter {

	public void doWork() {

		String pDatabase = Props.get("database");
		String pHost = Props.get("host");
		int pPort = Props.getInt("port");
		String pCollection = Props.get("collection");
		String pExtension = Props.get("extension");
		String pDirectory = Props.get("directory");
		boolean pRecursive = Props.getBoolean("recursive");
		String language = Props.get("language");
		//
		String pUniqueField = Props.get("uniquefield");
		String pTimestampField = Props.get("timestampfield");
		String pLanguageField = Props.get("languagefield");
		
		MongoClient mclient = new MongoClient(pHost,
				pPort);
		MongoDatabase mdatabase = mclient.getDatabase(pDatabase);
		MongoCollection<Document> mcollection = mdatabase.getCollection(pCollection);
		String[] extensionsArr = { pExtension };

		Log.print("collection #items = " + mcollection.count());

		try {
			Iterator<File> itf = FileUtils.iterateFiles(
					new File(pDirectory), extensionsArr,
					pRecursive);
			long count = 0;
			long inserted = 0;
			long upserted = 0;
			long skipped = 0;
			long skippedWrongLanguage = 0;
			long duplicates = 0;
			while (itf.hasNext()) {
				File f = itf.next();
				Log.print("item " + count++ + ", " + f);

				// read the file
				// TODO handle situation where file contains more than one object like the -jsonArray switch to mongoimport
				String jsonStr = FileUtils.readFileToString(f);
				try {
					Document localDoc = Document.parse(jsonStr);

					// TODO make language comparison optional
					// TODO generalize, not just language but any criteria (rename props at least)
					String localLanguage = localDoc.getString(pLanguageField);

					if (localLanguage.compareToIgnoreCase(language) == 0) {
						// get the timestamp
						Instant localModified = Instant.parse(localDoc
								.getString(pTimestampField));

						// determine the unique key
						String documentId = localDoc.getString(pUniqueField);

						// query to find if this document exists already
						boolean found = false;
						boolean newer = false;
						FindIterable<Document> results = mcollection
								.find(new BasicDBObject(pUniqueField,
										documentId));
						int docsFound = 0;
						Instant remoteModified = Instant.parse(localDoc
								.getString(pTimestampField));
						// TODO kludgy
						for (Document remoteDoc : results) {
							found = true;
							remoteModified = Instant.parse(remoteDoc
									.getString(pTimestampField));
							if (localModified.isAfter(remoteModified)) {
								newer = true;
							}
							docsFound++;
						}

						if (docsFound > 1) {
							Log.print("WARNING - duplicate " + documentId
									+ " found");
							// TODO do something about duplicates
							duplicates++;
						}
						if (!found) {
							Log.print("Doc " + documentId
									+ " does not exist, inserting");
							mcollection.insertOne(localDoc);
							inserted++;
						} else {
							if (newer) {
								Log.print("Doc " + documentId + " exists ("
										+ remoteModified + ") but this one ("
										+ localModified
										+ ") is newer, upserting");
								mcollection.deleteOne(new BasicDBObject(
										pUniqueField, documentId));
								mcollection.insertOne(localDoc);
								upserted++;
							} else {
								Log.print("Doc " + documentId + " exists ("
										+ remoteModified + ") but this one ("
										+ localModified
										+ ") is older or same, skipping");
								skipped++;
							}
						}

					} else {
						Log.print("skipping, language is " + localLanguage);
						skippedWrongLanguage++;
					}
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			Log.print("total=" + count);
			Log.print("inserted=" + inserted);
			Log.print("upserted=" + upserted);
			Log.print("skipped=" + skipped);
			Log.print("skippedWrongLanguage=" + skippedWrongLanguage);
			Log.print("duplicates=" + duplicates);		
		} catch (IOException e) {
			e.printStackTrace();
		}
		mclient.close();

	}

	public static void main(String[] args) {

		MongoImporter mi = new MongoImporter();
		mi.doWork();
	}

}
